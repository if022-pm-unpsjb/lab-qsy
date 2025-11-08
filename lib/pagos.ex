defmodule Libremarket.Pagos do
  @moduledoc false
  def procesar() do
    # 70% true, 30% false
    Enum.random(1..100) <= 70
  end
end

defmodule Libremarket.Pagos.Server do
  @moduledoc """
  Servidor de Pagos con elección de líder
  """
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}
  @service_name "pagos"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def is_leader?() do
    Libremarket.ZookeeperLeader.is_leader?(@service_name)
  end

  def procesarPago(pid \\ @global_name, idPago) do
    GenServer.call(pid, {:procesarPago, idPago})
  end

  @impl true
  def init(_opts) do
    Logger.info("Servidor de Pagos iniciado")
    Logger.info("Esperando elección de líder mediante Zookeeper...")

    {:ok, _pid} = Libremarket.ZookeeperLeader.start_link(
      service_name: @service_name,
      on_leader_change: &handle_leader_change/1
    )

    {:ok, %{is_leader: false}}
  end

  @impl true
  def handle_call({:procesarPago, idPago}, _from, state) do
    resultado = Libremarket.Pagos.procesar()
    state = Map.put(state, idPago, resultado)
    {:reply, resultado, state}
  end

  @impl true
  def handle_call(:listarPagos, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info({:leader_change, is_leader}, state) do
    Logger.info("""
    [Pagos] Cambio de liderazgo
    Nodo: #{Node.self()}
    Es líder: #{is_leader}
    """)
    {:noreply, %{state | is_leader: is_leader}}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp handle_leader_change(is_leader) do
    case :global.whereis_name(__MODULE__) do
      :undefined -> :ok
      pid -> send(pid, {:leader_change, is_leader})
    end
  end
end

defmodule Libremarket.Pagos.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Pagos con elección de líder
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "pagos_queue"
  @check_leader_interval 5_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :check_leadership)
    {:ok, %{channel: nil, consumer_tag: nil, is_consuming: false}}
  end

  @impl true
  def handle_info(:check_leadership, state) do
    is_leader = Libremarket.Pagos.Server.is_leader?()
    
    new_state = 
      cond do
        is_leader and not state.is_consuming ->
          Logger.info("[Consumer Pagos] Este nodo es LÍDER, iniciando consumo de mensajes")
          start_consuming(state)
        
        not is_leader and state.is_consuming ->
          Logger.info("[Consumer Pagos] Este nodo ya NO es líder, deteniendo consumo de mensajes")
          stop_consuming(state)
        
        true ->
          state
      end
    
    Process.send_after(self(), :check_leadership, @check_leader_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:setup, state) do
    with {:ok, channel} <- Connection.get_channel(),
         :ok <- setup_queue(channel),
         {:ok, consumer_tag} <- AMQP.Basic.consume(channel, @queue) do
      Logger.info("Consumer de Pagos iniciado en cola: #{@queue}")
      {:noreply, %{state | channel: channel, consumer_tag: consumer_tag, is_consuming: true}}
    else
      error ->
        Logger.error("Error configurando consumer de Pagos: #{inspect(error)}")
        Process.send_after(self(), :setup, 5000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, state) do
    spawn(fn -> handle_message(payload, meta) end)
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}
  @impl true
  def handle_info({:basic_cancel, _meta}, state), do: {:stop, :normal, state}
  @impl true
  def handle_info({:basic_cancel_ok, _meta}, state), do: {:noreply, state}

  defp setup_queue(channel) do
    with :ok <- AMQP.Exchange.declare(channel, @exchange, :topic, durable: true),
         {:ok, _info} <- AMQP.Queue.declare(channel, @queue, durable: true),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "pagos.requests") do
      :ok
    end
  end

  defp handle_message(payload, meta) do
    case Jason.decode(payload) do
      {:ok, message} ->
        process_message(message)
        ack_message(meta)
      {:error, reason} ->
        Logger.error("Error decodificando mensaje: #{inspect(reason)}")
        nack_message(meta)
    end
  end

  defp process_message(%{"request_type" => "procesar_pago", "compra_id" => compra_id, "pago_id" => pago_id}) do
    Logger.info("Procesando pago #{pago_id} para compra #{compra_id}")

    resultado = Libremarket.Pagos.Server.procesarPago(pago_id)

    if resultado do
      # Pago aprobado
      Publisher.publish("pagos.events", %{
        "event_type" => "pago_procesado",
        "pago_id" => pago_id,
        "resultado" => "aprobado",
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

      Publisher.publish("pagos.responses", %{
        "response_type" => "pago_procesado",
        "compra_id" => compra_id,
        "pago_id" => pago_id,
        "aprobado" => true,
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    else
      # Pago rechazado
      Publisher.publish("pagos.events", %{
        "event_type" => "pago_rechazado",
        "pago_id" => pago_id,
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

      Publisher.publish("pagos.responses", %{
        "response_type" => "pago_rechazado",
        "compra_id" => compra_id,
        "pago_id" => pago_id,
        "aprobado" => false,
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    end
  end

  defp process_message(message) do
    Logger.warning("Mensaje no reconocido en Pagos Consumer: #{inspect(message)}")
  end

  defp ack_message(meta) do
    with {:ok, channel} <- Connection.get_channel() do
      AMQP.Basic.ack(channel, meta.delivery_tag)
    end
  end

  defp nack_message(meta) do
    with {:ok, channel} <- Connection.get_channel() do
      AMQP.Basic.nack(channel, meta.delivery_tag, requeue: false)
    end
  end

  defp start_consuming(state) do
    send(self(), :setup)
    state
  end

  defp stop_consuming(state) do
    if state.channel && state.consumer_tag do
      try do
        AMQP.Basic.cancel(state.channel, state.consumer_tag)
        Logger.info("[Consumer Pagos] Consumo detenido")
      catch
        _, _ -> :ok
      end
    end
    %{state | is_consuming: false, consumer_tag: nil}
  end
end
