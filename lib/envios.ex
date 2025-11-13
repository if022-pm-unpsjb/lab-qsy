defmodule Libremarket.Envios do
  @moduledoc false
  def calcular(:retira), do: 0
  def calcular(:correo), do: Enum.random(500..1500)
end

defmodule Libremarket.Envios.Server do
  @moduledoc """
  Servidor de Envíos con elección de líder
  """
  use GenServer
  require Logger

  @service_name "envios"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def is_leader?() do
    try do
      Libremarket.LeaderElection.is_leader?(@service_name)
    catch
      _, _ -> false
    end
  end

  def procesarEnvio(idCompra, forma_entrega) do
    case find_leader_node() do
      nil -> {:error, :no_leader}
      node -> :rpc.call(node, GenServer, :call, [__MODULE__, {:procesarEnvio, idCompra, forma_entrega}])
    end
  end

  def listarEnvios() do
    case find_any_node() do
      nil -> %{}
      node -> :rpc.call(node, GenServer, :call, [__MODULE__, :listarEnvios])
    end
  end

  @impl true
  def init(_opts) do
    Logger.info("Servidor de Envíos iniciado")
    Logger.info("Esperando elección de líder...")

    {:ok, _pid} = Libremarket.LeaderElection.start_link(
      service_name: @service_name,
      on_leader_change: &handle_leader_change/1
    )

    {:ok, %{is_leader: false}}
  end

  @impl true
  def handle_call({:procesarEnvio, idCompra, forma_entrega}, _from, state) do
    costo = Libremarket.Envios.calcular(forma_entrega)
    envio = %{
      id_compra: idCompra,
      forma: forma_entrega,
      costo: costo
    }
    state = Map.put(state, idCompra, envio)
    {:reply, envio, state}
  end

  @impl true
  def handle_call(:listarEnvios, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info({:leader_change, is_leader}, state) do
    Logger.info("""
    [Envios] Cambio de liderazgo
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
    case Process.whereis(__MODULE__) do
      nil ->
        Logger.warning("[Envios] No se encontró el proceso local")
        :ok
      pid when is_pid(pid) ->
        send(pid, {:leader_change, is_leader})
    end
  end

  defp find_leader_node() do
    all_nodes = [Node.self() | Node.list()]

    Enum.find(all_nodes, fn node ->
      node_str = Atom.to_string(node)
      if String.contains?(node_str, "envios") do
        try do
          :rpc.call(node, Libremarket.LeaderElection, :is_leader?, [@service_name], 2000) == true
        catch
          _, _ -> false
        end
      else
        false
      end
    end)
  end

  defp find_any_node() do
    all_nodes = [Node.self() | Node.list()]

    Enum.find(all_nodes, fn node ->
      String.contains?(Atom.to_string(node), "envios")
    end)
  end
end

defmodule Libremarket.Envios.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Envíos con elección de líder
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "envios_queue"
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
    is_leader = Libremarket.Envios.Server.is_leader?()

    new_state =
      cond do
        is_leader and not state.is_consuming ->
          Logger.info("[Consumer Envios] Este nodo es LÍDER, iniciando consumo de mensajes")
          start_consuming(state)

        not is_leader and state.is_consuming ->
          Logger.info("[Consumer Envios] Este nodo ya NO es líder, deteniendo consumo de mensajes")
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
      Logger.info("Consumer de Envíos iniciado en cola: #{@queue}")
      {:noreply, %{state | channel: channel, consumer_tag: consumer_tag, is_consuming: true}}
    else
      error ->
        Logger.error("Error configurando consumer de Envíos: #{inspect(error)}")
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
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "envios.requests") do
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

  defp process_message(%{"request_type" => "procesar_envio", "compra_id" => compra_id, "producto_id" => producto_id, "forma_entrega" => forma_entrega}) do
    Logger.info("Procesando envío para compra #{compra_id}, forma: #{forma_entrega}")

    forma_atom = String.to_atom(forma_entrega)
    envio = Libremarket.Envios.Server.procesarEnvio(producto_id, forma_atom)

    Publisher.publish("envios.events", %{
      "event_type" => "envio_creado",
      "envio" => %{
        "id_compra" => envio.id_compra,
        "forma" => Atom.to_string(envio.forma),
        "costo" => envio.costo
      },
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })

    Publisher.publish("envios.responses", %{
      "response_type" => "envio_procesado",
      "compra_id" => compra_id,
      "envio" => %{
        "id_compra" => envio.id_compra,
        "forma" => Atom.to_string(envio.forma),
        "costo" => envio.costo
      },
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  end

  defp process_message(message) do
    Logger.warning("Mensaje no reconocido en Envíos Consumer: #{inspect(message)}")
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
        Logger.info("[Consumer Envios] Consumo detenido")
      catch
        _, _ -> :ok
      end
    end
    %{state | is_consuming: false, consumer_tag: nil}
  end
end
