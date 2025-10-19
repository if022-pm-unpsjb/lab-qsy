defmodule Libremarket.Pagos do
  @moduledoc false
  def procesar() do
    # 70% true, 30% false
    Enum.random(1..100) <= 70
  end
end

defmodule Libremarket.Pagos.Server do
  @moduledoc """
  Servidor de Pagos
  """
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def procesarPago(pid \\ @global_name, idPago) do
    GenServer.call(pid, {:procesarPago, idPago})
  end

  @impl true
  def init(_opts) do
    Logger.info("Servidor de Pagos iniciado")
    {:ok, %{}}
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
end

defmodule Libremarket.Pagos.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Pagos
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "pagos_queue"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :setup)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:setup, state) do
    with {:ok, channel} <- Connection.get_channel(),
         :ok <- setup_queue(channel),
         {:ok, _consumer_tag} <- AMQP.Basic.consume(channel, @queue) do
      Logger.info("Consumer de Pagos iniciado en cola: #{@queue}")
      {:noreply, Map.put(state, :channel, channel)}
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
end
