defmodule LibreMarket.AMQP.Consumer do
  @moduledoc """
  Consume mensajes de RabbitMQ
  """
  use GenServer
  require Logger
  alias LibreMarket.AMQP.Connection

  @exchange "libremarket_exchange"
  @queue "libremarket_queue"

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
      Logger.info("Consumer iniciado en cola: #{@queue}")
      {:noreply, Map.put(state, :channel, channel)}
    else
      error ->
        Logger.error("Error configurando consumer: #{inspect(error)}")
        Process.send_after(self(), :setup, 5000)
        {:noreply, state}
    end
  end

  # Confirma el mensaje recibido
  @impl true
  def handle_info({:basic_deliver, payload, meta}, state) do
    Logger.info("Mensaje recibido: #{inspect(meta.routing_key)}")

    spawn(fn ->
      handle_message(payload, meta)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, _meta}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, _meta}, state) do
    {:noreply, state}
  end

  defp setup_queue(channel) do
    with :ok <- AMQP.Exchange.declare(channel, @exchange, :topic, durable: true),
         {:ok, _info} <- AMQP.Queue.declare(channel, @queue, durable: true),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "*.events") do
      :ok
    end
  end

  defp handle_message(payload, meta) do
    case Jason.decode(payload) do
      {:ok, message} ->
        process_message(message, meta.routing_key)
        ack_message(meta)

      {:error, reason} ->
        Logger.error("Error decodificando mensaje: #{inspect(reason)}")
        nack_message(meta)
    end
  end

  defp process_message(message, routing_key) do
    Logger.info("Procesando mensaje de #{routing_key}: #{inspect(message)}")
    # Aquí procesas el mensaje según tu lógica de negocio
    # Por ejemplo, actualizar base de datos, notificar otros servicios, etc.
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
