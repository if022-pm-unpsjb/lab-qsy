defmodule Libremarket.AMQP.Publisher do
  @moduledoc """
  Publica mensajes en RabbitMQ
  """
  require Logger
  alias Libremarket.AMQP.Connection

  @exchange "libremarket_exchange"

  def publish(routing_key, message, retry_count \\ 0) do
    with {:ok, channel} <- Connection.get_channel(),
         :ok <- ensure_exchange(channel),
         :ok <- publish_message(channel, routing_key, message) do
      Logger.info("Mensaje publicado: #{routing_key}")
      :ok
    else
      {:error, :no_channel} when retry_count < 3 ->
        Logger.warning("Canal no disponible, reintentando en 1 segundo...")
        Process.sleep(1000)
        publish(routing_key, message, retry_count + 1)

      {:error, :channel_dead} when retry_count < 3 ->
        Logger.warning("Canal muerto, reintentando en 1 segundo...")
        Process.sleep(1000)
        publish(routing_key, message, retry_count + 1)

      {:error, reason} ->
        Logger.error("Error publicando mensaje: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp ensure_exchange(channel) do
    AMQP.Exchange.declare(channel, @exchange, :topic, durable: true)
  end

  defp publish_message(channel, routing_key, message) do
    payload = Jason.encode!(message)

    AMQP.Basic.publish(
      channel,
      @exchange,
      routing_key,
      payload,
      persistent: true,
      content_type: "application/json"
    )
  end
end
