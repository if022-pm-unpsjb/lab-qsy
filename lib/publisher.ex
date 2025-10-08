defmodule LibreMarket.AMQP.Publisher do
  @moduledoc """
  Publica mensajes en RabbitMQ
  """
  require Logger
  alias LibreMarket.AMQP.Connection

  @exchange "libremarket_exchange"

  def publish(routing_key, message) do
    with {:ok, channel} <- Connection.get_channel(),
         :ok <- ensure_exchange(channel),
         :ok <- publish_message(channel, routing_key, message) do
      Logger.info("Mensaje publicado: #{routing_key}")
      :ok
    else
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
