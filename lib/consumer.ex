defmodule Libremarket.AMQP.Consumer do
  @moduledoc """
  Consume mensajes de RabbitMQ
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.Connection

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

    case routing_key do
      "ventas.events" ->
        process_venta_event(message)

      "compras.events" ->
        process_compra_event(message)

      "pagos.events" ->
        process_pago_event(message)

      "envios.events" ->
        process_envio_event(message)

      "infracciones.events" ->
        process_infraccion_event(message)

      _ ->
        Logger.warning("Routing key no reconocido: #{routing_key}")
        :ok
    end
  end

  defp process_venta_event(%{"event_type" => "producto_vendido", "producto_id" => producto_id} = _event) do
    Logger.info("Producto vendido: #{producto_id}")
    # El stock ya fue reducido por el servicio de ventas
    # Aquí podrías notificar a otros sistemas, analytics, etc.
    :ok
  end

  defp process_venta_event(%{"event_type" => "stock_actualizado", "producto_id" => producto_id, "stock" => stock} = _event) do
    Logger.info("Stock actualizado para producto #{producto_id}: #{stock} unidades")
    # Aquí podrías notificar a sistemas de inventario, generar alertas de stock bajo, etc.
    if stock < 3 do
      Logger.warning("¡Alerta! Stock bajo para producto #{producto_id}: #{stock} unidades")
    end
    :ok
  end

  defp process_venta_event(event) do
    Logger.warning("Evento de venta no reconocido: #{inspect(event)}")
    :ok
  end

  defp process_compra_event(%{"event_type" => "compra_realizada", "compra" => compra} = _event) do
    Logger.info("Compra realizada: #{inspect(compra)}")
    # Enviar notificación al cliente
    # Generar factura
    # Actualizar métricas de ventas
    # Trigger analytics
    :ok
  end

  defp process_compra_event(%{"event_type" => "compra_fallida", "motivo" => motivo, "producto_id" => producto_id} = _event) do
    Logger.warning("Compra fallida para producto #{producto_id}: #{motivo}")
    # Aquí podrías:
    # - Notificar al usuario del error
    # - Registrar métricas de fallos
    # - Sugerir productos alternativos
    :ok
  end

  defp process_compra_event(event) do
    Logger.warning("Evento de compra no reconocido: #{inspect(event)}")
    :ok
  end

  defp process_pago_event(%{"event_type" => "pago_procesado", "pago_id" => pago_id, "resultado" => resultado} = _event) do
    Logger.info("Pago #{pago_id} procesado: #{resultado}")
    # Aquí podrías:
    # - Enviar confirmación de pago al cliente
    # - Actualizar sistema contable
    # - Generar recibo
    :ok
  end

  defp process_pago_event(%{"event_type" => "pago_rechazado", "pago_id" => pago_id, "producto_id" => producto_id} = _event) do
    Logger.warning("Pago rechazado: #{pago_id} para producto #{producto_id}")
    # El stock ya fue repuesto por el servicio de compras
    # Aquí podrías:
    # - Notificar al usuario del rechazo
    # - Sugerir métodos de pago alternativos
    :ok
  end

  defp process_pago_event(event) do
    Logger.warning("Evento de pago no reconocido: #{inspect(event)}")
    :ok
  end

  defp process_envio_event(%{"event_type" => "envio_creado", "envio" => envio} = _event) do
    Logger.info("Envío creado: #{inspect(envio)}")
    # Aquí podrías:
    # - Notificar al cliente con código de seguimiento
    # - Integrar con sistema de logística
    # - Generar etiqueta de envío
    :ok
  end

  defp process_envio_event(%{"event_type" => "envio_despachado", "envio_id" => envio_id} = _event) do
    Logger.info("Envío despachado: #{envio_id}")
    # Aquí podrías:
    # - Notificar al cliente que el paquete está en camino
    # - Actualizar estado en sistema de tracking
    # - Enviar notificación push
    :ok
  end

  defp process_envio_event(event) do
    Logger.warning("Evento de envío no reconocido: #{inspect(event)}")
    :ok
  end

  defp process_infraccion_event(%{"event_type" => "infraccion_detectada", "compra_id" => compra_id, "producto_id" => producto_id} = _event) do
    Logger.warning("Infracción detectada en compra: #{compra_id}, producto: #{producto_id}")
    # El stock ya fue repuesto por el servicio de compras
    # Aquí podrías:
    # - Notificar al sistema de seguridad
    # - Registrar intento de fraude
    # - Bloquear usuario si es reincidente
    # - Generar alerta para equipo de seguridad
    :ok
  end

  defp process_infraccion_event(event) do
    Logger.warning("Evento de infracción no reconocido: #{inspect(event)}")
    :ok
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
