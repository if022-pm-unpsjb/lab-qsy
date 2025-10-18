defmodule Libremarket.Compras.Server do
  @moduledoc """
  Servidor de Compras - Orquesta el flujo de compra mediante mensajes AMQP
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.Publisher

  @global_name {:global, __MODULE__}

  defp atomizar_keys(map) when is_map(map) do
  Map.new(map, fn {k, v} ->
    {String.to_atom(k), v}
  end)
end

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def comprar(pid \\ @global_name, producto_id, medio_pago, forma_entrega) do
    GenServer.call(pid, {:comprar, producto_id, medio_pago, forma_entrega}, 15000)
  end

  def listar_compras(pid \\ @global_name) do
    GenServer.call(pid, :listar_compras)
  end

  @impl true
  def init(state) do
    Logger.info("Servidor de Compras iniciado")
    {:ok, Map.merge(state, %{compras_en_proceso: %{}, compras_realizadas: []})}
  end

  @impl true
  def handle_call({:comprar, producto_id, medio_pago, forma_entrega}, from, state) do
    compra_id = "compra_#{:rand.uniform(100000)}"

    Logger.info("Iniciando compra #{compra_id} para producto #{producto_id}")

    # Guardar información de la compra en proceso
    compra_info = %{
      compra_id: compra_id,
      producto_id: producto_id,
      medio_pago: medio_pago,
      forma_entrega: forma_entrega,
      from: from,
      estado: :verificando_stock,
      timestamp: DateTime.utc_now()
    }

    nuevo_state = put_in(state, [:compras_en_proceso, compra_id], compra_info)

    # 1. Solicitar verificación de stock (primer mensaje AMQP)
    Publisher.publish("ventas.requests", %{
      "request_type" => "verificar_stock",
      "compra_id" => compra_id,
      "producto_id" => producto_id,
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })

    {:noreply, nuevo_state}
  end

  @impl true
  def handle_call(:listar_compras, _from, state) do
    {:reply, state.compras_realizadas, state}
  end

  # Handlers para respuestas de Ventas
  @impl true
  def handle_cast({:stock_verificado, compra_id, producto}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        Logger.warning("Compra #{compra_id} no encontrada en proceso")
        {:noreply, state}

      compra_info ->
        Logger.info("Stock verificado para compra #{compra_id}, confirmando venta...")

        # 2. Solicitar confirmación de venta (reservar stock)
        nuevo_state = put_in(state, [:compras_en_proceso, compra_id, :estado], :confirmando_venta)
        nuevo_state = put_in(nuevo_state, [:compras_en_proceso, compra_id, :producto], atomizar_keys(producto))

        Publisher.publish("ventas.requests", %{
          "request_type" => "confirmar_venta",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        {:noreply, nuevo_state}
    end
  end

  @impl true
  def handle_cast({:venta_confirmada, compra_id, producto}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.info("Venta confirmada para compra #{compra_id}, detectando infracciones...")

        # 3. Solicitar detección de infracciones
        nuevo_state = put_in(state, [:compras_en_proceso, compra_id, :estado], :detectando_infracciones)
        nuevo_state = put_in(nuevo_state, [:compras_en_proceso, compra_id, :producto], atomizar_keys(producto))

        Publisher.publish("infracciones.requests", %{
          "request_type" => "detectar_infracciones",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        {:noreply, nuevo_state}
    end
  end

  @impl true
  def handle_cast({:error_stock, compra_id, error}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.error("Error de stock para compra #{compra_id}: #{error}")
        GenServer.reply(compra_info.from, {:error, String.to_atom(error)})
        nuevo_state = Map.update!(state, :compras_en_proceso, &Map.delete(&1, compra_id))
        {:noreply, nuevo_state}
    end
  end

  # Handlers para respuestas de Infracciones
  @impl true
  def handle_cast({:infraccion_detectada, compra_id}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.warning("Infracción detectada para compra #{compra_id}, reponiendo stock...")

        # Solicitar reposición de stock
        Publisher.publish("ventas.requests", %{
          "request_type" => "reponer_stock",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder al cliente
        GenServer.reply(compra_info.from, {:error, :infraccion_detectada})

        # Limpiar compra en proceso
        nuevo_state = Map.update!(state, :compras_en_proceso, &Map.delete(&1, compra_id))
        {:noreply, nuevo_state}
    end
  end

  @impl true
  def handle_cast({:sin_infraccion, compra_id}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.info("Sin infracción para compra #{compra_id}, procesando pago...")

        # 4. Solicitar procesamiento de pago
        nuevo_state = put_in(state, [:compras_en_proceso, compra_id, :estado], :procesando_pago)

        Publisher.publish("pagos.requests", %{
          "request_type" => "procesar_pago",
          "compra_id" => compra_id,
          "pago_id" => "pago_#{compra_info.producto_id}",
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        {:noreply, nuevo_state}
    end
  end

  # Handlers para respuestas de Pagos
  @impl true
  def handle_cast({:pago_aprobado, compra_id}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.info("Pago aprobado para compra #{compra_id}, procesando envío...")

        # 5. Solicitar procesamiento de envío
        nuevo_state = put_in(state, [:compras_en_proceso, compra_id, :estado], :procesando_envio)

        Publisher.publish("envios.requests", %{
          "request_type" => "procesar_envio",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "forma_entrega" => Atom.to_string(compra_info.forma_entrega),
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        {:noreply, nuevo_state}
    end
  end

  @impl true
  def handle_cast({:pago_rechazado, compra_id}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.warning("Pago rechazado para compra #{compra_id}, reponiendo stock...")

        # Reponer stock
        Publisher.publish("ventas.requests", %{
          "request_type" => "reponer_stock",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder al cliente
        GenServer.reply(compra_info.from, {:error, :pago_rechazado})

        # Limpiar compra en proceso
        nuevo_state = Map.update!(state, :compras_en_proceso, &Map.delete(&1, compra_id))
        {:noreply, nuevo_state}
    end
  end

  # Handlers para respuestas de Envíos
  @impl true
  def handle_cast({:envio_procesado, compra_id, envio}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.info("Envío procesado para compra #{compra_id}, finalizando compra...")

        # Compra exitosa - construir respuesta
        compra = %{
          producto: compra_info.producto,
          medio_pago: compra_info.medio_pago,
          forma_entrega: compra_info.forma_entrega,
          envio: envio,
          timestamp: DateTime.utc_now()
        }

        # Publicar evento de compra realizada
        Publisher.publish("compras.events", %{
          "event_type" => "compra_realizada",
          "compra_id" => compra_id,
          "compra" => %{
            "producto_id" => compra_info.producto.id,
            "medio_pago" => Atom.to_string(compra_info.medio_pago),
            "forma_entrega" => Atom.to_string(compra_info.forma_entrega),
            "envio" => envio
          },
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder al cliente
        GenServer.reply(compra_info.from, {:ok, compra})

        # Actualizar estado
        nuevo_state = Map.update!(state, :compras_realizadas, &[compra | &1])
        nuevo_state = Map.update!(nuevo_state, :compras_en_proceso, &Map.delete(&1, compra_id))

        {:noreply, nuevo_state}
    end
  end

  @impl true
  def handle_cast({:envio_error, compra_id}, state) do
    case Map.get(state.compras_en_proceso, compra_id) do
      nil ->
        {:noreply, state}

      compra_info ->
        Logger.error("Error en envío para compra #{compra_id}, reponiendo stock...")

        # Reponer stock
        Publisher.publish("ventas.requests", %{
          "request_type" => "reponer_stock",
          "compra_id" => compra_id,
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Publicar evento de compra fallida
        Publisher.publish("compras.events", %{
          "event_type" => "compra_fallida",
          "compra_id" => compra_id,
          "motivo" => "error_envio",
          "producto_id" => compra_info.producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder al cliente
        GenServer.reply(compra_info.from, {:error, :error_envio})

        # Limpiar compra en proceso
        nuevo_state = Map.update!(state, :compras_en_proceso, &Map.delete(&1, compra_id))
        {:noreply, nuevo_state}
    end
  end
end

defmodule Libremarket.Compras.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Compras.
  Escucha respuestas de otros servicios y coordina el flujo.
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.Connection

  @exchange "libremarket_exchange"
  @queue "compras_queue"

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
      Logger.info("Consumer de Compras iniciado en cola: #{@queue}")
      {:noreply, Map.put(state, :channel, channel)}
    else
      error ->
        Logger.error("Error configurando consumer de Compras: #{inspect(error)}")
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
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "ventas.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "infracciones.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "pagos.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "envios.responses") do
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

  # Respuestas de Ventas
  defp process_message(%{"response_type" => "stock_verificado", "compra_id" => compra_id, "producto" => producto}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:stock_verificado, compra_id, producto})
  end

  defp process_message(%{"response_type" => "venta_confirmada", "compra_id" => compra_id, "producto" => producto}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:venta_confirmada, compra_id, producto})
  end

  defp process_message(%{"response_type" => "error_stock", "compra_id" => compra_id, "error" => error}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:error_stock, compra_id, error})
  end

  # Respuestas de Infracciones
  defp process_message(%{"response_type" => "infraccion_detectada", "compra_id" => compra_id}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:infraccion_detectada, compra_id})
  end

  defp process_message(%{"response_type" => "sin_infraccion", "compra_id" => compra_id}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:sin_infraccion, compra_id})
  end

  # Respuestas de Pagos
  defp process_message(%{"response_type" => "pago_procesado", "compra_id" => compra_id, "aprobado" => true}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:pago_aprobado, compra_id})
  end

  defp process_message(%{"response_type" => "pago_rechazado", "compra_id" => compra_id}) do
    GenServer.cast({:global, Libremarket.Compras.Server}, {:pago_rechazado, compra_id})
  end

  # Respuestas de Envíos
  defp process_message(%{"response_type" => "envio_procesado", "compra_id" => compra_id, "envio" => envio}) do
    envio_atom = %{
      id_compra: envio["id_compra"],
      forma: String.to_atom(envio["forma"]),
      costo: envio["costo"]
    }
    GenServer.cast({:global, Libremarket.Compras.Server}, {:envio_procesado, compra_id, envio_atom})
  end

  defp process_message(message) do
    Logger.warning("Mensaje no reconocido en Compras Consumer: #{inspect(message)}")
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
