defmodule Libremarket.Compras do
  @doc """
  Lógica principal del proceso de compra.
  """
  alias Libremarket.AMQP.Publisher
  def procesar_compra(producto_id, medio_pago, forma_entrega) do
    # 1. Verificar stock
    case Libremarket.Ventas.Server.verificar_producto(producto_id) do
      {:error, :sin_stock} ->
        {:error, :sin_stock}

      {:error, :producto_no_encontrado} ->
        {:error, :producto_no_encontrado}

      {:ok, _producto} ->
        # 2. Confirmar compra y reservar producto
        case Libremarket.Ventas.Server.confirmar_venta(producto_id) do
          {:error, reason} ->
            {:error, reason}

          {:ok, producto_reservado} ->
            # 3. Detectar infracciones
            case Libremarket.Infracciones.Server.detectar_infracciones(producto_id) do
              true ->
                # Liberar producto (devolvemos stock +1)
                reponer_producto(producto_reservado.id)

                # Publicar evento de infracción
                Publisher.publish("infracciones.events", %{
                  "event_type" => "infraccion_detectada",
                  "compra_id" => "compra_#{:rand.uniform(10000)}",
                  "producto_id" => producto_id,
                  "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                })

                {:error, :infraccion_detectada}

              false ->
                # 4. Procesar pago
                case Libremarket.Pagos.Server.procesarPago(producto_id) do
                  false ->
                    reponer_producto(producto_reservado.id)

                    # Publicar evento de pago rechazado
                    Publisher.publish("pagos.events", %{
                      "event_type" => "pago_rechazado",
                      "pago_id" => "pago_#{producto_id}",
                      "producto_id" => producto_id,
                      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                    })

                    {:error, :pago_rechazado}

                  true ->
                    # Publicar evento de pago procesado
                    Publisher.publish("pagos.events", %{
                      "event_type" => "pago_procesado",
                      "pago_id" => "pago_#{producto_id}",
                      "resultado" => "aprobado",
                      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                    })

                    # 5. Registrar envío
                    case Libremarket.Envios.Server.procesarEnvio(producto_id, forma_entrega) do
                      envio when is_map(envio) ->
                        # Publicar evento de envío creado
                        Publisher.publish("envios.events", %{
                          "event_type" => "envio_creado",
                          "envio" => envio,
                          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                        })

                        compra = %{
                          producto: producto_reservado,
                          medio_pago: medio_pago,
                          forma_entrega: forma_entrega,
                          envio: envio,
                          timestamp: DateTime.utc_now()
                        }

                        # Publicar evento de compra realizada
                        Publisher.publish("compras.events", %{
                          "event_type" => "compra_realizada",
                          "compra" => compra,
                          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                        })

                        {:ok, compra}

                      _error ->
                        reponer_producto(producto_reservado.id)

                        # Publicar evento de compra fallida
                        Publisher.publish("compras.events", %{
                          "event_type" => "compra_fallida",
                          "motivo" => "error_envio",
                          "producto_id" => producto_id,
                          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
                        })

                        {:error, :error_envio}
                    end
                end
            end
        end
    end
  end

  # Función auxiliar para reponer stock en caso de infracción
  defp reponer_producto(producto_id) do
    # Sencillo: reducimos -1 en confirmar_venta, acá sumamos +1 manual
    try do
      GenServer.cast(Libremarket.Ventas.Server, {:reponer_stock, producto_id})
    rescue
      error ->
        IO.puts("Error reponiendo stock: #{inspect(error)}")
    end
  end
end

defmodule Libremarket.Compras.Server do
  use GenServer

  @global_name {:global, __MODULE__}

  # API del cliente
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def comprar(pid \\ @global_name, producto_id, medio_pago, forma_entrega) do
    GenServer.call(pid, {:comprar, producto_id, medio_pago, forma_entrega}, 10000)
  end

  @impl true
  def init(state) do
    IO.puts("Servidor de Compras iniciado")
    {:ok, Map.put(state, :compras_realizadas, [])}
  end

  @impl true
  def handle_call({:comprar, producto_id, medio_pago, forma_entrega}, _from, state) do
    resultado = Libremarket.Compras.procesar_compra(producto_id, medio_pago, forma_entrega)

    nuevo_state =
      case resultado do
        {:ok, compra} ->
          %{state | compras_realizadas: [compra | state.compras_realizadas]}

        _ ->
          state
      end

    {:reply, resultado, nuevo_state}
  end

  @impl true
  def handle_call(:listar_compras, _from, state) do
    {:reply, state.compras_realizadas, state}
  end
end

defmodule Libremarket.Compras.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Compras.
  Escucha respuestas de otros servicios y actualiza el estado de las compras.
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
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "pagos.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "envios.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "infracciones.responses"),
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "infracciones.events") do
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
    Logger.info("Compras Consumer recibió mensaje de #{routing_key}: #{inspect(message)}")

    case routing_key do
      "ventas.responses" -> handle_ventas_response(message)
      "infracciones.responses" -> handle_infracciones_response(message)
      "pagos.responses" -> handle_pagos_response(message)
      "envios.responses" -> handle_envios_response(message)
      "infracciones.events" -> handle_infracciones_event(message)
      _ -> Logger.warning("Routing key no reconocido: #{routing_key}")
    end
  end

  # ========================================
  # MANEJO DE RESPUESTAS DE VENTAS
  # ========================================

  defp handle_ventas_response(%{
    "response_type" => "stock_verificado",
    "compra_id" => compra_id,
    "producto" => producto
  }) do
    Logger.info("Stock verificado para compra #{compra_id}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:stock_verificado, compra_id, producto}
    )
  end

  defp handle_ventas_response(%{
    "response_type" => "venta_confirmada",
    "compra_id" => compra_id,
    "producto" => producto
  }) do
    Logger.info("Venta confirmada para compra #{compra_id}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:venta_confirmada, compra_id, producto}
    )
  end

  defp handle_ventas_response(%{
    "response_type" => "error_stock",
    "compra_id" => compra_id,
    "error" => reason
  }) do
    Logger.warning("Error de stock para compra #{compra_id}: #{reason}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:error_stock, compra_id, reason}
    )
  end

  defp handle_ventas_response(_), do: :ok

  # ========================================
  # MANEJO DE RESPUESTAS DE INFRACCIONES
  # ========================================

  defp handle_infracciones_response(%{
    "response_type" => "infracciones_detectadas",
    "compra_id" => compra_id,
    "tiene_infraccion" => tiene_infraccion
  }) do
    Logger.info("Infracciones detectadas para compra #{compra_id}: #{tiene_infraccion}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:infracciones_detectadas, compra_id, tiene_infraccion}
    )
  end

  defp handle_infracciones_response(_), do: :ok

  defp handle_infracciones_event(%{
    "event_type" => "infraccion_detectada",
    "compra_id" => compra_id
  }) do
    Logger.warning("Evento de infracción detectada para compra #{compra_id}")
  end

  defp handle_infracciones_event(_), do: :ok

  # ========================================
  # MANEJO DE RESPUESTAS DE PAGOS
  # ========================================

  defp handle_pagos_response(%{
    "response_type" => "pago_procesado",
    "compra_id" => compra_id,
    "resultado" => resultado
  }) do
    Logger.info("Pago procesado para compra #{compra_id}: #{resultado}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:pago_procesado, compra_id, resultado}
    )
  end

  defp handle_pagos_response(_), do: :ok

  # ========================================
  # MANEJO DE RESPUESTAS DE ENVÍOS
  # ========================================

  defp handle_envios_response(%{
    "response_type" => "envio_procesado",
    "compra_id" => compra_id,
    "envio" => envio
  }) do
    Logger.info("Envío procesado para compra #{compra_id}")

    # Convertir envío de mapa con strings a átomos
    envio_atom = %{
      id_compra: envio["id_compra"],
      forma: String.to_existing_atom(envio["forma"]),
      costo: envio["costo"]
    }

    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:envio_procesado, compra_id, envio_atom}
    )
  end

  defp handle_envios_response(%{
    "response_type" => "error_envio",
    "compra_id" => compra_id
  }) do
    Logger.error("Error procesando envío para compra #{compra_id}")
    GenServer.cast(
      {:global, Libremarket.Compras.Server},
      {:envio_procesado, compra_id, nil}
    )
  end

  defp handle_envios_response(_), do: :ok

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
