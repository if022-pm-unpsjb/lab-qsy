defmodule Libremarket.Envios do
  @moduledoc false

  # Simula cálculo del costo de envío
  def calcular(:retira), do: 0
  def calcular(:correo), do: Enum.random(500..1500)
end

defmodule Libremarket.Envios.Server do
  @moduledoc """
  Servidor de Envíos
  """

  use GenServer

  @global_name {:global, __MODULE__}

  @doc "Arranca el servidor de Envíos."
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  @doc """
  Procesa un envío para un `idCompra` con una `forma_entrega` (:retira | :correo).
  Devuelve un mapa con el resultado del envío y su costo.
  """
  def procesarEnvio(pid \\ @global_name, idCompra, forma_entrega) do
    GenServer.call(pid, {:procesarEnvio, idCompra, forma_entrega})
  end

  @doc """
  Lista todos los envíos procesados.
  """
  def listarEnvios(pid \\ @global_name) do
    GenServer.call(pid, :listarEnvios)
  end

  # =========
  # Callbacks
  # =========

  @impl true
  def init(_opts), do: {:ok, %{}}

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
end

defmodule Libremarket.Envios.Consumer do
  @moduledoc """
  Consumer AMQP específico para el servicio de Envíos
  Escucha mensajes de envios.requests y publica respuestas
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "envios_queue"

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
      Logger.info("Consumer de Envíos iniciado en cola: #{@queue}")
      {:noreply, Map.put(state, :channel, channel)}
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
        Logger.error("Error decodificando mensaje en Envíos: #{inspect(reason)}")
        nack_message(meta)
    end
  end

  defp process_message(%{
    "request_type" => "procesar_envio",
    "compra_id" => compra_id,
    "producto_id" => producto_id,
    "forma_entrega" => forma_entrega
  }) do
    Logger.info("Procesando envío para compra #{compra_id}, forma: #{forma_entrega}")

    try do
      forma_atom = String.to_existing_atom(forma_entrega)
      envio = Libremarket.Envios.Server.procesarEnvio(producto_id, forma_atom)

      # Publicar evento de envío creado
      Publisher.publish("envios.events", %{
        "event_type" => "envio_creado",
        "compra_id" => compra_id,
        "envio" => %{
          "id_compra" => envio.id_compra,
          "forma" => Atom.to_string(envio.forma),
          "costo" => envio.costo
        },
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

      # Responder a compras con el envío procesado
      Publisher.publish("compras.responses", %{
        "response_type" => "envio_procesado",
        "compra_id" => compra_id,
        "success" => true,
        "envio" => %{
          "id_compra" => envio.id_compra,
          "forma" => Atom.to_string(envio.forma),
          "costo" => envio.costo
        },
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

      Logger.info("Envío procesado exitosamente para compra #{compra_id}: #{inspect(envio)}")

    rescue
      e ->
        Logger.error("Error procesando envío para compra #{compra_id}: #{inspect(e)}")

        # Publicar error
        Publisher.publish("compras.responses", %{
          "response_type" => "envio_procesado",
          "compra_id" => compra_id,
          "success" => false,
          "error" => "error_envio",
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
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
end
