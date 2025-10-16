defmodule Libremarket.Infracciones do
  @moduledoc false

  @doc """
  Detecta infracciones con 30% de probabilidad
  """
  def detectar_infracciones() do
    :rand.uniform(100) <= 30
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer

  @global_name {:global, __MODULE__}

  # API del cliente
  @doc """
  Crea un nuevo servidor de Infracciones.
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def detectar_infracciones(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:detectar_infracciones, id_compra})
  end

  def listar_infracciones(pid \\ @global_name) do
    GenServer.call(pid, :listar_infracciones)
  end

  # Callbacks
  @impl true
  def init(_state) do
    IO.puts("Servidor de Infracciones iniciado")
    {:ok, %{infracciones: %{}}}
  end

  @impl true
  def handle_call({:detectar_infracciones, id_compra}, _from, state) do
    tiene_infraccion = Libremarket.Infracciones.detectar_infracciones()

    nuevo_state =
      if tiene_infraccion do
        nuevas_infracciones = [
          %{id_compra: id_compra, timestamp: DateTime.utc_now()} | state.infracciones
        ]

        %{state | infracciones: nuevas_infracciones}
      else
        state
      end

    {:reply, tiene_infraccion, nuevo_state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state.infracciones, state}
  end
end

defmodule Libremarket.Infracciones.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Infracciones
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "infracciones_queue"

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
      Logger.info("Consumer de Infracciones iniciado en cola: #{@queue}")
      {:noreply, Map.put(state, :channel, channel)}
    else
      error ->
        Logger.error("Error configurando consumer de Infracciones: #{inspect(error)}")
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
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "infracciones.requests") do
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

  defp process_message(%{
    "request_type" => "detectar_infracciones",
    "compra_id" => compra_id,
    "producto_id" => producto_id
  }) do
    Logger.info("Detectando infracciones para compra #{compra_id}, producto: #{producto_id}")

    try do
      tiene_infraccion = Libremarket.Infracciones.Server.detectar_infracciones(producto_id)

      if tiene_infraccion do
        # Publicar evento de infracción detectada
        Publisher.publish("infracciones.events", %{
          "event_type" => "infraccion_detectada",
          "compra_id" => compra_id,
          "producto_id" => producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        Logger.warning("¡Infracción detectada! Compra: #{compra_id}, Producto: #{producto_id}")
      else
        Logger.info("Sin infracciones para compra #{compra_id}")
      end

      # Responder a compras
      Publisher.publish("infracciones.responses", %{
        "response_type" => "infracciones_detectadas",
        "compra_id" => compra_id,
        "tiene_infraccion" => tiene_infraccion,
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

    rescue
      e ->
        Logger.error("Error detectando infracciones: #{inspect(e)}")

        # En caso de error, reportar como sin infracciones para no bloquear
        Publisher.publish("infracciones.responses", %{
          "response_type" => "infracciones_detectadas",
          "compra_id" => compra_id,
          "tiene_infraccion" => false,
          "error" => "Error en detección",
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
  end

  defp process_message(message) do
    Logger.warning("Mensaje no reconocido en Infracciones Consumer: #{inspect(message)}")
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
