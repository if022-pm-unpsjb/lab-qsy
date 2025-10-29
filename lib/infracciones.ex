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
  Servidor de Infracciones con Replicación Manual.

  PRIMARIO (IS_REPLICA=false):
  - Procesa mensajes AMQP
  - Detecta infracciones (escrituras)
  - Replica estado a las réplicas
  - Responde lecturas

  RÉPLICA (IS_REPLICA=true):
  - NO procesa mensajes AMQP
  - Solo recibe replicación de estado del primario
  - Responde lecturas
  """
  use GenServer
  require Logger

  @replication_interval 2_000

  # Client API

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectar_infracciones(id_compra) do
    case Process.whereis(__MODULE__) do
      nil ->
        {:error, :server_not_running}
      _pid ->
        GenServer.call(__MODULE__, {:detectar_infracciones, id_compra})
    end
  end

  def listar_infracciones() do
    case Process.whereis(__MODULE__) do
      nil ->
        []
      _pid ->
        GenServer.call(__MODULE__, :listar_infracciones)
    end
  end

  def get_replication_info() do
    case Process.whereis(__MODULE__) do
      nil ->
        {:error, :server_not_running}
      _pid ->
        GenServer.call(__MODULE__, :get_replication_info)
    end
  end

  # Server Callbacks

  @impl true
  def init(_state) do
    is_replica = System.get_env("IS_REPLICA") == "true"

    Logger.info("""
    Servidor de Infracciones iniciado en nodo #{Node.self()}
    Modo: #{if is_replica, do: "RÉPLICA (solo recibe estado)", else: "PRIMARIO (procesa mensajes)"}
    """)

    # Programar replicación periódica solo si somos primario
    if not is_replica do
      Process.send_after(self(), :replicate_state, @replication_interval)
    end

    {:ok, %{
      infracciones: [],
      replicated_from: nil,
      last_replication: nil,
      is_replica: is_replica
    }}
  end

  @impl true
  def handle_call({:detectar_infracciones, id_compra}, _from, state) do
    # Solo el primario puede detectar infracciones
    if state.is_replica do
      Logger.warning("Intento de escritura en réplica - operación rechazada")
      {:reply, {:error, :not_primary}, state}
    else
      tiene_infraccion = Libremarket.Infracciones.detectar_infracciones()

      nuevo_state =
        if tiene_infraccion do
          nuevas_infracciones = [
            %{id_compra: id_compra, timestamp: DateTime.utc_now(), node: Node.self()}
            | state.infracciones
          ]

          # Replicar inmediatamente a las réplicas
          spawn(fn -> replicate_to_replicas(nuevas_infracciones) end)

          %{state | infracciones: nuevas_infracciones}
        else
          state
        end

      {:reply, tiene_infraccion, nuevo_state}
    end
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    # Las lecturas pueden ejecutarse en cualquier nodo (primario o réplica)
    {:reply, state.infracciones, state}
  end

  @impl true
  def handle_call(:get_replication_info, _from, state) do
    info = %{
      node: Node.self(),
      is_replica: state.is_replica,
      infracciones_count: length(state.infracciones),
      replicated_from: state.replicated_from,
      last_replication: state.last_replication
    }
    {:reply, info, state}
  end

  @impl true
  def handle_call({:replicate_state, infracciones, from_node}, _from, state) do
    # Las réplicas aceptan replicación
    if state.is_replica do
      Logger.info("Replicando estado desde primario #{from_node}: #{length(infracciones)} infracciones")

      new_state = %{state |
        infracciones: infracciones,
        replicated_from: from_node,
        last_replication: DateTime.utc_now()
      }

      {:reply, :ok, new_state}
    else
      # El primario no necesita recibir replicación
      {:reply, {:error, :is_primary}, state}
    end
  end

  @impl true
  def handle_info(:replicate_state, state) do
    # Solo el primario replica
    if not state.is_replica do
      replicate_to_replicas(state.infracciones)
    end

    # Programar siguiente replicación solo si somos primario
    if not state.is_replica do
      Process.send_after(self(), :replicate_state, @replication_interval)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private Functions

  defp replicate_to_replicas(infracciones) do
    replica_nodes = get_replica_nodes()

    if Enum.empty?(replica_nodes) do
      Logger.debug("No hay réplicas disponibles para replicar")
    else
      Logger.info("Replicando a #{length(replica_nodes)} réplicas")

      Enum.each(replica_nodes, fn node ->
        spawn(fn ->
          try do
            :rpc.call(node, GenServer, :call, [
              __MODULE__,
              {:replicate_state, infracciones, Node.self()}
            ], 3_000)
          catch
            _, error ->
              Logger.error("Error replicando a #{node}: #{inspect(error)}")
          end
        end)
      end)
    end
  end

  defp get_replica_nodes() do
    # Obtener todos los nodos conectados
    all_nodes = Node.list()

    # Filtrar solo los nodos que tienen el servidor de infracciones
    Enum.filter(all_nodes, fn node ->
      # Verificar si el nodo tiene el proceso de infracciones
      String.contains?(Atom.to_string(node), "infracciones")
    end)
  end
end

defmodule Libremarket.Infracciones.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Infracciones.

  IMPORTANTE: Este Consumer SOLO se inicia en el nodo PRIMARIO.
  Las réplicas NO tienen Consumer y por lo tanto NO procesan mensajes AMQP.
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "infracciones_queue"

  def start_link(opts \\ []) do
    Logger.info("Iniciando Consumer de Infracciones - Este nodo procesará mensajes AMQP")
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

  defp process_message(%{"request_type" => "detectar_infracciones", "compra_id" => compra_id, "producto_id" => producto_id}) do
    Logger.info("Consumer procesando: detectar infracciones para compra #{compra_id}, producto #{producto_id}")

    tiene_infraccion = Libremarket.Infracciones.Server.detectar_infracciones(producto_id)

    case tiene_infraccion do
      {:error, reason} ->
        Logger.error("Error detectando infracciones: #{inspect(reason)}")

      true ->
        # Publicar evento de infracción
        Publisher.publish("infracciones.events", %{
          "event_type" => "infraccion_detectada",
          "compra_id" => compra_id,
          "producto_id" => producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder a compras
        Publisher.publish("infracciones.responses", %{
          "response_type" => "infraccion_detectada",
          "compra_id" => compra_id,
          "tiene_infraccion" => true,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      false ->
        # No hay infracción, continuar
        Publisher.publish("infracciones.responses", %{
          "response_type" => "sin_infraccion",
          "compra_id" => compra_id,
          "tiene_infraccion" => false,
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
