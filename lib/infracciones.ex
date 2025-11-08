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
  Servidor de Infracciones con Replicación y ZooKeeper.

  - ZooKeeper determina quién es el líder
  - Solo el líder procesa mensajes AMQP
  - Todos los nodos pueden responder lecturas
  - El líder replica a los seguidores
  """
  use GenServer
  require Logger

  @service_name :infracciones
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

  def promote_to_leader() do
    GenServer.cast(__MODULE__, :promote_to_leader)
  end

  def demote_to_follower() do
    GenServer.cast(__MODULE__, :demote_to_follower)
  end

  # Server Callbacks

  @impl true
  def init(_state) do
    Logger.info("""
    Servidor de Infracciones iniciado en nodo #{Node.self()}
    Esperando elección de ZooKeeper...
    """)

    {:ok, %{
      infracciones: [],
      replicated_from: nil,
      last_replication: nil,
      is_leader: false,
      consumer_pid: nil
    }}
  end

  @impl true
  def handle_call({:detectar_infracciones, id_compra}, _from, state) do
    # Solo el líder puede detectar infracciones
    if not state.is_leader do
      Logger.warning("Intento de escritura en seguidor - operación rechazada")
      {:reply, {:error, :not_leader}, state}
    else
      tiene_infraccion = Libremarket.Infracciones.detectar_infracciones()

      nuevo_state =
        if tiene_infraccion do
          nuevas_infracciones = [
            %{id_compra: id_compra, timestamp: DateTime.utc_now(), node: Node.self()}
            | state.infracciones
          ]

          # Replicar inmediatamente a los seguidores
          spawn(fn -> replicate_to_followers(nuevas_infracciones) end)

          %{state | infracciones: nuevas_infracciones}
        else
          state
        end

      {:reply, tiene_infraccion, nuevo_state}
    end
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state.infracciones, state}
  end

  @impl true
  def handle_call(:get_replication_info, _from, state) do
    info = %{
      node: Node.self(),
      is_leader: state.is_leader,
      infracciones_count: length(state.infracciones),
      replicated_from: state.replicated_from,
      last_replication: state.last_replication
    }
    {:reply, info, state}
  end

  @impl true
  def handle_call({:replicate_state, infracciones, from_node}, _from, state) do
    # Los seguidores aceptan replicación
    if not state.is_leader do
      if length(infracciones) > 0 do
        Logger.info("Replicando estado desde líder #{from_node}: #{length(infracciones)} infracciones")
      end

      new_state = %{state |
        infracciones: infracciones,
        replicated_from: from_node,
        last_replication: DateTime.utc_now()
      }

      {:reply, :ok, new_state}
    else
      {:reply, {:error, :is_leader}, state}
    end
  end

  @impl true
  def handle_cast(:promote_to_leader, state) do
    Logger.info("🎯 PROMOVIDO A LÍDER - Iniciando Consumer AMQP")

    # Iniciar el Consumer AMQP
    {:ok, consumer_pid} = Libremarket.Infracciones.Consumer.start_link([])

    # Iniciar replicación periódica
    Process.send_after(self(), :replicate_state, @replication_interval)

    {:noreply, %{state | is_leader: true, consumer_pid: consumer_pid}}
  end

  @impl true
  def handle_cast(:demote_to_follower, state) do
    Logger.info("📋 DEGRADADO A SEGUIDOR - Deteniendo Consumer AMQP")

    # Detener el Consumer AMQP si existe
    if state.consumer_pid && Process.alive?(state.consumer_pid) do
      Process.exit(state.consumer_pid, :normal)
    end

    {:noreply, %{state | is_leader: false, consumer_pid: nil}}
  end

  @impl true
  def handle_info(:replicate_state, state) do
    # Solo el líder replica
    if state.is_leader do
      replicate_to_followers(state.infracciones)
      Process.send_after(self(), :replicate_state, @replication_interval)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private Functions

  defp replicate_to_followers(infracciones) do
    follower_nodes = get_follower_nodes()

    if not Enum.empty?(follower_nodes) and length(infracciones) > 0 do
      Logger.info("Replicando #{length(infracciones)} infracciones a #{length(follower_nodes)} seguidores")

      Enum.each(follower_nodes, fn node ->
        spawn(fn ->
          try do
            result = :rpc.call(node, GenServer, :call, [
              __MODULE__,
              {:replicate_state, infracciones, Node.self()}
            ], 3_000)

            case result do
              :ok ->
                Logger.debug("✓ Replicación exitosa a #{node}")
              {:error, reason} ->
                Logger.error("Error en replicación a #{node}: #{inspect(reason)}")
              other ->
                Logger.warning("Respuesta inesperada de #{node}: #{inspect(other)}")
            end
          catch
            kind, error ->
              Logger.error("Error replicando a #{node}: #{kind} - #{inspect(error)}")
          end
        end)
      end)
    end
  end

  defp get_follower_nodes() do
    all_nodes = Node.list()

    Enum.filter(all_nodes, fn node ->
      String.contains?(Atom.to_string(node), "infracciones")
    end)
  end
end

defmodule Libremarket.Infracciones.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Infracciones.

  IMPORTANTE: Este Consumer se inicia/detiene dinámicamente según
  si el nodo es líder o seguidor (determinado por ZooKeeper).
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "infracciones_queue"

  def start_link(opts \\ []) do
    Logger.info("Iniciando Consumer de Infracciones - Este nodo es LÍDER")
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
        Publisher.publish("infracciones.events", %{
          "event_type" => "infraccion_detectada",
          "compra_id" => compra_id,
          "producto_id" => producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        Publisher.publish("infracciones.responses", %{
          "response_type" => "infraccion_detectada",
          "compra_id" => compra_id,
          "tiene_infraccion" => true,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      false ->
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
