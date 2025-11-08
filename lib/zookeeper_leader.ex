defmodule Libremarket.ZookeeperLeader do
  @moduledoc """
  Cliente Zookeeper para elección de líder distribuido.
  
  Utiliza Zookeeper para coordinar la elección de líder entre múltiples nodos
  de un mismo servicio. Solo el líder procesará mensajes AMQP.
  """
  use GenServer
  require Logger

  @zk_hosts [{'zookeeper', 2181}]
  @session_timeout 10_000
  @election_path "/libremarket/leader"

  defmodule State do
    defstruct [
      :pid,
      :service_name,
      :node_id,
      :is_leader,
      :election_node,
      :on_leader_change,
      :connected
    ]
  end

  # Client API

  def start_link(opts) do
    service_name = Keyword.fetch!(opts, :service_name)
    on_leader_change = Keyword.get(opts, :on_leader_change)
    
    GenServer.start_link(__MODULE__, 
      %{service_name: service_name, on_leader_change: on_leader_change}, 
      name: via_tuple(service_name))
  end

  def is_leader?(service_name) do
    try do
      GenServer.call(via_tuple(service_name), :is_leader, 5_000)
    catch
      :exit, _ -> false
    end
  end

  def get_leader_info(service_name) do
    try do
      GenServer.call(via_tuple(service_name), :get_leader_info, 5_000)
    catch
      :exit, _ -> {:error, :not_available}
    end
  end

  # Server Callbacks

  @impl true
  def init(%{service_name: service_name, on_leader_change: on_leader_change}) do
    node_id = "#{service_name}_#{Node.self()}_#{:erlang.unique_integer([:positive])}"
    
    Logger.info("""
    [ZK Leader] Iniciando elección de líder para servicio: #{service_name}
    Node ID: #{node_id}
    """)

    state = %State{
      service_name: service_name,
      node_id: node_id,
      is_leader: false,
      on_leader_change: on_leader_change,
      connected: false
    }

    # Conectar a Zookeeper de forma asíncrona
    send(self(), :connect_zookeeper)

    {:ok, state}
  end

  @impl true
  def handle_call(:is_leader, _from, state) do
    {:reply, state.is_leader && state.connected, state}
  end

  @impl true
  def handle_call(:get_leader_info, _from, state) do
    info = %{
      service: state.service_name,
      node_id: state.node_id,
      is_leader: state.is_leader,
      connected: state.connected,
      election_node: state.election_node
    }
    {:reply, {:ok, info}, state}
  end

  @impl true
  def handle_info(:connect_zookeeper, state) do
    case :erlzk.connect(@zk_hosts, @session_timeout, [
      monitor: self(),
      chroot: ""
    ]) do
      {:ok, pid} ->
        Logger.info("[ZK Leader] Conectado a Zookeeper exitosamente")
        new_state = %{state | pid: pid, connected: true}
        send(self(), :participate_in_election)
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("[ZK Leader] Error conectando a Zookeeper: #{inspect(reason)}")
        Logger.info("[ZK Leader] Reintentando conexión en 5 segundos...")
        Process.send_after(self(), :connect_zookeeper, 5_000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:participate_in_election, state) do
    service_path = "#{@election_path}/#{state.service_name}"
    
    # Crear el path base si no existe
    ensure_path_exists(state.pid, service_path)

    # Crear nodo efímero secuencial para la elección
    election_node_path = "#{service_path}/node-"
    
    case :erlzk.create(state.pid, election_node_path, state.node_id, [
      :ephemeral,
      :sequence
    ]) do
      {:ok, created_path} ->
        Logger.info("[ZK Leader] Nodo de elección creado: #{created_path}")
        new_state = %{state | election_node: created_path}
        send(self(), :check_leadership)
        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("[ZK Leader] Error creando nodo de elección: #{inspect(reason)}")
        Process.send_after(self(), :participate_in_election, 5_000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:check_leadership, state) do
    service_path = "#{@election_path}/#{state.service_name}"
    
    case :erlzk.get_children(state.pid, service_path, self()) do
      {:ok, children} ->
        sorted_children = Enum.sort(children)
        my_node = Path.basename(state.election_node)
        
        is_leader = List.first(sorted_children) == my_node
        
        if is_leader != state.is_leader do
          Logger.info("""
          [ZK Leader] Cambio de liderazgo detectado
          Servicio: #{state.service_name}
          Nodo: #{state.node_id}
          Es líder: #{is_leader}
          Nodos participantes: #{inspect(sorted_children)}
          """)

          # Notificar cambio de liderazgo
          if state.on_leader_change do
            spawn(fn -> state.on_leader_change.(is_leader) end)
          end
        end

        {:noreply, %{state | is_leader: is_leader}}

      {:error, reason} ->
        Logger.error("[ZK Leader] Error verificando liderazgo: #{inspect(reason)}")
        Process.send_after(self(), :check_leadership, 5_000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:node_children_changed, _path}, state) do
    Logger.debug("[ZK Leader] Cambio en nodos detectado, verificando liderazgo...")
    send(self(), :check_leadership)
    {:noreply, state}
  end

  @impl true
  def handle_info({:disconnected, _pid}, state) do
    Logger.warning("[ZK Leader] Desconectado de Zookeeper")
    new_state = %{state | connected: false, is_leader: false}
    
    # Notificar pérdida de liderazgo
    if state.is_leader && state.on_leader_change do
      spawn(fn -> state.on_leader_change.(false) end)
    end
    
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:connected, _pid}, state) do
    Logger.info("[ZK Leader] Reconectado a Zookeeper")
    new_state = %{state | connected: true}
    send(self(), :participate_in_election)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:expired, _pid}, state) do
    Logger.warning("[ZK Leader] Sesión de Zookeeper expirada, reconectando...")
    send(self(), :connect_zookeeper)
    {:noreply, %{state | connected: false, is_leader: false, pid: nil}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("[ZK Leader] Mensaje no manejado: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private Functions

  defp via_tuple(service_name) do
    {:via, Registry, {Libremarket.Registry, {:zk_leader, service_name}}}
  end

  defp ensure_path_exists(pid, path) do
    parts = String.split(path, "/", trim: true)
    
    Enum.reduce(parts, "", fn part, acc ->
      current_path = "#{acc}/#{part}"
      
      case :erlzk.create(pid, current_path, "", [:persistent]) do
        {:ok, _} -> 
          Logger.debug("[ZK Leader] Path creado: #{current_path}")
          current_path
        {:error, :node_exists} -> 
          current_path
        {:error, reason} ->
          Logger.error("[ZK Leader] Error creando path #{current_path}: #{inspect(reason)}")
          current_path
      end
    end)
  end
end
