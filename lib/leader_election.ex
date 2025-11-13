defmodule Libremarket.LeaderElection do
  @moduledoc """
  Implementación de elección de líder usando Bully Algorithm.

  Cada servicio tiene múltiples nodos que participan en la elección.
  El nodo con mayor prioridad (basado en node name) se convierte en líder.
  """
  use GenServer
  require Logger

  @election_timeout 5_000
  @heartbeat_interval 3_000
  @coordinator_check_interval 2_000

  defmodule State do
    defstruct [
      :service_name,
      :node_priority,
      :is_leader,
      :current_leader,
      :on_leader_change,
      :election_in_progress,
      :last_heartbeat,
      :participant_nodes
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

  def start_election(service_name) do
    try do
      GenServer.cast(via_tuple(service_name), :start_election)
    catch
      :exit, _ -> :ok
    end
  end

  # Server Callbacks

  @impl true
  def init(%{service_name: service_name, on_leader_change: on_leader_change}) do
    node_priority = calculate_priority(Node.self())

    Logger.info("""
    [Leader Election] Iniciando elección de líder para servicio: #{service_name}
    Nodo: #{Node.self()}
    Prioridad: #{node_priority}
    """)

    state = %State{
      service_name: service_name,
      node_priority: node_priority,
      is_leader: false,
      current_leader: nil,
      on_leader_change: on_leader_change,
      election_in_progress: false,
      last_heartbeat: DateTime.utc_now(),
      participant_nodes: []
    }

    send(self(), :discover_nodes)
    Process.send_after(self(), :start_election, 1_000)
    Process.send_after(self(), :check_leader, @coordinator_check_interval)

    {:ok, state}
  end

  @impl true
  def handle_call(:is_leader, _from, state) do
    {:reply, state.is_leader, state}
  end

  @impl true
  def handle_call(:get_leader_info, _from, state) do
    info = %{
      service: state.service_name,
      node: Node.self(),
      priority: state.node_priority,
      is_leader: state.is_leader,
      current_leader: state.current_leader,
      participant_nodes: state.participant_nodes
    }
    {:reply, {:ok, info}, state}
  end

  @impl true
  def handle_cast(:start_election, state) do
    new_state = initiate_election(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:election, from_node, from_priority}, state) do
    Logger.debug("[Leader Election] Recibida elección desde #{from_node} (prioridad: #{from_priority})")

    if from_priority < state.node_priority do
      send_message(from_node, state.service_name, {:ok, Node.self(), state.node_priority})
      new_state = initiate_election(state)
      {:noreply, new_state}
    else
      send_message(from_node, state.service_name, {:ok, Node.self(), state.node_priority})
      {:noreply, %{state | election_in_progress: true}}
    end
  end

  @impl true
  def handle_cast({:ok, _from_node, _from_priority}, state) do
    {:noreply, %{state | election_in_progress: true}}
  end

  @impl true
  def handle_cast({:coordinator, leader_node, leader_priority}, state) do
    Logger.info("[Leader Election] Nuevo líder: #{leader_node} (prioridad: #{leader_priority})")

    was_leader = state.is_leader
    is_leader = (leader_node == Node.self())

    new_state = %{state |
      current_leader: leader_node,
      is_leader: is_leader,
      election_in_progress: false,
      last_heartbeat: DateTime.utc_now()
    }

    if was_leader != is_leader and state.on_leader_change do
      spawn(fn -> state.on_leader_change.(is_leader) end)
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:heartbeat, leader_node}, state) do
    if state.current_leader == leader_node do
      {:noreply, %{state | last_heartbeat: DateTime.utc_now()}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:discover_nodes, state) do
    all_nodes = [Node.self() | Node.list()]

    participant_nodes = Enum.filter(all_nodes, fn node ->
      node_str = Atom.to_string(node)
      String.contains?(node_str, state.service_name)
    end)

    Logger.debug("[Leader Election] Nodos participantes: #{inspect(participant_nodes)}")

    Process.send_after(self(), :discover_nodes, 10_000)

    {:noreply, %{state | participant_nodes: participant_nodes}}
  end

  @impl true
  def handle_info(:start_election, state) do
    new_state = initiate_election(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:check_leader, state) do
    new_state = check_leader_alive(state)
    Process.send_after(self(), :check_leader, @coordinator_check_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:send_heartbeat, state) do
    if state.is_leader do
      broadcast_message(state.participant_nodes, state.service_name,
                       {:heartbeat, Node.self()})
      Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:election_timeout, state) do
    if state.election_in_progress do
      Logger.info("[Leader Election] Timeout de elección, convirtiéndose en líder")
      new_state = become_leader(state)
      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("[Leader Election] Mensaje no manejado: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private Functions

  defp via_tuple(service_name) do
    {:via, Registry, {Libremarket.Registry, {:leader_election, service_name}}}
  end

  defp calculate_priority(node) do
    node_str = Atom.to_string(node)

    case Regex.run(~r/(\d+)/, node_str) do
      [_, num] -> String.to_integer(num)
      _ -> :erlang.phash2(node)
    end
  end

  defp initiate_election(state) do
    Logger.info("[Leader Election] Iniciando elección en nodo #{Node.self()}")

    higher_priority_nodes = Enum.filter(state.participant_nodes, fn node ->
      node != Node.self() and calculate_priority(node) > state.node_priority
    end)

    if Enum.empty?(higher_priority_nodes) do
      Logger.info("[Leader Election] Sin nodos de mayor prioridad, convirtiéndose en líder")
      become_leader(state)
    else
      Logger.debug("[Leader Election] Enviando elección a: #{inspect(higher_priority_nodes)}")

      Enum.each(higher_priority_nodes, fn node ->
        send_message(node, state.service_name, {:election, Node.self(), state.node_priority})
      end)

      Process.send_after(self(), :election_timeout, @election_timeout)

      %{state | election_in_progress: true}
    end
  end

  defp become_leader(state) do
    Logger.info("[Leader Election] #{Node.self()} es ahora el LÍDER")

    broadcast_message(state.participant_nodes, state.service_name,
                     {:coordinator, Node.self(), state.node_priority})

    Process.send_after(self(), :send_heartbeat, @heartbeat_interval)

    was_leader = state.is_leader
    new_state = %{state |
      is_leader: true,
      current_leader: Node.self(),
      election_in_progress: false,
      last_heartbeat: DateTime.utc_now()
    }

    if not was_leader and state.on_leader_change do
      spawn(fn -> state.on_leader_change.(true) end)
    end

    new_state
  end

  defp check_leader_alive(state) do
    if not state.is_leader and state.current_leader do
      time_since_heartbeat = DateTime.diff(DateTime.utc_now(), state.last_heartbeat, :millisecond)

      if time_since_heartbeat > (@heartbeat_interval * 2) do
        Logger.warning("[Leader Election] Líder no responde, iniciando nueva elección")
        initiate_election(%{state | current_leader: nil, is_leader: false})
      else
        state
      end
    else
      state
    end
  end

  defp send_message(target_node, service_name, message) do
    spawn(fn ->
      try do
        target_name = {:via, Registry, {Libremarket.Registry, {:leader_election, service_name}}}
        :rpc.call(target_node, GenServer, :cast, [target_name, message], 2_000)
      catch
        _, _ -> :ok
      end
    end)
  end

  defp broadcast_message(nodes, service_name, message) do
    Enum.each(nodes, fn node ->
      if node != Node.self() do
        send_message(node, service_name, message)
      end
    end)
  end
end
