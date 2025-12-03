defmodule Libremarket.LeaderElection do
  @moduledoc """
  Implementación del algoritmo Bully para elección de líder distribuido.

  Características:
  - Elección basada en prioridad (número del nodo)
  - Heartbeats del líder
  - Re-elección automática cuando el líder falla
  - Resolución de conflictos cuando hay múltiples líderes
  """
  use GenServer
  require Logger

  @heartbeat_interval 3_000
  @heartbeat_timeout 10_000
  @election_timeout 5_000
  @discovery_interval 5_000

  # Client API

  def start_link(opts) do
    service_name = Keyword.fetch!(opts, :service_name)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(service_name))
  end

  def is_leader?(service_name) do
    try do
      GenServer.call(via_tuple(service_name), :is_leader, 2_000)
    catch
      :exit, _ -> false
    end
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    service_name = Keyword.fetch!(opts, :service_name)
    on_leader_change = Keyword.get(opts, :on_leader_change)

    node_name = Node.self()
    priority = extract_priority(node_name)

    Logger.info("""
    [Leader Election] Iniciando elección de líder para servicio: #{service_name}
    Nodo: #{node_name}
    Prioridad: #{priority}
    """)

    # Programar tareas periódicas
    Process.send_after(self(), :discover_nodes, 1_000)
    Process.send_after(self(), :check_leader_health, @heartbeat_timeout)
    Process.send_after(self(), :send_heartbeat, @heartbeat_interval)

    # Iniciar elección después de dar tiempo suficiente al cluster gossip (6 segundos)
    Process.send_after(self(), :start_election, 6_000)

    {:ok, %{
      service_name: service_name,
      node_name: node_name,
      priority: priority,
      is_leader: false,
      current_leader: nil,
      last_heartbeat: nil,
      nodes: [node_name],
      on_leader_change: on_leader_change,
      election_in_progress: false,
      first_election_done: false
    }}
  end

  @impl true
  def handle_call(:is_leader, _from, state) do
    {:reply, state.is_leader, state}
  end

  @impl true
  def handle_call({:election, from_node, from_priority}, _from, state) do
    Logger.info("[Leader Election] Recibido mensaje de elección de #{from_node} (prioridad #{from_priority})")

    cond do
      from_priority > state.priority ->
        # Nodo de mayor prioridad está compitiendo
        Logger.info("[Leader Election] Respondiendo OK a nodo de mayor prioridad")

        # Si era líder, dejar de serlo
        new_state = if state.is_leader do
          Logger.info("[Leader Election] Dejando de ser líder")
          notify_leader_change(state, false)
          %{state | is_leader: false, current_leader: from_node}
        else
          %{state | current_leader: from_node}
        end

        {:reply, :ok, new_state}

      true ->
        # Nodo de menor o igual prioridad - debemos ser líder
        Logger.info("[Leader Election] Rechazando nodo de menor prioridad")

        # Si no somos líder pero hay un líder conocido de mayor prioridad, no hacer nada
        if not state.is_leader and state.current_leader != nil do
          current_leader_priority = extract_priority(state.current_leader)
          if current_leader_priority > state.priority do
            Logger.info("[Leader Election] Ya hay un líder de mayor prioridad: #{state.current_leader}")
            {:reply, :reject, state}
          else
            # El líder actual es de menor prioridad, convertirnos en líder
            Logger.info("[Leader Election] No soy líder pero tengo mayor prioridad que el actual, convirtiéndome en líder")
            new_state = become_leader(%{state | election_in_progress: false, first_election_done: true})
            {:reply, :reject, new_state}
          end
        else
          # Si no somos líder y no hay líder conocido, convertirnos en líder
          new_state = if not state.is_leader do
            Logger.info("[Leader Election] No soy líder pero tengo mayor prioridad, convirtiéndome en líder inmediatamente")
            become_leader(%{state | election_in_progress: false, first_election_done: true})
          else
            state
          end
          {:reply, :reject, new_state}
        end
    end
  end

  @impl true
  def handle_call({:coordinator, from_node, from_priority}, _from, state) do
    Logger.info("[Leader Election] Recibido anuncio de coordinador de #{from_node} (prioridad #{from_priority})")

    cond do
      from_priority > state.priority ->
        # Aceptar el nuevo líder
        new_state = if state.is_leader do
          Logger.info("[Leader Election] Dejando de ser líder, aceptando nuevo coordinador")
          notify_leader_change(state, false)
          %{state | is_leader: false, current_leader: from_node, last_heartbeat: System.monotonic_time(:millisecond), election_in_progress: false}
        else
          %{state | current_leader: from_node, last_heartbeat: System.monotonic_time(:millisecond), election_in_progress: false}
        end

        {:reply, :ok, new_state}

      from_priority == state.priority and from_node != state.node_name ->
        # Conflicto: dos nodos con la misma prioridad
        # Desempatar por nombre de nodo
        if from_node > state.node_name do
          Logger.warning("[Leader Election] Conflicto de prioridad, aceptando #{from_node} por desempate")
          new_state = if state.is_leader do
            notify_leader_change(state, false)
            %{state | is_leader: false, current_leader: from_node, last_heartbeat: System.monotonic_time(:millisecond), election_in_progress: false}
          else
            %{state | current_leader: from_node, last_heartbeat: System.monotonic_time(:millisecond), election_in_progress: false}
          end
          {:reply, :ok, new_state}
        else
          Logger.warning("[Leader Election] Conflicto de prioridad, rechazando #{from_node} por desempate")
          {:reply, :reject, state}
        end

      true ->
        # Nodo de menor prioridad intentando ser líder
        Logger.warning("[Leader Election] Rechazando coordinador de menor prioridad")
        {:reply, :reject, state}
    end
  end

  @impl true
  def handle_cast({:heartbeat, from_node}, state) do
    if state.current_leader == from_node do
      {:noreply, %{state | last_heartbeat: System.monotonic_time(:millisecond)}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:discover_nodes, state) do
    nodes = discover_service_nodes(state.service_name)

    if length(nodes) != length(state.nodes) do
      Logger.debug("[Leader Election] Nodos participantes: #{inspect(nodes)}")
    end

    # CRÍTICO: Solo iniciar re-elección si realmente el líder actual no está en la lista
    # o si no hay líder y ya pasó la primera elección
    new_state = if state.is_leader and state.first_election_done do
      higher_priority_nodes = Enum.filter(nodes, fn node ->
        extract_priority(node) > state.priority
      end)

      # CAMBIO CLAVE: Solo ceder liderazgo si hay nodos de mayor prioridad Y no estamos en elección
      if not Enum.empty?(higher_priority_nodes) and not state.election_in_progress do
        # Verificar si estos nodos están realmente disponibles antes de ceder
        available_higher = Enum.any?(higher_priority_nodes, fn node ->
          try do
            :rpc.call(node, :erlang, :node, [], 1000) == node
          catch
            _, _ -> false
          end
        end)

        if available_higher do
          Logger.info("[Leader Election] Detectados nodos de mayor prioridad disponibles, cediendo liderazgo")
          notify_leader_change(state, false)
          Process.send(self(), :start_election, [])
          %{state | nodes: nodes, election_in_progress: true, is_leader: false, current_leader: nil}
        else
          %{state | nodes: nodes}
        end
      else
        %{state | nodes: nodes}
      end
    else
      # Si no somos líder pero aún no hubo primera elección y ya hay nodos disponibles
      if not state.first_election_done and length(nodes) > 1 and not state.election_in_progress do
        Logger.info("[Leader Election] Nodos descubiertos antes de primera elección, esperando...")
      end
      %{state | nodes: nodes}
    end

    Process.send_after(self(), :discover_nodes, @discovery_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:check_leader_health, state) do
    new_state = if not state.is_leader and state.current_leader != nil do
      current_time = System.monotonic_time(:millisecond)
      time_since_heartbeat = if state.last_heartbeat do
        current_time - state.last_heartbeat
      else
        @heartbeat_timeout + 1
      end

      if time_since_heartbeat > @heartbeat_timeout do
        Logger.warning("[Leader Election] Líder #{state.current_leader} no responde, iniciando elección")
        # Actualizar lista de nodos ANTES de iniciar elección
        updated_nodes = discover_service_nodes(state.service_name)
        Process.send(self(), :start_election, [])
        %{state | current_leader: nil, last_heartbeat: nil, election_in_progress: true, nodes: updated_nodes}
      else
        state
      end
    else
      state
    end

    Process.send_after(self(), :check_leader_health, @heartbeat_timeout)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:send_heartbeat, state) do
    if state.is_leader do
      send_heartbeat_to_followers(state)
    end

    Process.send_after(self(), :send_heartbeat, @heartbeat_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:start_election, state) do
    Logger.debug("[Leader Election] handle_info(:start_election) - election_in_progress: #{state.election_in_progress}")

    # Permitir re-elección si no somos líder, incluso si election_in_progress está en true
    if state.election_in_progress and state.is_leader do
      {:noreply, state}
    else
      Logger.info("[Leader Election] Iniciando elección en nodo #{state.node_name}")

      # CAMBIO CRÍTICO: Verificar primero si ya hay un líder activo de mayor prioridad
      if state.current_leader != nil do
        leader_priority = extract_priority(state.current_leader)
        if leader_priority > state.priority do
          # Ya hay un líder de mayor prioridad, no hacer nada
          Logger.info("[Leader Election] Ya existe un líder de mayor prioridad: #{state.current_leader}, cancelando elección")
          {:noreply, %{state | election_in_progress: false, first_election_done: true}}
        else
          # El líder actual es de menor prioridad, continuar con elección
          conduct_election(state)
        end
      else
        # No hay líder conocido, conducir elección normal
        conduct_election(state)
      end
    end
  end

  @impl true
  def handle_info(:election_timeout, state) do
    if state.election_in_progress and not state.is_leader and state.current_leader == nil do
      Logger.warning("[Leader Election] Timeout de elección, iniciando nueva elección")
      Process.send(self(), :start_election, [])
      {:noreply, %{state | election_in_progress: false}}
    else
      {:noreply, %{state | election_in_progress: false}}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private Functions

  defp conduct_election(state) do
    # Obtener nodos de mayor prioridad
    higher_priority_nodes = Enum.filter(state.nodes, fn node ->
      node != state.node_name and extract_priority(node) > state.priority
    end)

    new_state = if Enum.empty?(higher_priority_nodes) do
      # No hay nodos de mayor prioridad, convertirse en líder
      Logger.info("[Leader Election] Sin nodos de mayor prioridad, convirtiéndose en líder")
      state_after_leader = become_leader(state)
      %{state_after_leader | first_election_done: true, election_in_progress: false}
    else
      # Enviar mensaje de elección a nodos de mayor prioridad
      Logger.info("[Leader Election] Enviando mensajes de elección a #{length(higher_priority_nodes)} nodos")

      responses = Enum.map(higher_priority_nodes, fn node ->
        try do
          result = :rpc.call(node, GenServer, :call, [
            via_tuple(state.service_name),
            {:election, state.node_name, state.priority}
          ], 2_000)
          {node, result}
        catch
          _, _ -> {node, :error}
        end
      end)

      # Si algún nodo de mayor prioridad respondió OK o REJECT, esperar
      valid_responses = Enum.filter(responses, fn {_node, result} ->
        result == :ok or result == :reject
      end)

      if Enum.empty?(valid_responses) do
        # Nadie respondió, convertirse en líder
        Logger.info("[Leader Election] Ningún nodo de mayor prioridad respondió, convirtiéndose en líder")
        state_after_leader = become_leader(state)
        %{state_after_leader | first_election_done: true, election_in_progress: false}
      else
        Logger.info("[Leader Election] Nodos de mayor prioridad respondieron, esperando coordinador")
        # Esperar un tiempo para que el nuevo líder se anuncie
        Process.send_after(self(), :election_timeout, @election_timeout)
        %{state | is_leader: false, election_in_progress: true, first_election_done: true}
      end
    end

    {:noreply, new_state}
  end

  defp become_leader(state) do
    Logger.info("[Leader Election] #{state.node_name} es ahora el LÍDER")

    # Anunciar a todos los nodos
    announce_coordinator(state)

    # Notificar cambio de liderazgo
    notify_leader_change(state, true)

    %{state |
      is_leader: true,
      current_leader: state.node_name,
      last_heartbeat: System.monotonic_time(:millisecond),
      election_in_progress: false
    }
  end

  defp announce_coordinator(state) do
    other_nodes = Enum.filter(state.nodes, fn node -> node != state.node_name end)

    if not Enum.empty?(other_nodes) do
      Logger.info("[Leader Election] Anunciando liderazgo a #{length(other_nodes)} nodos")

      Enum.each(other_nodes, fn node ->
        spawn(fn ->
          try do
            :rpc.call(node, GenServer, :call, [
              via_tuple(state.service_name),
              {:coordinator, state.node_name, state.priority}
            ], 2_000)
          catch
            _, _ -> :ok
          end
        end)
      end)
    end
  end

  defp send_heartbeat_to_followers(state) do
    follower_nodes = Enum.filter(state.nodes, fn node ->
      node != state.node_name
    end)

    Enum.each(follower_nodes, fn node ->
      spawn(fn ->
        try do
          :rpc.cast(node, GenServer, :cast, [
            via_tuple(state.service_name),
            {:heartbeat, state.node_name}
          ])
        catch
          _, _ -> :ok
        end
      end)
    end)
  end

  defp notify_leader_change(state, is_leader) do
    if state.on_leader_change do
      spawn(fn ->
        try do
          state.on_leader_change.(is_leader)
        catch
          kind, error ->
            Logger.error("Error en callback de cambio de líder: #{kind} - #{inspect(error)}")
        end
      end)
    end
  end

  defp discover_service_nodes(service_name) do
    all_nodes = [Node.self() | Node.list()]

    Enum.filter(all_nodes, fn node ->
      node_str = Atom.to_string(node)
      String.contains?(node_str, service_name)
    end)
    |> Enum.sort()
  end

  defp extract_priority(node_name) do
    node_str = Atom.to_string(node_name)

    case Regex.run(~r/(\d+)@/, node_str) do
      [_, number] -> String.to_integer(number)
      _ -> 0
    end
  end

  defp via_tuple(service_name) do
    {:via, Registry, {Libremarket.Registry, {:leader_election, service_name}}}
  end
end
