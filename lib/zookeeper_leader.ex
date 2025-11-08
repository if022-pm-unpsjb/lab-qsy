defmodule Libremarket.ZookeeperLeader do
  @moduledoc """
  Elección de líder en ZooKeeper con nodos efímeros secuenciales.
  Compatible con variantes de erlzk que usan:
    - create/3 para persistentes
    - create/4 con FLAGS ([:ephemeral, :sequential])   **o**
    - create/5 con ACL + FLAGS (:acl_open_unsafe, [:ephemeral, :sequential])
  """
  use GenServer
  require Logger

  @election_path "/libremarket/leader_election"
  @heartbeat_interval 5_000

  defmodule State do
    defstruct [
      :zk_pid,
      :service_name,
      :node_id,
      :election_node,
      :is_leader,
      :leader_callback,
      :follower_callback,
      :watch_ref
    ]
  end

  # =========== API ===========

  def start_link(opts) do
    service = Keyword.fetch!(opts, :service_name)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(service))
  end

  def is_leader?(service) do
    try do
      GenServer.call(via_tuple(service), :is_leader, 5_000)
    catch
      :exit, _ -> false
    end
  end

  def get_leader_info(service) do
    try do
      GenServer.call(via_tuple(service), :get_leader_info, 5_000)
    catch
      :exit, _ -> nil
    end
  end

  # ======== GenServer ========

  @impl true
  def init(opts) do
    service_name      = Keyword.fetch!(opts, :service_name)
    leader_callback   = Keyword.get(opts, :on_leader)
    follower_callback = Keyword.get(opts, :on_follower)

    zk_host = System.get_env("ZOOKEEPER_HOST") || "localhost:2181"
    node_id = System.get_env("NODE_ID") || Integer.to_string(:rand.uniform(1000))

    Logger.info("Iniciando election para #{service_name} (ZK=#{zk_host} NodeID=#{node_id})")

    state = %State{
      service_name: service_name,
      node_id: node_id,
      leader_callback: leader_callback,
      follower_callback: follower_callback,
      is_leader: false
    }

    send(self(), :connect_zookeeper)
    {:ok, state}
  end

  @impl true
  def handle_call(:is_leader, _from, st), do: {:reply, st.is_leader, st}

  @impl true
  def handle_call(:get_leader_info, _from, st) do
    {:reply, %{node_id: st.node_id, is_leader: st.is_leader, election_node: st.election_node}, st}
  end

  @impl true
  def handle_info(:connect_zookeeper, st) do
    {host_chars, port} = parse_host_port(System.get_env("ZOOKEEPER_HOST") || "localhost:2181")

    case :erlzk.connect([{host_chars, port}], 30_000) do
      {:ok, zk} ->
        Logger.info("Conectado a ZooKeeper en #{to_string(host_chars)}:#{port}")
        _ = ensure_election_path(zk)
        send(self(), :participate_in_election)
        {:noreply, %{st | zk_pid: zk}}

      {:error, reason} ->
        Logger.error("Error conectando a ZooKeeper: #{inspect(reason)}")
        Process.send_after(self(), :connect_zookeeper, 5_000)
        {:noreply, st}
    end
  end

  @impl true
  def handle_info(:participate_in_election, %{zk_pid: nil} = st) do
    Process.send_after(self(), :connect_zookeeper, 2_000)
    {:noreply, st}
  end

  @impl true
  def handle_info(:participate_in_election, st) do
    data =
      Jason.encode!(%{
        node_id: st.node_id,
        elixir_node: Node.self(),
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
      })

    case zk_create_ephemeral_seq(st.zk_pid, "#{@election_path}/n_", data) do
      {:ok, election_node} ->
        Logger.info("Nodo de elección creado: #{election_node}")
        st = %{st | election_node: to_string(election_node)}
        send(self(), :check_leadership)
        {:noreply, st}

      {:error, reason} ->
        Logger.error("Error creando nodo de elección: #{inspect(reason)}")
        Process.send_after(self(), :participate_in_election, 5_000)
        {:noreply, st}
    end
  end

  @impl true
  def handle_info(:check_leadership, st) do
    case :erlzk.get_children(st.zk_pid, @election_path) do
      {:ok, children} when is_list(children) ->
        if children == [] do
          Logger.warning("Lista de candidatos vacía; reintento")
          Process.send_after(self(), :check_leadership, 2_000)
          {:noreply, st}
        else
          sorted      = Enum.sort(children)
          leader_node = hd(sorted)
          my_node     = st.election_node |> String.split("/") |> List.last()
          was_leader  = st.is_leader
          is_leader   = (to_string(leader_node) == my_node)
          new_st      = %{st | is_leader: is_leader}

          cond do
            is_leader and not was_leader ->
              Logger.info("🎯 SOY EL LÍDER - #{st.service_name}")
              if st.leader_callback, do: st.leader_callback.()

            not is_leader and was_leader ->
              Logger.info("📋 Ahora soy SEGUIDOR - #{st.service_name}")
              if st.follower_callback, do: st.follower_callback.()

            is_leader ->
              Logger.debug("Sigo siendo líder")

            true ->
              Logger.debug("Seguidor; líder actual: #{leader_node}")
          end

          unless is_leader do
            watch_predecessor(new_st, sorted, my_node)
          end

          {:noreply, new_st}
        end

      {:error, reason} ->
        Logger.error("Error verificando liderazgo: #{inspect(reason)}")
        Process.send_after(self(), :check_leadership, 5_000)
        {:noreply, st}
    end
  end

  @impl true
  def handle_info({:erlzk, _watcher, {:node_deleted, _path}}, st) do
    send(self(), :check_leadership)
    {:noreply, %{st | watch_ref: nil}}
  end

  @impl true
  def handle_info(:heartbeat, st) do
    if st.zk_pid && Process.alive?(st.zk_pid) do
      Process.send_after(self(), :heartbeat, @heartbeat_interval)
      {:noreply, st}
    else
      Logger.error("Conexión ZooKeeper perdida, reconectando...")
      send(self(), :connect_zookeeper)
      {:noreply, %{st | zk_pid: nil, is_leader: false}}
    end
  end

  @impl true
  def handle_info(_msg, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, st) do
    if st.zk_pid, do: :erlzk.close(st.zk_pid)
    :ok
  end

  # ========== Privado ==========

  defp via_tuple(service),
    do: {:via, Registry, {Libremarket.Registry, {:zk_leader, service}}}

  # Crea paths persistentes, probando create/3 y si falla por aridad/ACL, create/5 con ACL explícito.
  defp ensure_election_path(zk) do
    with :ok <- zk_create_persistent(zk, "/libremarket"),
         :ok <- zk_create_persistent(zk, @election_path) do
      :ok
    else
      other -> other
    end
  end

  defp zk_create_persistent(zk, path) do
    try do
      case :erlzk.create(zk, path, "") do
        {:ok, _} -> :ok
        {:error, :node_exists} -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      FunctionClauseError ->
        # Variante: some erlzk exige ACL + FLAGS (flags vacíos = persistente)
        case :erlzk.create(zk, path, "", :acl_open_unsafe, []) do
          {:ok, _} -> :ok
          {:error, :node_exists} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp zk_create_ephemeral_seq(zk, path, data) do
    try do
      # Variante A: erlzk con FLAGS en aridad /4
      :erlzk.create(zk, path, data, [:ephemeral, :sequential])
    rescue
      FunctionClauseError ->
        # Variante B: erlzk con ACL + FLAGS en aridad /5
        :erlzk.create(zk, path, data, :acl_open_unsafe, [:ephemeral, :sequential])
    end
  end

  defp watch_predecessor(st, sorted_children, my_node) do
    idx = Enum.find_index(sorted_children, &(&1 == my_node))

    if idx && idx > 0 do
      predecessor = Enum.at(sorted_children, idx - 1)
      path = "#{@election_path}/#{predecessor}"

      case :erlzk.exists(st.zk_pid, path, watch: true) do
        {:ok, _stat} ->
          Logger.debug("Observando predecesor #{predecessor}")
          %{st | watch_ref: {:watching, path}}

        {:ok, :undefined} ->
          send(self(), :check_leadership)
          st

        {:error, reason} ->
          Logger.error("No se pudo observar predecesor #{predecessor}: #{inspect(reason)}")
          st
      end
    else
      st
    end
  end

  # "host[:port]"
  defp parse_host_port(env) do
    case String.split(env, ":") do
      [host, port_str] -> {String.to_charlist(host), String.to_integer(port_str)}
      [host]           -> {String.to_charlist(host), 2181}
    end
  end
end
