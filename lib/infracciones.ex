defmodule Libremarket.Infracciones do
  @moduledoc false
  @doc """
  Detecta infracciones con 50% de probabilidad
  """
  def detectar_infracciones() do
    :rand.uniform(100) <= 50
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Servidor de Infracciones con elección de líder mediante Bully Algorithm.

  LÍDER (elegido por elección distribuida):
  - Procesa mensajes AMQP
  - Detecta infracciones (escrituras)
  - Replica estado a las réplicas
  - Responde lecturas

  SEGUIDOR (no líder):
  - NO procesa mensajes AMQP
  - Solo recibe replicación de estado del líder
  - Responde lecturas
  """
  use GenServer
  require Logger

  @replication_interval 2_000
  @service_name "infracciones"

  # Client API

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectar_infracciones(id_compra) do
    case find_leader_node() do
      nil ->
        {:error, :no_leader}
      node ->
        :rpc.call(node, GenServer, :call, [__MODULE__, {:detectar_infracciones, id_compra}])
    end
  end

  def listar_infracciones() do
    case find_any_node() do
      nil ->
        []
      node ->
        :rpc.call(node, GenServer, :call, [__MODULE__, :listar_infracciones])
    end
  end

  def get_replication_info() do
    case find_any_node() do
      nil ->
        {:error, :no_node}
      node ->
        :rpc.call(node, GenServer, :call, [__MODULE__, :get_replication_info])
    end
  end

  def is_leader?() do
    try do
      Libremarket.LeaderElection.is_leader?(@service_name)
    catch
      _, _ -> false
    end
  end

  # Server Callbacks

  @impl true
  def init(_state) do
    Logger.info("Servidor de Infracciones iniciado en nodo #{Node.self()}")

    {:ok, _pid} = Libremarket.LeaderElection.start_link(
      service_name: @service_name,
      on_leader_change: &handle_leader_change/1
    )

    Process.send_after(self(), :replicate_state, @replication_interval)

    {:ok, %{
      infracciones: [],
      replicated_from: nil,
      last_replication: nil,
      is_leader: false
    }}
  end

  @impl true
  def handle_call({:detectar_infracciones, id_compra}, _from, state) do
    if not state.is_leader do
      {:reply, {:error, :not_leader}, state}
    else
      tiene_infraccion = Libremarket.Infracciones.detectar_infracciones()

      nuevo_state =
        if tiene_infraccion do
          nuevas_infracciones = [
            %{id_compra: id_compra, timestamp: DateTime.utc_now(), node: Node.self()}
            | state.infracciones
          ]

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
    if not state.is_leader do
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
  def handle_info({:leader_change, is_leader}, state) do
    status = if is_leader, do: "LÍDER", else: "SEGUIDOR"
    Logger.info("[Infracciones] #{status}")
    {:noreply, %{state | is_leader: is_leader}}
  end

  @impl true
  def handle_info(:replicate_state, state) do
    if state.is_leader do
      replicate_to_followers(state.infracciones)
    end

    Process.send_after(self(), :replicate_state, @replication_interval)

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private Functions

  defp handle_leader_change(is_leader) do
    case Process.whereis(__MODULE__) do
      nil ->
        :ok
      pid when is_pid(pid) ->
        send(pid, {:leader_change, is_leader})
    end
  end

  defp replicate_to_followers(infracciones) do
    follower_nodes = get_follower_nodes()

    if not Enum.empty?(follower_nodes) and length(infracciones) > 0 do
      Enum.each(follower_nodes, fn node ->
        spawn(fn ->
          try do
            :rpc.call(node, GenServer, :call, [
              __MODULE__,
              {:replicate_state, infracciones, Node.self()}
            ], 3_000)
          catch
            _, _ -> :ok
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

  defp find_leader_node() do
    all_nodes = [Node.self() | Node.list()]

    Enum.find(all_nodes, fn node ->
      node_str = Atom.to_string(node)
      if String.contains?(node_str, "infracciones") do
        try do
          :rpc.call(node, Libremarket.LeaderElection, :is_leader?, [@service_name], 2000) == true
        catch
          _, _ -> false
        end
      else
        false
      end
    end)
  end

  defp find_any_node() do
    all_nodes = [Node.self() | Node.list()]

    Enum.find(all_nodes, fn node ->
      String.contains?(Atom.to_string(node), "infracciones")
    end)
  end
end

defmodule Libremarket.Infracciones.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Infracciones.

  IMPORTANTE: Este Consumer SOLO procesa mensajes cuando el nodo es LÍDER.
  Los seguidores NO procesan mensajes AMQP.
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "infracciones_queue"
  @check_leader_interval 5_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :check_leadership)
    {:ok, %{channel: nil, consumer_tag: nil, is_consuming: false}}
  end

  @impl true
  def handle_info(:check_leadership, state) do
    is_leader = Libremarket.Infracciones.Server.is_leader?()

    new_state =
      cond do
        is_leader and not state.is_consuming ->
          Logger.info("[Consumer Infracciones] LÍDER - Iniciando consumo")
          start_consuming(state)

        not is_leader and state.is_consuming ->
          Logger.info("[Consumer Infracciones] SEGUIDOR - Deteniendo consumo")
          stop_consuming(state)

        true ->
          state
      end

    Process.send_after(self(), :check_leadership, @check_leader_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:setup, state) do
    with {:ok, channel} <- Connection.get_channel(),
         :ok <- setup_queue(channel),
         {:ok, consumer_tag} <- AMQP.Basic.consume(channel, @queue) do
      {:noreply, %{state | channel: channel, consumer_tag: consumer_tag, is_consuming: true}}
    else
      error ->
        Logger.error("Error configurando consumer: #{inspect(error)}")
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
    Logger.warning("Mensaje no reconocido: #{inspect(message)}")
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

  defp start_consuming(state) do
    send(self(), :setup)
    state
  end

  defp stop_consuming(state) do
    if state.channel && state.consumer_tag do
      try do
        AMQP.Basic.cancel(state.channel, state.consumer_tag)
      catch
        _, _ -> :ok
      end
    end
    %{state | is_consuming: false, consumer_tag: nil}
  end
end
