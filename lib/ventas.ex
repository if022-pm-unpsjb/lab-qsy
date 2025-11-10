defmodule Libremarket.Ventas do
  @doc """
  Genera productos iniciales con stock aleatorio
  """
  def generar_productos_iniciales() do
    productos = [
      {"Notebook Lenovo", 1},
      {"Mouse Logitech", 2},
      {"Teclado Mecánico", 3},
      {"Monitor 24\"", 4},
      {"Auriculares Bluetooth", 5},
      {"Webcam HD", 6},
      {"Tablet Samsung", 7},
      {"Smartphone Motorola", 8},
      {"Cargador USB-C", 9},
      {"Disco SSD 500GB", 10},
      {"RAM 8GB DDR4", 11},
      {"Impresora HP", 12}
    ]

    Enum.reduce(productos, %{}, fn {producto, id}, acc ->
      Map.put(acc, producto, %{
        id: id,
        nombre: producto,
        precio: :rand.uniform(50000) + 5000,
        stock: :rand.uniform(10)
      })
    end)
  end

  def verificar_stock(productos, producto_id) do
    case Enum.find(productos, fn {_k, v} -> v.id == producto_id end) do
      nil ->
        {:error, :producto_no_encontrado}

      {_k, producto} ->
        if producto.stock > 0,
          do: {:ok, producto},
          else: {:error, :sin_stock}
    end
  end

  def reducir_stock(productos, producto_id) do
    Enum.map(productos, fn {k, v} ->
      if v.id == producto_id do
        {k, %{v | stock: v.stock - 1}}
      else
        {k, v}
      end
    end)
    |> Enum.into(%{})
  end

  def aumentar_stock(productos, producto_id) do
    Enum.map(productos, fn {k, v} ->
      if v.id == producto_id do
        {k, %{v | stock: v.stock + 1}}
      else
        {k, v}
      end
    end)
    |> Enum.into(%{})
  end
end

defmodule Libremarket.Ventas.Server do
  @moduledoc """
  Servidor de Ventas - Maneja productos y stock con elección de líder
  """
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}
  @service_name "ventas"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def is_leader?() do
    # Obtener el PID del proceso global
    case :global.whereis_name(__MODULE__) do
      :undefined ->
        false

      pid when is_pid(pid) ->
        # Consultar al ZookeeperLeader en el nodo donde está el servidor
        target_node = node(pid)

        try do
          :rpc.call(target_node, Libremarket.ZookeeperLeader, :is_leader?, [@service_name], 5000)
        catch
          _, _ -> false
        end
    end
  end

  def verificar_producto(pid \\ @global_name, producto_id) do
    GenServer.call(pid, {:verificar_producto, producto_id})
  end

  def confirmar_venta(pid \\ @global_name, producto_id) do
    GenServer.call(pid, {:confirmar_venta, producto_id})
  end

  def listar_productos(pid \\ @global_name) do
    GenServer.call(pid, :listar_productos)
  end

  def reponer_stock(pid \\ @global_name, producto_id) do
    GenServer.cast(pid, {:reponer_stock, producto_id})
  end

  @impl true
  def init(_state) do
    productos = Libremarket.Ventas.generar_productos_iniciales()
    Logger.info("Servidor de Ventas iniciado con #{map_size(productos)} productos")
    Logger.info("Esperando elección de líder mediante Zookeeper...")

    {:ok, _pid} = Libremarket.ZookeeperLeader.start_link(
      service_name: @service_name,
      on_leader_change: &handle_leader_change/1
    )

    {:ok, %{productos: productos, ventas: [], is_leader: false}}
  end

  @impl true
  def handle_call({:verificar_producto, producto_id}, _from, state) do
    case Libremarket.Ventas.verificar_stock(state.productos, producto_id) do
      {:ok, producto} ->
        {:reply, {:ok, producto}, state}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:confirmar_venta, producto_id}, _from, state) do
    case Libremarket.Ventas.verificar_stock(state.productos, producto_id) do
      {:ok, producto} ->
        nuevos_productos = Libremarket.Ventas.reducir_stock(state.productos, producto_id)
        nuevas_ventas = [%{producto: producto, timestamp: DateTime.utc_now()} | state.ventas]
        nuevo_state = %{state | productos: nuevos_productos, ventas: nuevas_ventas}

        {:reply, {:ok, producto}, nuevo_state}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:listar_productos, _from, state) do
    {:reply, state.productos, state}
  end

  @impl true
  def handle_cast({:reponer_stock, producto_id}, state) do
    nuevos_productos = Libremarket.Ventas.aumentar_stock(state.productos, producto_id)
    nuevo_state = %{state | productos: nuevos_productos}
    Logger.info("Stock repuesto para producto #{producto_id}")
    {:noreply, nuevo_state}
  end

  @impl true
  def handle_info({:leader_change, is_leader}, state) do
    Logger.info("""
    [Ventas] Cambio de liderazgo
    Nodo: #{Node.self()}
    Es líder: #{is_leader}
    """)
    {:noreply, %{state | is_leader: is_leader}}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp handle_leader_change(is_leader) do
    pid = :global.whereis_name(__MODULE__)

    Logger.debug("[Ventas] handle_leader_change llamado: is_leader=#{is_leader}, pid=#{inspect(pid)}")

    case pid do
      :undefined ->
        Logger.warning("[Ventas] No se encontró el proceso registrado globalmente")
        :ok
      pid when is_pid(pid) ->
        Logger.debug("[Ventas] Enviando mensaje {:leader_change, #{is_leader}} a #{inspect(pid)}")
        send(pid, {:leader_change, is_leader})
    end
  end
end

defmodule Libremarket.Ventas.Consumer do
  @moduledoc """
  Consumer AMQP para el servicio de Ventas con elección de líder.
  Solo procesa mensajes cuando el nodo es líder.
  """
  use GenServer
  require Logger
  alias Libremarket.AMQP.{Connection, Publisher}

  @exchange "libremarket_exchange"
  @queue "ventas_queue"
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
    is_leader = Libremarket.Ventas.Server.is_leader?()

    new_state =
      cond do
        is_leader and not state.is_consuming ->
          Logger.info("[Consumer Ventas] Este nodo es LÍDER, iniciando consumo de mensajes")
          start_consuming(state)

        not is_leader and state.is_consuming ->
          Logger.info("[Consumer Ventas] Este nodo ya NO es líder, deteniendo consumo de mensajes")
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
      Logger.info("Consumer de Ventas iniciado en cola: #{@queue}")
      {:noreply, %{state | channel: channel, consumer_tag: consumer_tag, is_consuming: true}}
    else
      error ->
        Logger.error("Error configurando consumer de Ventas: #{inspect(error)}")
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
         :ok <- AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "ventas.requests") do
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

  defp process_message(%{"request_type" => "verificar_stock", "compra_id" => compra_id, "producto_id" => producto_id}) do
    Logger.info("Verificando stock para producto #{producto_id}, compra #{compra_id}")

    case Libremarket.Ventas.Server.verificar_producto(producto_id) do
      {:ok, producto} ->
        # Responder con stock verificado
        Publisher.publish("ventas.responses", %{
          "response_type" => "stock_verificado",
          "compra_id" => compra_id,
          "producto" => %{
            "id" => producto.id,
            "nombre" => producto.nombre,
            "precio" => producto.precio,
            "stock" => producto.stock
          },
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      {:error, reason} ->
        # Responder con error
        Publisher.publish("ventas.responses", %{
          "response_type" => "error_stock",
          "compra_id" => compra_id,
          "error" => Atom.to_string(reason),
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
  end

  defp process_message(%{"request_type" => "confirmar_venta", "compra_id" => compra_id, "producto_id" => producto_id}) do
    Logger.info("Confirmando venta para producto #{producto_id}, compra #{compra_id}")

    case Libremarket.Ventas.Server.confirmar_venta(producto_id) do
      {:ok, producto} ->
        # Publicar evento de venta confirmada
        Publisher.publish("ventas.events", %{
          "event_type" => "producto_vendido",
          "producto_id" => producto_id,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Publicar evento de stock actualizado
        Publisher.publish("ventas.events", %{
          "event_type" => "stock_actualizado",
          "producto_id" => producto_id,
          "stock" => producto.stock - 1,
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

        # Responder a compras
        Publisher.publish("ventas.responses", %{
          "response_type" => "venta_confirmada",
          "compra_id" => compra_id,
          "producto" => %{
            "id" => producto.id,
            "nombre" => producto.nombre,
            "precio" => producto.precio,
            "stock" => producto.stock
          },
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      {:error, reason} ->
        Publisher.publish("ventas.responses", %{
          "response_type" => "error_venta",
          "compra_id" => compra_id,
          "error" => Atom.to_string(reason),
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
  end

  defp process_message(%{"request_type" => "reponer_stock", "producto_id" => producto_id}) do
    Logger.info("Reponiendo stock para producto #{producto_id}")
    Libremarket.Ventas.Server.reponer_stock(producto_id)

    # Publicar evento de stock repuesto
    Publisher.publish("ventas.events", %{
      "event_type" => "stock_repuesto",
      "producto_id" => producto_id,
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  end

  defp process_message(message) do
    Logger.warning("Mensaje no reconocido en Ventas Consumer: #{inspect(message)}")
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
        Logger.info("[Consumer Ventas] Consumo detenido")
      catch
        _, _ -> :ok
      end
    end
    %{state | is_consuming: false, consumer_tag: nil}
  end
end
