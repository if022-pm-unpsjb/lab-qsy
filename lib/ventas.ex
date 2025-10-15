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
    Servidor de Ventas - Maneja productos y stock
  """

  use GenServer
  alias Libremarket.AMQP.Publisher

  @global_name {:global, __MODULE__}

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
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

  # reponer stock en caso de infracción
  def reponer_stock(pid \\ @global_name, producto_id) do
    GenServer.cast(pid, {:reponer_stock, producto_id})
  end

  @impl true
  def init(_state) do
    productos = Libremarket.Ventas.generar_productos_iniciales()
    IO.puts("Servidor de Ventas iniciado con #{map_size(productos)} productos")
    {:ok, %{productos: productos, ventas: []}}
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
        
        # Publicar evento de producto vendido
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
        
        {:reply, {:ok, producto}, nuevo_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:listar_productos, _from, state) do
    {:reply, state.productos, state}
  end

  #  manejar reponer stock
  @impl true
  def handle_cast({:reponer_stock, producto_id}, state) do
    nuevos_productos = Libremarket.Ventas.aumentar_stock(state.productos, producto_id)
    nuevo_state = %{state | productos: nuevos_productos}
    {:noreply, nuevo_state}
  end
end
