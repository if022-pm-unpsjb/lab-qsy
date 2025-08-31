defmodule Libremarket.Ventas do
  @doc """
    Genera productos iniciales con stock aleatorio
  """
  def generar_productos_iniciales() do
    productos = [
      "Notebook Lenovo",
      "Mouse Logitech",
      "Teclado Mecánico",
      "Monitor 24\"",
      "Auriculares Bluetooth",
      "Webcam HD",
      "Tablet Samsung",
      "Smartphone Motorola",
      "Cargador USB-C",
      "Disco SSD 500GB",
      "RAM 8GB DDR4",
      "Impresora HP"
    ]

    Enum.reduce(productos, %{}, fn producto, acc ->
      Map.put(acc, producto, %{
        id: :rand.uniform(500),
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

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def verificar_proucto(pid \\ __MODULE__, producto_id) do
    GenServer.call(pid, {:verificar_producto, producto_id})
  end

  def confirmar_venta(pid \\ __MODULE__, producto_id) do
    GenServer.call(pid, {:confirmar_venta, producto_id})
  end

  def listar_productos(pid \\ __MODULE__) do
    GenServer.call(pid, :listar_productos)
  end

   # reponer stock en caso de infracción
  def reponer_stock(pid \\ __MODULE__, producto_id) do
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
