defmodule Libremarket.Ui do
  def comprar(producto_id, forma_entrega, medio_pago) do
    try do
      :erpc.call(
        :compras@maty,
        Libremarket.Compras.Server,
        :comprar,
        [producto_id, medio_pago, forma_entrega],
        30000
      )
    rescue
      error ->
        IO.puts("Error comunicándose con servicio de compras: #{inspect(error)}")
        {:error, :servicio_no_disponible}
    end
  end

  def listar_productos() do
    try do
      :erpc.call(:ventas@maty, Libremarket.Ventas.Server, :listar_productos, [], 10000)
    rescue
      error ->
        IO.puts("Error obteniendo productos: #{inspect(error)}")
        %{}
    end
  end

  def mostrar_productos() do
    productos = listar_productos()
    IO.puts("\n=== PRODUCTOS DISPONIBLES ===")

    Enum.each(productos, fn {_key, producto} ->
      IO.puts(
        "ID: #{producto.id} | #{producto.nombre} | Precio: $#{producto.precio} | Stock: #{producto.stock}"
      )
    end)
  end
end
