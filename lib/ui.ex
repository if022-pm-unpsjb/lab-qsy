defmodule Libremarket.Ui do
  def comprar(producto_id, forma_entrega, medio_pago) do
    Libremarket.Compras.Server.comprar(producto_id, medio_pago, forma_entrega)
  end

  def listar_productos() do
    Libremarket.Ventas.Server.listar_productos()
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
