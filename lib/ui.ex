defmodule Libremarket.Ui do
  @moduledoc """
  Interfaz de usuario para interactuar con Libremarket.
  Las compras son asíncronas - devuelven un ID de compra para seguimiento.
  """

  def comprar_async(producto_id, forma_entrega, medio_pago) do
    try do
      # Generar ID de compra único
      compra_id = "compra_#{:rand.uniform(100000)}"

      # Iniciar compra de forma asíncrona
      Task.start(fn ->
        Libremarket.Compras.Server.comprar(producto_id, medio_pago, forma_entrega)
      end)

      {:ok, %{
        compra_id: compra_id,
        mensaje: "Compra iniciada. Los resultados se procesarán de forma asíncrona.",
        producto_id: producto_id
      }}
    rescue
      error ->
        IO.puts("Error comunicándose con servicio de compras: #{inspect(error)}")
        {:error, :servicio_no_disponible}
    end
  end

  def comprar(producto_id, forma_entrega, medio_pago) do
    try do
      # Compra síncrona con timeout mayor
      Libremarket.Compras.Server.comprar(producto_id, medio_pago, forma_entrega)
    rescue
      error ->
        IO.puts("Error comunicándose con servicio de compras: #{inspect(error)}")
        {:error, :servicio_no_disponible}
    catch
      :exit, {:timeout, _} ->
        IO.puts("⏱️  Timeout: La compra está tomando mucho tiempo.")
        IO.puts("Esto puede significar que algún servicio no está respondiendo.")
        IO.puts("Verifica que todos los servicios estén corriendo:")
        IO.puts("  - Ventas.Server y Ventas.Consumer")
        IO.puts("  - Pagos.Server y Pagos.Consumer")
        IO.puts("  - Envios.Server y Envios.Consumer")
        IO.puts("  - Infracciones.Server y Infracciones.Consumer")
        {:error, :timeout}
    end
  end

  def listar_productos() do
    try do
      Libremarket.Ventas.Server.listar_productos()
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

  def verificar_servicios() do
    IO.puts("\n=== VERIFICACIÓN DE SERVICIOS ===")

    servicios = [
      {"Ventas Server", Libremarket.Ventas.Server},
      {"Ventas Consumer", Libremarket.Ventas.Consumer},
      {"Compras Server", Libremarket.Compras.Server},
      {"Compras Consumer", Libremarket.Compras.Consumer},
      {"Pagos Server", Libremarket.Pagos.Server},
      {"Pagos Consumer", Libremarket.Pagos.Consumer},
      {"Envíos Server", Libremarket.Envios.Server},
      {"Envíos Consumer", Libremarket.Envios.Consumer},
      {"Infracciones Server", Libremarket.Infracciones.Server},
      {"Infracciones Consumer", Libremarket.Infracciones.Consumer},
      {"AMQP Connection", Libremarket.AMQP.Connection}
    ]

    total = length(servicios)
    activos = Enum.count(servicios, fn {_nombre, modulo} ->
      case Process.whereis(modulo) do
        nil -> false
        _pid -> true
      end
    end)

    Enum.each(servicios, fn {nombre, modulo} ->
      case Process.whereis(modulo) do
        nil -> IO.puts("❌ #{nombre}")
        pid -> IO.puts("✅ #{nombre}: #{inspect(pid)}")
      end
    end)

    IO.puts("\nResultado: #{activos}/#{total} servicios activos")

    if activos < total do
      IO.puts("\n⚠️  Algunos servicios no están corriendo!")
      IO.puts("En Docker, asegúrate de que todos los contenedores estén up:")
      IO.puts("  docker-compose ps")
      IO.puts("  docker-compose logs -f")
    end

    IO.puts("===================================\n")

    {activos, total}
  end

  def listar_compras() do
    try do
      GenServer.call({:global, Libremarket.Compras.Server}, :listar_compras)
    rescue
      error ->
        IO.puts("Error obteniendo compras: #{inspect(error)}")
        []
    end
  end

  def mostrar_compras() do
    compras = listar_compras()
    IO.puts("\n=== COMPRAS REALIZADAS ===")

    if Enum.empty?(compras) do
      IO.puts("No hay compras realizadas aún.")
    else
      Enum.with_index(compras, 1)
      |> Enum.each(fn {compra, idx} ->
        IO.puts("\nCompra ##{idx}:")
        IO.puts("  Producto: #{compra.producto.nombre}")
        IO.puts("  Precio: $#{compra.producto.precio}")
        IO.puts("  Medio de pago: #{compra.medio_pago}")
        IO.puts("  Forma de entrega: #{compra.forma_entrega}")
        IO.puts("  Costo de envío: $#{compra.envio.costo}")
        IO.puts("  Timestamp: #{compra.timestamp}")
      end)
    end

    IO.puts("===========================\n")
  end
end
