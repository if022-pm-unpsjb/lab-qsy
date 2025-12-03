defmodule Libremarket.Ui do
  @moduledoc """
  Interfaz de usuario para interactuar con Libremarket.
  Las compras son asíncronas - devuelven un ID de compra para seguimiento.
  """

  # Timeout para llamadas RPC (20 segundos para dar tiempo al GenServer)
  @rpc_timeout 20_000

  # Función auxiliar para encontrar un nodo de un servicio específico
  defp find_service_node(service_name) do
    nodes = [Node.self() | Node.list()]

    Enum.find(nodes, fn node ->
      node_str = Atom.to_string(node)
      String.contains?(node_str, service_name)
    end)
  end

  def comprar_async(producto_id, forma_entrega, medio_pago) do
    try do
      compra_id = "compra_#{:rand.uniform(100_000)}"

      Task.start(fn ->
        comprar(producto_id, forma_entrega, medio_pago)
      end)

      {:ok,
       %{
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
      # Buscar el nodo líder específicamente
      case find_leader_node("compras") do
        nil ->
          IO.puts("No se encontró ningún líder de compras disponible")
          {:error, :servicio_no_disponible}

        leader_node ->
          IO.puts("Realizando compra en nodo líder: #{leader_node}")

          # Hacer RPC call con timeout explícito
          case :rpc.call(leader_node, Libremarket.Compras.Server, :comprar,
                        [producto_id, medio_pago, forma_entrega], @rpc_timeout) do
            {:ok, result} ->
              {:ok, result}

            {:error, reason} ->
              {:error, reason}

            {:badrpc, {:EXIT, {:timeout, _}}} ->
              IO.puts("Timeout: La compra está tomando mucho tiempo.")
              IO.puts("Esto puede significar que algún servicio no está respondiendo.")
              {:error, :timeout}

            {:badrpc, reason} ->
              IO.puts("Error de RPC: #{inspect(reason)}")
              {:error, :servicio_no_disponible}

            other ->
              IO.puts("Respuesta inesperada de compras: #{inspect(other)}")
              {:error, :respuesta_inesperada}
          end
      end
    rescue
      error ->
        IO.puts("Excepción comunicándose con servicio de compras: #{inspect(error)}")
        {:error, :servicio_no_disponible}
    catch
      :exit, {:timeout, _} ->
        IO.puts("Timeout: La compra está tomando mucho tiempo.")
        {:error, :timeout}
    end
  end

  # Función auxiliar para encontrar el nodo líder de un servicio
  defp find_leader_node(service_name) do
    nodes = [Node.self() | Node.list()]

    compras_nodes = Enum.filter(nodes, fn node ->
      node_str = Atom.to_string(node)
      String.contains?(node_str, service_name)
    end)

    # Buscar el líder entre los nodos de compras
    Enum.find(compras_nodes, fn node ->
      try do
        :rpc.call(node, Libremarket.LeaderElection, :is_leader?, [service_name], 2000) == true
      catch
        _, _ -> false
      end
    end)
  end

  def listar_productos() do
    try do
      case find_service_node("ventas") do
        nil ->
          IO.puts("No se encontró ningún nodo de ventas disponible")
          %{}

        node ->
          case :rpc.call(node, Libremarket.Ventas.Server, :listar_productos, [], 5000) do
            {:badrpc, _} -> %{}
            productos -> productos
          end
      end
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
      {"Ventas Server", "ventas"},
      {"Compras Server", "compras"},
      {"Pagos Server", "pagos"},
      {"Envíos Server", "envios"},
      {"Infracciones Server", "infracciones"},
      {"AMQP Connection", nil}
    ]

    total = length(servicios)

    activos =
      Enum.count(servicios, fn {_nombre, servicio} ->
        case servicio do
          nil ->
            # AMQP Connection usa registro local
            Process.whereis(Libremarket.AMQP.Connection) != nil

          service_name ->
            # Buscar nodos del servicio
            all_nodes = [Node.self() | Node.list()]

            Enum.any?(all_nodes, fn node ->
              node_str = Atom.to_string(node)
              String.contains?(node_str, service_name)
            end)
        end
      end)

    Enum.each(servicios, fn {nombre, servicio} ->
      status = case servicio do
        nil ->
          case Process.whereis(Libremarket.AMQP.Connection) do
            nil -> "✗ NO ACTIVO"
            pid -> "✓ #{inspect(pid)}"
          end

        service_name ->
          all_nodes = [Node.self() | Node.list()]

          nodes = Enum.filter(all_nodes, fn node ->
            node_str = Atom.to_string(node)
            String.contains?(node_str, service_name)
          end)

          if Enum.empty?(nodes) do
            "✗ NO ACTIVO"
          else
            leader = Enum.find(nodes, fn node ->
              try do
                :rpc.call(node, Libremarket.LeaderElection, :is_leader?, [service_name], 2000) == true
              catch
                _, _ -> false
              end
            end)

            if leader do
              "✓ Líder: #{leader} | Nodos: #{length(nodes)}"
            else
              "⚠ #{length(nodes)} nodos, sin líder"
            end
          end
      end

      IO.puts("#{nombre}: #{status}")
    end)

    IO.puts("\nResultado: #{activos}/#{total} servicios activos")

    if activos < total do
      IO.puts("\nAlgunos servicios no están corriendo")
    end

    {activos, total}
  end

  def listar_compras() do
    try do
      case find_service_node("compras") do
        nil ->
          []

        node ->
          case :rpc.call(node, Libremarket.Compras.Server, :listar_compras, [], 5000) do
            {:badrpc, _} -> []
            compras -> compras
          end
      end
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
  end

  def listar_infracciones() do
    try do
      # Buscar en todos los nodos donde pueda estar el servicio de infracciones
      nodes = [Node.self() | Node.list()]

      # Primero intentar buscar un nodo específico de infracciones
      infracciones_node = Enum.find(nodes, fn node ->
        node_str = Atom.to_string(node)
        String.contains?(node_str, "infracciones")
      end)

      # Si no existe nodo específico, buscar en todos los nodos
      node = infracciones_node || Enum.find(nodes, fn n ->
        try do
          :rpc.call(n, Process, :whereis, [Libremarket.Infracciones.Server], 2000) != nil
        catch
          _, _ -> false
        end
      end)

      case node do
        nil ->
          IO.puts("No se encontró el servicio de infracciones")
          []

        n ->
          case :rpc.call(n, Libremarket.Infracciones.Server, :listar_infracciones, [], 5000) do
            {:badrpc, _} -> []
            infracciones -> infracciones
          end
      end
    rescue
      error ->
        IO.puts("Error obteniendo infracciones: #{inspect(error)}")
        []
    end
  end
end
