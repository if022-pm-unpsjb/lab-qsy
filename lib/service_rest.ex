defmodule Libremarket.ServiceRest do
  @moduledoc """
  Servicio REST para exponer los recursos de Libremarket.
  Permite listar productos, realizar compras y consultar compras.
  """

  use Plug.Router
  use Plug.ErrorHandler

  plug Plug.Logger
  plug :match
  plug Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason

  plug :dispatch

  # Funciones auxiliares para encontrar nodos
  defp find_compras_node() do
    nodes = [Node.self() | Node.list()]

    Enum.find(nodes, fn node ->
      node_str = Atom.to_string(node)
      String.contains?(node_str, "compras")
    end)
  end

  # Listar productos disponibles
  get "/productos" do
    productos = Libremarket.Ui.listar_productos()
    send_resp(conn, 200, Jason.encode!(productos))
  end

  # Listar compras realizadas
  get "/compras" do
    case find_compras_node() do
      nil ->
        send_resp(conn, 503, Jason.encode!(%{
          error: "Servicio de compras no disponible"
        }))

      node ->
        try do
          compras = :rpc.call(node, Libremarket.Compras.Server, :listar_compras, [])
          send_resp(conn, 200, Jason.encode!(compras))
        rescue
          error ->
            IO.puts("Error obteniendo compras: #{inspect(error)}")
            send_resp(conn, 500, Jason.encode!(%{
              error: "Error interno",
              details: inspect(error)
            }))
        end
    end
  end

  # Listar infracciones detectadas
  get "/infracciones" do
    try do
      infracciones = Libremarket.Ui.listar_infracciones()
      send_resp(conn, 200, Jason.encode!(infracciones))
    rescue
      error ->
        IO.puts("Error obteniendo infracciones: #{inspect(error)}")
        send_resp(conn, 500, Jason.encode!(%{
          error: "Error interno",
          details: inspect(error)
        }))
    end
  end

  # Realizar una compra
  post "/comprar" do
    case conn.body_params do
      %{
        "producto_id" => producto_id,
        "medio_pago" => medio_pago,
        "forma_entrega" => forma_entrega
      } ->
        case Libremarket.Ui.comprar(producto_id, String.to_atom(forma_entrega), String.to_atom(medio_pago)) do
          {:ok, compra} ->
            send_resp(conn, 201, Jason.encode!(%{
              status: "ok",
              compra: compra
            }))

          {:error, reason} ->
            send_resp(conn, 400, Jason.encode!(%{
              status: "error",
              reason: to_string(reason)
            }))
        end

      _ ->
        send_resp(conn, 400, Jason.encode!(%{error: "JSON inválido"}))
    end
  end

  match _ do
    send_resp(conn, 404, Jason.encode!(%{error: "Ruta no encontrada"}))
  end

  def handle_errors(conn, %{reason: reason}) do
    IO.puts("Error en request: #{inspect(reason)}")
    send_resp(conn, conn.status, Jason.encode!(%{error: "Error interno"}))
  end

  def child_spec(_opts) do
    Plug.Cowboy.child_spec(
      scheme: :http,
      plug: __MODULE__,
      options: [port: 4001]
    )
  end
end
