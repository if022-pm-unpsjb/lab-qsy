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

  # Listar productos disponibles
  get "/productos" do
    productos = Libremarket.Ui.listar_productos()
    send_resp(conn, 200, Jason.encode!(productos))
  end

  # Listar compras realizadas
  get "/compras" do
    compras = Libremarket.Compras.Server.listar_compras()
    send_resp(conn, 200, Jason.encode!(compras))
  end

  # Listar infracciones detectadas
  get "/infracciones" do
    try do
      # Buscar nodo de infracciones en el cluster
      infracciones_nodes = Node.list()
        |> Enum.filter(&String.contains?(to_string(&1), "infracciones"))

      infracciones = case infracciones_nodes do
        [node | _] ->
          # Llamar al primer nodo de infracciones encontrado
          case :rpc.call(node, Libremarket.Infracciones.Server, :listar_infracciones, [], 5000) do
            {:badrpc, reason} ->
              IO.puts("Error RPC: #{inspect(reason)}")
              []
            result when is_list(result) ->
              result
            _ ->
              []
          end
        [] ->
          # Si no hay nodos remotos, intentar local
          case Process.whereis(Libremarket.Infracciones.Server) do
            nil -> []
            _pid -> Libremarket.Infracciones.Server.listar_infracciones()
          end
      end

      send_resp(conn, 200, Jason.encode!(infracciones))
    rescue
      error ->
        IO.puts("Error obteniendo infracciones: #{inspect(error)}")
        send_resp(conn, 500, Jason.encode!(%{error: "Error interno", details: inspect(error)}))
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
