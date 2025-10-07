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

  # =========
  # RUTAS
  # =========

  # Listar productos disponibles
  get "/productos" do
    productos = Libremarket.Ui.listar_productos()
    send_resp(conn, 200, Jason.encode!(productos))
  end

  # Listar compras realizadas
  get "/compras" do
    compras =
      GenServer.call({:global, Libremarket.Compras.Server}, :listar_compras)

    send_resp(conn, 200, Jason.encode!(compras))
  end

  # Realizar una compra
  post "/comprar" do
    case conn.body_params do
      %{
        "producto_id" => producto_id,
        "medio_pago" => medio_pago,
        "forma_entrega" => forma_entrega
      } ->
        case Libremarket.Ui.comprar(producto_id, forma_entrega, medio_pago) do
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

  # Ruta por defecto
  match _ do
    send_resp(conn, 404, Jason.encode!(%{error: "Ruta no encontrada"}))
  end

  # =========
  # MANEJO DE ERRORES
  # =========
  def handle_errors(conn, %{reason: reason}) do
    IO.puts("Error en request: #{inspect(reason)}")
    send_resp(conn, conn.status, Jason.encode!(%{error: "Error interno"}))
  end

  # =========
  # INTEGRACIÓN CON SUPERVISOR
  # =========
  def child_spec(_opts) do
    Plug.Cowboy.child_spec(
      scheme: :http,
      plug: __MODULE__,
      options: [port: 4001]
    )
  end
end
