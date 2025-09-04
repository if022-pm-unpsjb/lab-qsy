defmodule Libremarket.Compras do
  @doc """
  Lógica principal del proceso de compra.
  """
  def procesar_compra(producto_id, medio_pago, forma_entrega) do
    # 1. Verificar stock
    case Libremarket.Ventas.Server.verificar_proucto(producto_id) do
      {:error, :sin_stock} ->
        {:error, :sin_stock}

      {:error, :producto_no_encontrado} ->
        {:error, :producto_no_encontrado}

      {:ok, _producto} ->
        # 2. Confirmar compra y reservar producto
        case Libremarket.Ventas.Server.confirmar_venta(producto_id) do
          {:error, reason} ->
            {:error, reason}

          {:ok, producto_reservado} ->
            # 3. Detectar infracciones
            case Libremarket.Infracciones.Server.detectar_infracciones(producto_id) do
              true ->
                # Liberar producto (devolvemos stock +1)
                reponer_producto(producto_reservado.id)
                {:error, :infraccion_detectada}

              false ->
                # 4. Procesar pago
                case Libremarket.Pago.Server.procesarPago(producto_id) do
                  false ->
                    reponer_producto(producto_reservado.id)
                    {:error, :pago_rechazado}

                  true ->
                    # 5. Registrar envío
                    envio = Libremarket.Envio.Server.procesarEnvio(producto_id, forma_entrega)

                    compra = %{
                      producto: producto_reservado,
                      medio_pago: medio_pago,
                      forma_entrega: forma_entrega,
                      envio: envio,
                      timestamp: DateTime.utc_now()
                    }

                    {:ok, compra}
                end
            end
        end
    end
  end

  # Función auxiliar para reponer stock en caso de infracción
  defp reponer_producto(producto_id) do
    # Sencillo: reducimos -1 en confirmar_venta, acá sumamos +1 manual
    GenServer.cast(Libremarket.Ventas.Server, {:reponer_stock, producto_id})
  end
end

defmodule Libremarket.Compras.Server do
  use GenServer

  # API del cliente
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, producto_id, medio_pago, forma_entrega) do
    GenServer.call(pid, {:comprar, producto_id, medio_pago, forma_entrega}, 10000)
  end

  @impl true
  def init(state) do
    IO.puts("Servidor de Compras iniciado")
    {:ok, Map.put(state, :compras_realizadas, [])}
  end

  @impl true
  def handle_call({:comprar, producto_id, medio_pago, forma_entrega}, _from, state) do
    resultado = Libremarket.Compras.procesar_compra(producto_id, medio_pago, forma_entrega)

    nuevo_state =
      case resultado do
        {:ok, compra} ->
          %{state | compras_realizadas: [compra | state.compras_realizadas]}

        _ ->
          state
      end

    {:reply, resultado, nuevo_state}
  end
end
