defmodule Libremarket.Compras do
  @doc """
  Lógica principal del proceso de compra
  """

  def procesar_compra(producto_id, medio_pago, forma_entrega) do
    Libremarket.Infracciones.Server.detectar_infracciones()
  end
end

defmodule Libremarket.Compras.Server do
  @moduledoc """
  Compras
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Compras
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, producto_id, medio_pago, forma_entrega) do
    GenServer.call(pid, {:comprar, producto_id, medio_pago, forma_entrega}, 10000)
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    IO.puts("Servidor de Compras iniciado")
    {:ok, Map.put(state, :compras_realizadas, [])}
  end

  @doc """
  Callback para un call :comprar
  """
  @impl true
  def handle_call({:comprar, producto_id, medio_pago, forma_entrega}, _from, state) do
    # TODO: implementar procesar_compra
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
