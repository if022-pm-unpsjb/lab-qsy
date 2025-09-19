defmodule Libremarket.Envios do
  @moduledoc false

  # Simula cálculo del costo de envío
  def calcular(:retira), do: 0
  def calcular(:correo), do: Enum.random(500..1500)
end

defmodule Libremarket.Envios.Server do
  @moduledoc """
  Servidor de Envíos
  """

  use GenServer

  @global_name {:global, __MODULE__}

  @doc "Arranca el servidor de Envíos."
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  @doc """
  Procesa un envío para un `idCompra` con una `forma_entrega` (:retira | :correo).
  Devuelve un mapa con el resultado del envío y su costo.
  """
  def procesarEnvio(pid \\ @global_name, idCompra, forma_entrega) do
    GenServer.call(pid, {:procesarEnvio, idCompra, forma_entrega})
  end

  @doc """
  Lista todos los envíos procesados.
  """
  def listarEnvios(pid \\ @global_name) do
    GenServer.call(pid, :listarEnvios)
  end

  # =========
  # Callbacks
  # =========

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:procesarEnvio, idCompra, forma_entrega}, _from, state) do
    costo = Libremarket.Envios.calcular(forma_entrega)

    envio = %{
      id_compra: idCompra,
      forma: forma_entrega,
      costo: costo
    }

    state = Map.put(state, idCompra, envio)
    {:reply, envio, state}
  end

  @impl true
  def handle_call(:listarEnvios, _from, state) do
    {:reply, state, state}
  end
end
