defmodule Libremarket.Pagos do
  @moduledoc false

  def procesar() do
    # 70% true, 30% false
    Enum.random(1..100) <= 70
  end
end

defmodule Libremarket.Pagos.Server do
  @moduledoc """
  Servidor de Pagos
  """

  use GenServer

  # =========
  # API
  # =========

  @doc "Arranca el servidor de Pagos."
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Procesa un pago identificado por `idPago`.
  Devuelve `true` (aceptado) o `false` (rechazado).
  """
  def procesarPago(pid \\ __MODULE__, idPago) do
    GenServer.call(pid, {:procesarPago, idPago})
  end

  # =========
  # Callbacks
  # =========

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:procesarPago, idPago}, _from, state) do
    resultado = Libremarket.Pagos.procesar()
    state = Map.put(state, idPago, resultado)
    {:reply, resultado, state}
  end

  @impl true
  def handle_call(:listarPagos, _from, state) do
    {:reply, state, state}
  end
end
