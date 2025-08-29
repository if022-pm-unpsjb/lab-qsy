defmodule Libremarket.Infracciones do
  @moduledoc false

  @doc """
  Detecta infracciones con 30% de probabilidad
  """
  def detectar_infracciones() do
    :rand.uniform(100) <= 30
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer

  # API del cliente
  @doc """
  Crea un nuevo servidor de Infracciones.
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectar_infracciones(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:detectar_infracciones, id_compra})
  end

  def listar_infracciones(pid \\ __MODULE__) do
    GenServer.call(pid, :listar_infracciones)
  end

  # Callbacks
  @impl true
  def init(_state) do
    IO.puts("Servidor de Infracciones iniciado")
    {:ok, %{infracciones: %{}}}
  end

  @impl true
  def handle_call({:detectar_infracciones, id_compra}, _from, state) do
    tiene_infraccion = Libremarket.Infracciones.detectar_infracciones()

    nuevo_state =
      if tiene_infraccion do
        nuevas_infracciones = [
          %{id_compra: id_compra, timestamp: DateTime.utc_now()} | state.infracciones
        ]

        %{state | infracciones: nuevas_infracciones}
      else
        state
      end

    {:reply, tiene_infraccion, nuevo_state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state.infracciones, state}
  end
end
