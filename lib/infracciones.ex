defmodule Libremarket.Infracciones do
  @moduledoc false

  @spec detectarInfracciones() :: atom()
  def detectarInfracciones() do
   Enum.random([true, false])
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

  def detectarInfracciones(pid \\ __MODULE__, idCompra) do
    GenServer.call(pid, {:detectarInfracciones,idCompra} )
  end

  def listarInfracciones() do
    GenServer.call(__MODULE__, :listarInfracciones)
  end

  # Callbacks
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:detectarInfracciones,idCompra}, _from, state) do
    infraccion = Libremarket.Infracciones.detectarInfracciones()
    state = Map.put(state,idCompra,infraccion)
    {:reply, infraccion,state}
  end

   @impl true
  def handle_call(:listarInfracciones, _from, state) do
    {:reply, state, state}
  end



end
