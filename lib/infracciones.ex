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

  def detectarInfracciones(pid \\ __MODULE__) do
    GenServer.call(pid, :detectarInfracciones)
  end

  # Callbacks
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:detectarInfracciones, _from, state) do
    result = Libremarket.Infracciones.detectarInfracciones()
    {:reply, result, state}
  end
end
