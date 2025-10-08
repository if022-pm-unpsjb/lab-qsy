defmodule LibreMarket.AMQP.Connection do
  @moduledoc """
  Gestiona la conexión con RabbitMQ
  """
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_channel do
    GenServer.call(__MODULE__, :get_channel)
  end

  @impl true
  def init(_opts) do
    send(self(), :connect)
    {:ok, %{conn: nil, channel: nil}}
  end

  @impl true
  def handle_info(:connect, state) do
    case connect() do
      {:ok, conn, channel} ->
        Process.monitor(conn.pid)
        Logger.info("Conectado a RabbitMQ")
        {:noreply, %{conn: conn, channel: channel}}

      {:error, reason} ->
        Logger.error("Error conectando a RabbitMQ: #{inspect(reason)}")
        # Reintentar en 5 segundos
        Process.send_after(self(), :connect, 5000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, reason}, _state) do
    Logger.error("Conexión RabbitMQ perdida: #{inspect(reason)}")
    send(self(), :connect)
    {:noreply, %{conn: nil, channel: nil}}
  end

  @impl true
  def handle_call(:get_channel, _from, %{channel: channel} = state) do
    {:reply, {:ok, channel}, state}
  end

  defp connect do
    amqp_url = Application.get_env(:libremarket, :amqp)[:url]

    with {:ok, conn} <- AMQP.Connection.open(amqp_url),
         {:ok, channel} <- AMQP.Channel.open(conn) do
      {:ok, conn, channel}
    end
  end
end
