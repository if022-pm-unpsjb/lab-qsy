defmodule Libremarket.AMQP.Connection do
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
  def handle_call(:get_channel, _from, %{channel: nil} = state) do
    {:reply, {:error, :no_channel}, state}
  end

  @impl true
  def handle_call(:get_channel, _from, %{channel: channel} = state) do
    # Verificar si el canal está vivo
    if Process.alive?(channel.pid) do
      {:reply, {:ok, channel}, state}
    else
      Logger.warning("Canal AMQP muerto, reconectando...")
      send(self(), :connect)
      {:reply, {:error, :channel_dead}, %{state | channel: nil}}
    end
  end

  defp connect do
    amqp_config = Application.get_env(:libremarket, :amqp)
    amqp_url = if amqp_config, do: amqp_config[:url], else: nil

    if is_nil(amqp_url) do
      Logger.error("AMQP_URL no configurada. Verifica config/runtime.exs o la variable de entorno AMQP_URL")
      {:error, :no_amqp_url}
    else
      # Opciones SSL para CloudAMQP
      ssl_options = [
        cacerts: :public_key.cacerts_get(),
        verify: :verify_peer,
        server_name_indication: :disable,
        depth: 3
      ]

      connection_options = [
        ssl_options: ssl_options
      ]

      with {:ok, conn} <- AMQP.Connection.open(amqp_url, connection_options),
           {:ok, channel} <- AMQP.Channel.open(conn) do
        {:ok, conn, channel}
      end
    end
  end
end
