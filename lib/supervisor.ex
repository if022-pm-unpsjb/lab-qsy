defmodule Libremarket.Supervisor do
  use Supervisor
  require Logger

  @doc """
  Inicia el supervisor
  """
  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    topologies = [
      gossip: [
        strategy: Cluster.Strategy.Gossip,
        config: [
          port: 45892,
          if_addr: "0.0.0.0",
          multicast_addr: "127.0.0.1",
          broadcast_only: true,
          secret: "secret"
        ]
      ]
    ]

    # Servicios AMQP (siempre activos) - Solo Connection
    amqp_services = [
      Libremarket.AMQP.Connection
    ]

    # Determinar qué servicios ejecutar según SERVER_TO_RUN
    server_to_run = case System.get_env("SERVER_TO_RUN") do
      nil ->
        Logger.info("No se especificó SERVER_TO_RUN, no se iniciará ningún servicio de negocio")
        []

      # Servicio de Ventas con su Consumer
      "Elixir.Libremarket.Ventas.Server" ->
        Logger.info("Iniciando servicio de Ventas con Consumer")
        [
          {Libremarket.Ventas.Server, %{}},
          Libremarket.Ventas.Consumer
        ]

      # Servicio de Compras con su Consumer
      "Elixir.Libremarket.Compras.Server" ->
        Logger.info("Iniciando servicio de Compras con Consumer")
        [
          {Libremarket.Compras.Server, %{}},
          Libremarket.Compras.Consumer
        ]

      # Servicio de Pagos con su Consumer
      "Elixir.Libremarket.Pagos.Server" ->
        Logger.info("Iniciando servicio de Pagos con Consumer")
        [
          {Libremarket.Pagos.Server, %{}},
          Libremarket.Pagos.Consumer
        ]

      # Servicio de Envíos con su Consumer
      "Elixir.Libremarket.Envios.Server" ->
        Logger.info("Iniciando servicio de Envíos con Consumer")
        [
          {Libremarket.Envios.Server, %{}},
          Libremarket.Envios.Consumer
        ]

      # Servicio de Infracciones con su Consumer
      "Elixir.Libremarket.Infracciones.Server" ->
        Logger.info("Iniciando servicio de Infracciones con Consumer")
        [
          {Libremarket.Infracciones.Server, %{}},
          Libremarket.Infracciones.Consumer
        ]

      # Servicio REST (solo API, sin consumer)
      "Elixir.Libremarket.ServiceRest" ->
        Logger.info("Iniciando servicio REST API")
        [Libremarket.ServiceRest]

      # Simulador (solo para testing, ejecuta Compras)
      "Elixir.Libremarket.Compras.Server" ->
        Logger.info("Iniciando Compras (modo simulador)")
        [
          {Libremarket.Compras.Server, %{}},
          Libremarket.Compras.Consumer,
          Libremarket.ServiceRest  # Incluir REST en compras para testing
        ]

      server_name ->
        Logger.warning("Servidor no reconocido: #{server_name}")
        # Intentar parsear el nombre y crear el servicio
        try do
          module = String.to_existing_atom(server_name)
          Logger.info("Iniciando módulo: #{inspect(module)}")
          [{module, %{}}]
        rescue
          ArgumentError ->
            Logger.error("No se pudo convertir #{server_name} a módulo existente")
            []
        end
    end

    # Construir lista de children
    children =
      [
        {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]}
      ] ++ amqp_services ++ server_to_run

    Logger.info("Total de servicios a iniciar: #{length(children)}")
    Logger.info("- Cluster: 1")
    Logger.info("- AMQP Connection: 1")
    Logger.info("- Servicios de negocio: #{length(server_to_run)}")

    Supervisor.init(children, strategy: :one_for_one)
  end
end
