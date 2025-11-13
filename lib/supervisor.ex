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

    base_services = [
      {Registry, keys: :unique, name: Libremarket.Registry},
      {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]},
      Libremarket.AMQP.Connection
    ]

    server_to_run = case System.get_env("SERVER_TO_RUN") do
      nil ->
        Logger.info("No se especificó SERVER_TO_RUN, no se iniciará ningún servicio de negocio")
        []

      "Elixir.Libremarket.Ventas.Server" ->
        Logger.info("✓ Iniciando nodo de Ventas con elección de líder (Bully Algorithm)")
        [
          {Libremarket.Ventas.Server, %{}},
          Libremarket.Ventas.Consumer
        ]

      "Elixir.Libremarket.Compras.Server" ->
        Logger.info("✓ Iniciando nodo de Compras con elección de líder (Bully Algorithm)")
        [
          {Libremarket.Compras.Server, %{}},
          Libremarket.Compras.Consumer
        ]

      "Elixir.Libremarket.Pagos.Server" ->
        Logger.info("✓ Iniciando nodo de Pagos con elección de líder (Bully Algorithm)")
        [
          {Libremarket.Pagos.Server, %{}},
          Libremarket.Pagos.Consumer
        ]

      "Elixir.Libremarket.Envios.Server" ->
        Logger.info("✓ Iniciando nodo de Envíos con elección de líder (Bully Algorithm)")
        [
          {Libremarket.Envios.Server, %{}},
          Libremarket.Envios.Consumer
        ]

      "Elixir.Libremarket.Infracciones.Server" ->
        Logger.info("Iniciando nodo de Infracciones con elección de líder (Bully Algorithm)")
        [
          {Libremarket.Infracciones.Server, %{}},
          Libremarket.Infracciones.Consumer
        ]

      "Elixir.Libremarket.ServiceRest" ->
        Logger.info("Iniciando servicio REST API")
        [Libremarket.ServiceRest]

      server_name ->
        Logger.warning("Servidor no reconocido: #{server_name}")
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

    children = base_services ++ server_to_run

    Logger.info("Total de servicios a iniciar: #{length(children)}")
    Logger.info("- Servicios base: #{length(base_services)}")
    Logger.info("- Servicios de negocio: #{length(server_to_run)}")

    Supervisor.init(children, strategy: :one_for_one)
  end
end
