# config/config.exs - Configuración actualizada para Docker
import Config

# Configuración de nodos para Docker con --sname
config :libremarket,
  nodos: %{
    ventas: :ventas@maty,
    compras: :compras@maty,
    envios: :envios@maty,
    pagos: :pagos@maty,
    infracciones: :infracciones@maty,
    simulador: :simulador@maty
  }

# Configuración por ambiente
import_config "#{config_env()}.exs"
