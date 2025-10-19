import Config

# Configuración en tiempo de ejecución para AMQP
config :libremarket, :amqp,
  url:
    System.get_env("AMQP_URL") ||
      "amqps://jhhknvcs:pl-s-jfqppxPfNzU2Qn-oYUlmwUwxWqa@ram.lmq.cloudamqp.com/jhhknvcs"
