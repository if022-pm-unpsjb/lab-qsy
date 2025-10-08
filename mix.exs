defmodule Libremarket.MixProject do
  use Mix.Project

  def project do
    [
      app: :libremarket,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Libremarket, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:libcluster, "~> 3.4"},
      {:plug, "~> 1.14"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:amqp, "~> 3.3"},
      {:rabbit_common, "~> 3.13", override: true}
    ]
  end
end
