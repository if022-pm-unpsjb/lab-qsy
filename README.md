# Libremarket

Proyecto base para la aplicación LibreMarket.

## Docker

Para utilizar Docker para el desarrollo, usar los siguientes scripts:

- Para compilar la aplicación, ejecutar `./compile.sh`

- Para iniciar el interprete `iex` con el proyecto ejecutar `./iex.sh`

- Para actualizar las dependencias ejecutar `./mix-deps-gets.sh`

- Para levantar/detener la aplicación en múltiples contenedores Docker, usar `./libremarket start|stop`

## Mise

Como alternativa, para instalar un entorno local de Elixir usar [mise-en-place](https://mise.jdx.dev/):

- Primero, seguir la [guía de instalación](https://mise.jdx.dev/getting-started.html) de mise-en-place.

- Indicar que se quiere usar [Elixir como entorno](https://mise.jdx.dev/lang/elixir.html).
