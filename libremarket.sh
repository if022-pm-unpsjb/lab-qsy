#!/bin/bash
start() {
    # Levantar los contenedores en segundo plano
    export DOCKER_UID=$UID
    export DOCKER_GID=$GID
    export SECRET=secret
    docker compose up -d "$@"
}

stop() {
    export DOCKER_UID=$UID
    export DOCKER_GID=$GID
    export SECRET=secret
    
    # Si se pasan argumentos, parar contenedores específicos
    if [ $# -gt 0 ]; then
        docker compose stop "$@"
    else
        # Si no se pasan argumentos, parar todos
        docker compose down
    fi
}

# Comprobar el argumento proporcionado
if [[ $1 == "start" ]]; then
    shift
    start "$@"
elif [[ $1 == "stop" ]]; then
    shift
    stop "$@"
elif [[ $1 == "build" ]]; then
    docker run -it --rm -v "$(pwd)":/app -w /app -u $(id -u):$(id -g) -e MIX_HOME=/app/mix_home -e HEX_HOME=/app/hex_home --network host elixir:1.15.7-alpine mix compile
elif [[ $1 == "iex" ]]; then
    docker attach $2
else
    echo "Uso: $0 {start|stop [contenedor...]|iex nombre_de_contenedor|build}"
    exit 1
fi