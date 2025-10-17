#!/bin/bash

IMAGE="elixir:1.15.7-otp-26-alpine"

start() {
  export SECRET=secret
  export DOCKER_UID=$(id -u)
  export DOCKER_GID=$(id -g)
  docker compose up -d "$@"
}

stop() {
  export SECRET=secret
  export DOCKER_UID=$(id -u)
  export DOCKER_GID=$(id -g)
  docker compose down
}

restart(){ stop; start; }

clean() {
  # Limpio artefactos en el host (montados en /app dentro del contenedor)
  rm -rf _build deps mix_home hex_home build_path rebar_cache .rebar3
  # (opcional) evitar usar la tag flotante por accidente
  docker rmi elixir:alpine 2>/dev/null || true
  docker pull "$IMAGE"
}

build() {
  docker run -it --rm \
    -v "$(pwd)":/app -w /app \
    -u $(id -u):$(id -g) \
    -e MIX_HOME=/app/mix_home \
    -e HEX_HOME=/app/hex_home \
    -e REBAR_CACHE_DIR=/app/rebar_cache \
    -e REBAR_PROFILE=dev \
    -e MIX_ENV=dev \
    --network host \
    "$IMAGE" \
    sh -lc 'mix local.hex --force && mix local.rebar --force && mix deps.get && mix deps.compile --force && mix compile'
}

nuke() {
  # stop -> clean -> build -> start (reenvía args a start, p.ej. servicios)
  stop
  clean
  build
  start "$@"
}

if [[ $1 == "start" ]]; then
  shift; start "$@"
elif [[ $1 == "stop" ]]; then
  stop
elif [[ $1 == "restart" ]]; then
  restart
elif [[ $1 == "clean" ]]; then
  clean
elif [[ $1 == "build" ]]; then
  build
elif [[ $1 == "nuke" ]]; then
  shift; nuke "$@"
elif [[ $1 == "iex" ]]; then
  docker attach "$2"
else
  echo "Uso: $0 {start|stop|restart|clean|build|nuke|iex nombre_de_contenedor}"
  exit 1
fi
