# iex.sh
docker run -it --rm \
  -v "$(pwd)":/app -w /app \
  -u $(id -u):$(id -g) \
  --network host \
  -e MIX_HOME=/app/mix_home \
  -e HEX_HOME=/app/hex_home \
  -e ENABLE_REST=${ENABLE_REST:-false} \
  -e REST_PORT=${REST_PORT:-4000} \
  elixir:alpine \
  iex --sname "$1" --cookie "$2" -S mix
