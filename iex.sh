# iex.sh
docker run -it --rm \
  -v "$(pwd)":/app -w /app \
  -u $(id -u):$(id -g) \
  --network host \
  -e MIX_HOME=/app/mix_home \
  -e HEX_HOME=/app/hex_home \
  elixir:alpine \
  iex --sname "$1" --cookie "$2" -S mix
