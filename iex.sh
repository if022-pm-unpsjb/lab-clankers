# iex.sh
docker run -it --rm \
  -v "$(pwd)":/app -w /app \
  -u $(id -u):$(id -g) \
  --network libremarket \
  -e MIX_HOME=/app/mix_home \
  -e HEX_HOME=/app/hex_home \
  -e AMQP_URL="amqps://oiawxlbc:v4Z6nPfJiy01FZw3QmObwAPPz90bs8t-@jackal.rmq.cloudamqp.com/oiawxlbc?heartbeat=30&connection_timeout=15000" \
  elixir:1.15.7-otp-26-alpine \
  iex --sname "$1" --cookie "$2" -S mix
