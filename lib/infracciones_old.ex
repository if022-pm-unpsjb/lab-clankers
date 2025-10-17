# defmodule Libremarket.Infracciones do
#   @moduledoc "Lógica de infracciones"

#   @doc "Retorna true ~30% de las veces; usa el id_compra solo para trazas si querés."
#   def detectar_infraccion(_id_compra) do
#     infraccion? = :rand.uniform(100) <= 30
#     IO.puts("Hay infracción: #{infraccion?}")
#     infraccion?
#   end
# end
# defmodule Libremarket.Infracciones.Server do
#   @moduledoc "Infracciones"
#   use GenServer
#   require Logger
#   alias AMQP.{Connection, Channel, Queue, Basic}

#   @global_name {:global, __MODULE__}
#   @req_q  "infracciones.req"
#   @resp_q "compras.resp"

#   @min_backoff 500
#   @max_backoff 10_000

#   # ===== API cliente =====
#   def start_link(opts \\ %{}) do
#     GenServer.start_link(__MODULE__, opts, name: @global_name)
#   end

#   def detectarInfraccion(pid \\ @global_name, id_compra),
#     do: GenServer.call(pid, {:detectarInfraccion, id_compra})

#   def listarInfracciones(pid \\ @global_name),
#     do: GenServer.call(pid, :listarInfracciones)

#   # ===== Callbacks =====
#   @impl true
#   def init(_opts) do
#     Process.flag(:trap_exit, true)
#     state = %{conn: nil, chan: nil, backoff: @min_backoff}
#     # conectamos en diferido para no romper init
#     send(self(), :connect)
#     {:ok, state}
#   end

#   @impl true
#   def handle_info(:connect, %{backoff: backoff} = state) do
#     case connect_amqp() do
#       {:ok, conn, chan} ->
#         Logger.info("Infracciones.Server conectado a AMQP")
#         # Declaraciones idempotentes
#         {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
#         {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
#         {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: true)
#         Process.monitor(conn.pid)
#         {:noreply, %{state | conn: conn, chan: chan, backoff: @min_backoff}}

#       {:error, reason} ->
#         Logger.warning("AMQP no conectado (#{inspect(reason)}). Reintento en #{backoff} ms")
#         Process.send_after(self(), :connect, backoff)
#         {:noreply, %{state | conn: nil, chan: nil, backoff: min(backoff * 2, @max_backoff)}}
#     end
#   end

#   # Si la conexión/chán se cae, reconectamos
#   @impl true
#   def handle_info({:DOWN, _mref, :process, _pid, reason}, state) do
#     Logger.warning("AMQP DOWN: #{inspect(reason)}. Reintentando...")
#     safe_close(state.chan)
#     safe_close(state.conn)
#     Process.send_after(self(), :connect, @min_backoff)
#     {:noreply, %{state | conn: nil, chan: nil, backoff: @min_backoff}}
#   end

#   # Mensaje de negocio
#   @impl true
#   def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = state)
#       when is_binary(payload) and is_map(meta) and not is_nil(chan) do
#     cid = Map.get(meta, :correlation_id)

#     case Jason.decode(payload) do
#       {:ok, %{"id_compra" => id_compra}} ->
#         infr = Libremarket.Infracciones.detectar_infraccion(id_compra)
#         resp = Jason.encode!(%{id_compra: id_compra, infraccion: infr})

#         Basic.publish(
#           chan, "", @resp_q, resp,
#           correlation_id: cid,
#           content_type: "application/json"
#         )

#       other ->
#         Logger.warn("Payload inválido en #{@req_q}: #{inspect(other)}")
#     end

#     {:noreply, state}
#   end

#   # Si aún no hay canal, descartamos
#   def handle_info({:basic_deliver, _p, _m}, state) do
#     Logger.debug("Mensaje recibido pero sin canal; descartado.")
#     {:noreply, state}
#   end

#   @impl true
#   def handle_info({:basic_consume_ok, _}, state), do: {:noreply, state}
#   @impl true
#   def handle_info({:basic_cancel, _}, state), do: {:stop, :normal, state}
#   @impl true
#   def handle_info({:basic_cancel_ok, _}, state), do: {:noreply, state}
#   @impl true
#   def handle_info(msg, state) do
#     Logger.debug("Infracciones.Server ignoró: #{inspect(msg)}")
#     {:noreply, state}
#   end

#   # ===== Helpers AMQP =====

#   # Conexión "insegura" si INSECURE_AMQPS=1 y la URL es amqps:// (sin CA)
#   defp connect_amqp() do
#     with {:ok, url} <- fetch_amqp_url() do
#       insecure? = System.get_env("INSECURE_AMQPS") in ["1", "true", "TRUE"]
#       opts =
#         [connection_timeout: 15_000, requested_heartbeat: 30]
#         |> maybe_insecure_ssl(url, insecure?)

#       case Connection.open(url, opts) do
#         {:ok, conn} ->
#           case Channel.open(conn) do
#             {:ok, chan} -> {:ok, conn, chan}
#             {:error, reason} ->
#               safe_close(conn)
#               {:error, reason}
#           end

#         {:error, reason} ->
#           {:error, reason}
#       end
#     end
#   end

#   defp maybe_insecure_ssl(opts, url, true) do
#     # sólo si es amqps://
#     if String.starts_with?(url, "amqps://") do
#       # desactiva verificación de certificado/hostname (evita instalar CA)
#       Keyword.merge(opts, ssl_options: [verify: :verify_none, server_name_indication: :disable])
#     else
#       opts
#     end
#   end
#   defp maybe_insecure_ssl(opts, _url, _), do: opts

#   defp fetch_amqp_url() do
#     case System.get_env("AMQP_URL") |> to_string() |> String.trim() do
#       "" -> {:error, :missing_amqp_url}
#       url -> {:ok, url}
#     end
#   end

#   defp safe_close(%Channel{} = ch), do: Channel.close(ch)
#   defp safe_close(%Connection{} = c), do: Connection.close(c)
#   defp safe_close(_), do: :ok
# end
