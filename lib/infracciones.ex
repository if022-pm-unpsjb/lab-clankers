defmodule Libremarket.Infracciones do
  @moduledoc "Lógica de infracciones"

  @doc "Retorna true ~30% de las veces; usa el id_compra solo para trazas si querés."
  def detectar_infraccion(_id_compra) do
    infraccion? = :rand.uniform(100) <= 30
    IO.puts("Hay infracción: #{infraccion?}")
    infraccion?
  end
end


defmodule Libremarket.Infracciones.Server do
  @moduledoc "Infracciones"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, System.get_env("NOMBRE") || __MODULE__}
  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  # ===== API cliente =====
  def start_link(opts \\ %{}) do
    Logger.info("[Infracciones.Server] start_link/1 con opts=#{inspect(opts)}")
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def listarInfracciones(pid \\ @global_name) do
    Logger.info("[Infracciones.Server] listarInfracciones/1 → call")
    GenServer.call(pid, :listarInfracciones)
  end

  # ===== Callbacks =====
  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    base_state = %{infracciones: %{}}

    if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
      Logger.info("[Infracciones.Server] init/1 → preparando conexión AMQP diferida")
      state_primario = Map.merge(base_state, %{conn: nil, chan: nil, backoff: @min_backoff})
      send(self(), :connect)
      {:ok, state_primario}
    else
      {:ok, base_state}
    end
  end


  @impl true
  def handle_call(:listarInfracciones, _from, state) do
    Logger.info("[Infracciones.Server] handle_call(:listarInfracciones) → reply name=#{inspect(@global_name)} node=#{inspect(node())}")
    {:reply, state.infracciones, state}
  end


  @impl true
  def handle_info(:connect, %{backoff: backoff} = state) do
    Logger.info("[Infracciones.Server] handle_info(:connect) → intento de conexión AMQP (backoff=#{backoff}ms)")
    case connect_amqp() do
      {:ok, conn, chan} ->
        Logger.info("[Infracciones.Server] Conectado a AMQP ✔️")
        # Declaraciones idempotentes
        {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
        {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
        Logger.info("[Infracciones.Server] Declaradas colas req='#{@req_q}' resp='#{@resp_q}'")
        {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: true)
        Logger.info("[Infracciones.Server] Consumiendo de '#{@req_q}' (no_ack=true)")
        Process.monitor(conn.pid)
        {:noreply, %{state | conn: conn, chan: chan, backoff: @min_backoff}}

      {:error, reason} ->
        Logger.warning("[Infracciones.Server] AMQP no conectado (#{inspect(reason)}). Reintento en #{backoff} ms")
        Process.send_after(self(), :connect, backoff)
        {:noreply, %{state | conn: nil, chan: nil, backoff: min(backoff * 2, @max_backoff)}}
    end
  end

  # Si la conexión/chán se cae, reconectamos
  @impl true
  def handle_info({:DOWN, _mref, :process, pid, reason}, state) do
    Logger.warning("[Infracciones.Server] AMQP DOWN desde pid=#{inspect(pid)} reason=#{inspect(reason)}. Reintentando…")
    safe_close(state.chan)
    safe_close(state.conn)
    Process.send_after(self(), :connect, @min_backoff)
    {:noreply, %{state | conn: nil, chan: nil, backoff: @min_backoff}}
  end


  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = state)
      when is_binary(payload) and is_map(meta) and not is_nil(chan) do
    cid = Map.get(meta, :correlation_id)
    Logger.info("[Infracciones.Server] basic_deliver ▶️ cid=#{inspect(cid)} payload=#{payload}")

    case Jason.decode(payload) do
      {:ok, %{"id_compra" => id_compra}} ->
        Logger.info("[Infracciones.Server] Procesando id_compra=#{id_compra}")

        infr = Libremarket.Infracciones.detectar_infraccion(id_compra)

        # Guarda/actualiza el mapa: id_compra => true/false
        new_state =
          update_in(state, [:infracciones], fn m ->
            m = m || %{}
            Map.put(m, id_compra, infr)
          end)

        resp = Jason.encode!(%{id_compra: id_compra, infraccion: infr})
        Logger.info("[Infracciones.Server] Publicando respuesta en '#{@resp_q}' cid=#{inspect(cid)} infraccion=#{inspect(infr)}")

        Basic.publish(
          chan, "", @resp_q, resp,
          correlation_id: cid,
          content_type: "application/json"
        )

        {:noreply, new_state}

      {:ok, other} ->
        Logger.warning("[Infracciones.Server] Payload con forma inesperada en '#{@req_q}': #{inspect(other)}")
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("[Infracciones.Server] JSON inválido en '#{@req_q}': #{inspect(reason)}")
        {:noreply, state}
    end
  end


  # Si aún no hay canal, descartamos
  def handle_info({:basic_deliver, _p, meta}, state) do
    Logger.info("[Infracciones.Server] Mensaje recibido pero sin canal (descartado). meta=#{inspect(meta)}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, info}, state) do
    Logger.info("[Infracciones.Server] basic_consume_ok #{inspect(info)}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, info}, state) do
    Logger.warning("[Infracciones.Server] basic_cancel #{inspect(info)} → stop :normal")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, info}, state) do
    Logger.info("[Infracciones.Server] basic_cancel_ok #{inspect(info)}")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("[Infracciones.Server] Ignorado: #{inspect(msg)}")
    {:noreply, state}
  end

  # ===== Helpers AMQP =====

  defp connect_amqp() do
    with {:ok, url} <- fetch_amqp_url() do
      insecure? = System.get_env("INSECURE_AMQPS") in ["1", "true", "TRUE"]
      if insecure? and String.starts_with?(url, "amqps://") do
        Logger.info("[Infracciones.Server] INSECURE_AMQPS activo → SSL sin verificación (solo dev)")
      end

      opts =
        [connection_timeout: 15_000, requested_heartbeat: 30]
        |> maybe_insecure_ssl(url, insecure?)

      Logger.info("[Infracciones.Server] Abriendo conexión a #{redact(url)} opts=#{inspect(opts)}")

      case Connection.open(url, opts) do
        {:ok, conn} ->
          Logger.info("[Infracciones.Server] Connection.open ✔️, abriendo canal…")
          case Channel.open(conn) do
            {:ok, chan} ->
              Logger.info("[Infracciones.Server] Channel.open ✔️")
              {:ok, conn, chan}

            {:error, reason} ->
              Logger.warning("[Infracciones.Server] Channel.open ✖️ reason=#{inspect(reason)} → cerrando conexión")
              safe_close(conn)
              {:error, reason}
          end

        {:error, reason} ->
          Logger.warning("[Infracciones.Server] Connection.open ✖️ reason=#{inspect(reason)}")
          {:error, reason}
      end
    end
  end

  defp maybe_insecure_ssl(opts, url, true) do
    if String.starts_with?(url, "amqps://") do
      Keyword.merge(opts, ssl_options: [verify: :verify_none, server_name_indication: :disable])
    else
      opts
    end
  end
  defp maybe_insecure_ssl(opts, _url, _), do: opts

  defp fetch_amqp_url() do
    case System.get_env("AMQP_URL") |> to_string() |> String.trim() do
      "" ->
        Logger.warning("[Infracciones.Server] AMQP_URL faltante")
        {:error, :missing_amqp_url}

      url ->
        {:ok, url}
    end
  end

  defp safe_close(%Channel{} = ch) do
    Logger.info("[Infracciones.Server] Cerrando canal…")
    Channel.close(ch)
  end

  defp safe_close(%Connection{} = c) do
    Logger.info("[Infracciones.Server] Cerrando conexión…")
    Connection.close(c)
  end

  defp safe_close(_), do: :ok

  # Redacta credenciales en la URL para logs
  defp redact(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{userinfo: nil} -> url
      %URI{} = uri -> %{uri | userinfo: "***:***"} |> URI.to_string()
    end
  end
end
