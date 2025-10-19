defmodule Libremarket.Pagos do
  def autorizarPago(_id_compra) do
    # ~70% autoriza
    pago_autorizado = :rand.uniform(100) < 70
    IO.puts("Pago autorizado: #{pago_autorizado}")
    pago_autorizado
  end
end

defmodule Libremarket.Pagos.Server do
  @moduledoc "Worker AMQP de Pagos: consume pagos.req y publica compras.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}
  @req_q  "pagos.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  # ========= API =========
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  # Opcionales para pruebas manuales
  def autorizarPago(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:autorizarPago, id_compra})

  def publicarResultado(pid \\ @global_name, id_compra, opts \\ []),
    do: GenServer.call(pid, {:publicarResultado, id_compra, opts})

  # ========= Callbacks =========
  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    state = %{conn: nil, chan: nil, backoff: @min_backoff, resultados: %{}}
    send(self(), :connect)
    {:ok, state}
  end

  @impl true
  def handle_info(:connect, %{backoff: b} = s) do
    case connect_amqp() do
      {:ok, conn, chan} ->
        Logger.info("Pagos.Server conectado a AMQP")
        {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
        {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
        {:ok, _} = Basic.consume(chan, @req_q, nil, no_ack: true)
        Process.monitor(conn.pid)
        {:noreply, %{s | conn: conn, chan: chan, backoff: @min_backoff}}

      {:error, reason} ->
        Logger.warning("Pagos AMQP no conectado (#{inspect(reason)}). Reintento en #{b} ms")
        Process.send_after(self(), :connect, b)
        {:noreply, %{s | conn: nil, chan: nil, backoff: min(b * 2, @max_backoff)}}
    end
  end

  @impl true
  def handle_info({:DOWN, _mref, :process, _pid, reason}, s) do
    Logger.warning("Pagos AMQP DOWN: #{inspect(reason)}. Reintentando…")
    safe_close(s.chan); safe_close(s.conn)
    Process.send_after(self(), :connect, @min_backoff)
    {:noreply, %{s | conn: nil, chan: nil, backoff: @min_backoff}}
  end

  # ---- negocio AMQP: recibir pagos.req -> procesar -> responder compras.resp
  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = s)
      when is_binary(payload) and is_map(meta) and not is_nil(chan) do
    cid = Map.get(meta, :correlation_id)

    case Jason.decode(payload) do
      {:ok, %{"id_compra" => id}} ->
        {ok?, s2} = detect_and_store(s, id)
        :ok = publish_result(chan, id, ok?, cid, @resp_q)
        {:noreply, s2}

      other ->
        Logger.warn("Payload inválido en #{@req_q}: #{inspect(other)}")
        {:noreply, s}
    end
  end

  def handle_info({:basic_deliver, _p, _m}, s), do: {:noreply, s}
  @impl true
  def handle_info({:basic_consume_ok, _}, s), do: {:noreply, s}

  @impl true
  def handle_info({:basic_cancel, _}, s), do: {:stop, :normal, s}

  @impl true
  def handle_info({:basic_cancel_ok, _}, s), do: {:noreply, s}

  @impl true
  def handle_info(_msg, s), do: {:noreply, s}


  # ---- handle_call para pruebas manuales / utilitarios
  @impl true
  def handle_call({:autorizarPago, id}, _from, s) do
    {ok?, s2} = detect_and_store(s, id)
    {:reply, ok?, s2}
  end

  @impl true
  def handle_call({:publicarResultado, id, opts}, _from, %{chan: nil} = s),
    do: {:reply, {:error, :amqp_not_ready}, s}

  @impl true
  def handle_call({:publicarResultado, id, opts}, _from, %{chan: ch} = s) do
    cid   = Keyword.get(opts, :correlation_id)
    queue = Keyword.get(opts, :queue, @resp_q)
    {ok?, s2} = detect_and_store(s, id)
    :ok = publish_result(ch, id, ok?, cid, queue)
    {:reply, {:ok, %{id_compra: id, autorizacionPago: ok?}}, s2}
  end

  # ---- helpers
  defp detect_and_store(s, id) do
    case Map.fetch(s.resultados, id) do
      {:ok, v} -> {v, s}
      :error ->
        v = Libremarket.Pagos.autorizarPago(id)
        {v, %{s | resultados: Map.put(s.resultados, id, v)}}
    end
  end

  defp publish_result(chan, id, ok?, cid \\ nil, queue \\ @resp_q) do
    payload = Jason.encode!(%{id_compra: id, autorizacionPago: ok?})
    Basic.publish(chan, "", queue, payload,
      correlation_id: cid, content_type: "application/json"
    )
  end

  # AMQP con SSL opcionalmente inseguro (como Infracciones)
  defp connect_amqp() do
    with {:ok, url} <- fetch_amqp_url() do
      insecure? = System.get_env("INSECURE_AMQPS") in ["1","true","TRUE"]
      opts = [connection_timeout: 15_000, requested_heartbeat: 30]
             |> maybe_insecure_ssl(url, insecure?)
      case Connection.open(url, opts) do
        {:ok, conn} ->
          case Channel.open(conn) do
            {:ok, chan} -> {:ok, conn, chan}
            {:error, r} -> safe_close(conn); {:error, r}
          end
        {:error, r} -> {:error, r}
      end
    end
  end

  defp maybe_insecure_ssl(opts, url, true) do
    if String.starts_with?(url, "amqps://"),
      do: Keyword.merge(opts, ssl_options: [verify: :verify_none, server_name_indication: :disable]),
      else: opts
  end
  defp maybe_insecure_ssl(opts, _url, _), do: opts

  defp fetch_amqp_url() do
    case System.get_env("AMQP_URL") |> to_string() |> String.trim() do
      "" -> {:error, :missing_amqp_url}
      url -> {:ok, url}
    end
  end

  defp safe_close(%Channel{} = ch),    do: Channel.close(ch)
  defp safe_close(%Connection{} = co), do: Connection.close(co)
  defp safe_close(_),                  do: :ok
end
