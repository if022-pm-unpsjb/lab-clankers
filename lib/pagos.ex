defmodule Libremarket.Pagos do
  require Logger

  def autorizarPago(id_compra) do
    # ~70% autoriza
    pago_autorizado? = :rand.uniform(100) < 70
    IO.puts("Pago autorizado: #{pago_autorizado?}")
    replicate_to_replicas(id_compra, pago_autorizado?)
    pago_autorizado?
  end

  # --- Helpers internos ---
 defp replica_names do
    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "pagos-replica-")
    end)
  end

  # Envia el resultado a cada réplica mediante GenServer.call
  defp replicate_to_replicas(id_compra, pago_autorizado?) do
    replicas = replica_names()

    if replicas == [] do
      Logger.warning("[Pagos] ⚠️ No hay réplicas registradas globalmente para sincronizar")
    else
      Enum.each(replicas, fn replica_name ->
        Logger.info("[Pagos] GenServer.call → #{replica_name} id_compra=#{id_compra} pago_autorizado=#{pago_autorizado?}")

        try do
          GenServer.call({:global, replica_name}, {:replicar_resultado, id_compra, pago_autorizado?})
        catch
          kind, reason ->
            Logger.error("[Pagos] Error replicando en #{replica_name}: #{inspect({kind, reason})}")
        end
      end)
    end
  end

end

defmodule Libremarket.Pagos.Server do
  @moduledoc "Worker AMQP de Pagos: consume pagos.req y publica compras.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name nil
  @req_q  "pagos.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  # ========= API =========
  def start_link(opts \\ %{}) do

    nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Pagos.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})


  # Opcionales para pruebas manuales
  def publicarResultado(pid \\ @global_name, id_compra, opts \\ []),
    do: GenServer.call(global_name(), {:publicarResultado, id_compra, opts})

  def listarAutorizaciones(pid \\ @global_name) do
    Logger.info("[pagos.Server] listarAutorizaciones/1 → call")
    GenServer.call(global_name(), :listarAutorizaciones)
  end




  # ========= Callbacks =========

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    base_state = %{autorizacion_pagos: %{}}

    if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
      Logger.info("[Pagos.Server] init/1 → preparando conexión AMQP diferida")
      state_primario = Map.merge(base_state, %{conn: nil, chan: nil, backoff: @min_backoff})
      send(self(), :connect)
      {:ok, state_primario}
    else
      {:ok, base_state}
    end
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

  @impl true
  def handle_call(:listarAutorizaciones, _from, state) do
    Logger.info("[Pagos.Server] handle_call(:listarAutorizaciones) → reply name=#{inspect(@global_name)} node=#{inspect(node())}")
    {:reply, state.autorizacion_pagos, state}
  end

  @impl true
  def handle_call({:replicar_resultado, id_compra, autorizacion?}, _from, state) do
    Logger.info("[Pagos.Server] 🔁 Resultado replicado recibido id_compra=#{id_compra} autorizacion=#{autorizacion?}")
    nuevo = Map.update(state, :autorizacion_pagos, %{id_compra => autorizacion?}, fn map ->
      Map.put(map, id_compra, autorizacion?)
    end)
    {:reply, :ok, nuevo}
  end

  # ---- helpers
  defp detect_and_store(s, id) do
    case Map.fetch(s.autorizacion_pagos, id) do
      {:ok, v} -> {v, s}
      :error ->
        v = Libremarket.Pagos.autorizarPago(id)
        {v, %{s | autorizacion_pagos: Map.put(s.autorizacion_pagos, id, v)}}
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
