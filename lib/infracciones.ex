defmodule Libremarket.Infracciones do
  require Logger

  @doc """
  Genera una infracción aleatoria (~30%) y replica el resultado
  a los nodos de infracciones (réplicas) mediante RPC.

  Retorna el resultado (true/false).
  """
  def detectar_infraccion(id_compra) do
    infraccion? = :rand.uniform(100) <= 30
    Logger.info("[Infracciones] id_compra=#{id_compra} → infraccion?=#{infraccion?}")

    # Enviamos el resultado a las réplicas
    # replicate_to_replicas(id_compra, infraccion?)

    infraccion?
  end

  # --- Helpers internos ---

  # # Filtra nodos remotos que contienen "infracciones-" en el nombre
  # defp replica_nodes do
  #   Node.list()
  #   |> Enum.filter(fn n ->
  #     name = Atom.to_string(n)
  #     String.contains?(name, "infracciones-") and not String.contains?(name, "primario")
  #   end)
  # end

  # # Envia el resultado a cada réplica
  # defp replicate_to_replicas(id_compra, infraccion?) do
  #   replicas = replica_nodes()

  #   if replicas == [] do
  #     Logger.warning("[Infracciones] ⚠️ No hay réplicas conectadas para sincronizar")
  #   else
  #     Enum.each(replicas, fn nodo ->
  #       Logger.info("[Infracciones] RPC → #{nodo} id_compra=#{id_compra} infraccion=#{infraccion?}")
  #       try do
  #         :rpc.cast(nodo, Libremarket.Infracciones.Server, :replicar_resultado, [id_compra, infraccion?])
  #       catch
  #         kind, reason ->
  #           Logger.error("[Infracciones] Error replicando en #{nodo}: #{inspect({kind, reason})}")
  #       end
  #     end)
  #   end
  # end

  defp replicate_to_replicas(id_compra, infraccion?) do
    Logger.info("[Infracciones] id_compra=#{id_compra} infraccion=#{infraccion?}")
    GenServer.call({:global, :"infracciones-replica-1"}, {:replicar_resultado, id_compra, infraccion?})
    GenServer.call({:global, :"infracciones-replica-2"}, {:replicar_resultado, id_compra, infraccion?})
  end

end



defmodule Libremarket.Infracciones.Server do
  @moduledoc "Infracciones"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name nil

  #@global_name {:global, (System.get_env("NOMBRE") |> to_string() |> String.trim())}
  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  # ===== API cliente =====
  def start_link(opts \\ %{}) do
    nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Infracciones.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

  def listarInfracciones(pid \\ @global_name) do
    Logger.info("[Infracciones.Server] listarInfracciones/1 → call")
    GenServer.call(global_name(), :listarInfracciones)
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
  def handle_call({:replicar_resultado, id_compra, infraccion?}, _from, state) do
    Logger.info("[Infracciones.Server] 🔁 Resultado replicado recibido id_compra=#{id_compra} infraccion=#{infraccion?}")
    nuevo = Map.update(state, :infracciones, %{id_compra => infraccion?}, fn map ->
      Map.put(map, id_compra, infraccion?)
    end)
    {:reply, :ok, nuevo}
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

        infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
        Logger.info("[Infracciones] id_compra=#{id_compra} infraccion=#{infraccion}")
        GenServer.call({:global, :"infracciones-replica-1"}, {:replicar_resultado, id_compra, infraccion})
        GenServer.call({:global, :"infracciones-replica-2"}, {:replicar_resultado, id_compra, infraccion})

        # Guarda/actualiza el mapa: id_compra => true/false
        new_state =
          update_in(state, [:infracciones], fn m ->
            m = m || %{}
            Map.put(m, id_compra, infraccion)
          end)

        resp = Jason.encode!(%{id_compra: id_compra, infraccion: infraccion})
        Logger.info("[Infracciones.Server] Publicando respuesta en '#{@resp_q}' cid=#{inspect(cid)} infraccion=#{inspect(infraccion)}")

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
