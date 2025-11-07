defmodule Libremarket.Envios do
  @moduledoc "Lógica de envíos (pura / sin IO)."
  require Logger

  def calcularEnvio(:retira) do
    Logger.info("Forma de entrega seleccionada: :retira")
    0
  end

  def calcularEnvio(:correo) do
    Logger.info("Forma de entrega seleccionada: :correo")
    costo = :rand.uniform(10_000)
    Logger.debug("Costo aleatorio generado para envío: #{costo}")
    costo
  end

  # opcional: “persistencia” soft del agendado para pruebas
  def agendar_envio(id_compra, costo) do
    id_envio = :erlang.unique_integer([:positive])
    Logger.info("Agendando envío #{id_envio} para compra #{id_compra} con costo #{costo}")
    replicate_to_replicas(id_compra, costo)
    {:ok, %{id_envio: id_envio, id_compra: id_compra, costo: costo, estado: :pendiente}}
  end

  # --- Helpers internos ---
 defp replica_names do
    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "envios-replica-")
    end)
  end

  # Envia el resultado a cada réplica mediante GenServer.call
  defp replicate_to_replicas(id_compra, costo) do
    replicas = replica_names()

    if replicas == [] do
      Logger.warning("[Envios] ⚠️ No hay réplicas registradas globalmente para sincronizar")
    else
      Enum.each(replicas, fn replica_name ->
        Logger.info("[Envios] GenServer.call → #{replica_name} id_compra=#{id_compra} costo=#{costo}")

        try do
          GenServer.call({:global, replica_name}, {:replicar_resultado, id_compra, costo})
        catch
          kind, reason ->
            Logger.error("[Envios] Error replicando en #{replica_name}: #{inspect({kind, reason})}")
        end
      end)
    end
  end

end


defmodule Libremarket.Envios.Server do
  @moduledoc "Worker AMQP de Envíos: consume envios.req y publica compras.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name nil
  @req_q  "envios.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  # ========= API =========
  def start_link(opts \\ %{}) do
    # GenServer.start_link(__MODULE__, opts, name: @global_name)
    nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Envios.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

  def listarEnvios(pid \\ @global_name) do
    Logger.info("[Envios.Server] listarEnvios/1 → call")
    GenServer.call(global_name(), :listarEnvios)
  end

  # ========= Callbacks =========

  @impl true
  def handle_call(:listarEnvios, _from, state) do
    Logger.info("[Envios.Server] handle_call(:listarEnvios) → reply name=#{inspect(global_name())} node=#{inspect(node())}")
    {:reply, state.envios, state}
  end

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    base_state = %{envios: %{}}

    if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
      state_primario = Map.merge(base_state, %{conn: nil, chan: nil, backoff: @min_backoff})
      Logger.info("Envios.Server inicializado. Intentando conexión AMQP…")
      send(self(), :connect)
      {:ok, state_primario}
    else
      {:ok, base_state}
    end
  end

  @impl true
  def handle_info(:connect, %{backoff: backoff} = s) do
    case connect_amqp() do
      {:ok, conn, chan} ->
        Logger.info("Envios.Server conectado correctamente a AMQP")
        {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
        {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
        {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: true)
        Process.monitor(conn.pid)
        Logger.debug("Suscrito a cola #{@req_q} y listo para recibir mensajes.")
        {:noreply, %{s | conn: conn, chan: chan, backoff: @min_backoff}}

      {:error, reason} ->
        Logger.warning("No se pudo conectar a AMQP (#{inspect(reason)}). Reintento en #{backoff} ms")
        Process.send_after(self(), :connect, backoff)
        {:noreply, %{s | conn: nil, chan: nil, backoff: min(backoff * 2, @max_backoff)}}
    end
  end

  @impl true
  def handle_info({:DOWN, _mref, :process, _pid, reason}, s) do
    Logger.error("Conexión AMQP caída: #{inspect(reason)}. Intentando reconexión…")
    safe_close(s.chan)
    safe_close(s.conn)
    Process.send_after(self(), :connect, @min_backoff)
    {:noreply, %{s | conn: nil, chan: nil, backoff: @min_backoff}}
  end

  # ====== Mensajes de negocio (AMQP) ======
  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = s)
      when is_binary(payload) and not is_nil(chan) do
    cid = Map.get(meta, :correlation_id)
    Logger.debug("Mensaje recibido (CID: #{inspect(cid)}): #{payload}")

    case Jason.decode(payload) do
      # 1) calcular envío
      {:ok, %{"accion" => "calcular", "id_compra" => id, "forma_entrega" => forma}} ->
        Logger.info("Solicitud de cálculo de envío para compra #{id} (forma: #{forma})")

        forma_atom =
          case forma do
            "correo" -> :correo
            "retira" -> :retira
            _ -> :retira
          end

        costo = Libremarket.Envios.calcularEnvio(forma_atom)
        Logger.debug("Costo de envío calculado: #{costo}")

        resp = Jason.encode!(%{id_compra: id, precio_envio: costo})
        Basic.publish(chan, "", @resp_q, resp,
          correlation_id: cid, content_type: "application/json"
        )
        Logger.info("Respuesta publicada en #{@resp_q} para compra #{id}")
        {:noreply, s}

      # 2) agendar envío
      {:ok, %{"accion" => "agendar", "id_compra" => id, "costo" => costo}} ->
        Logger.info("Solicitud de agendado de envío para compra #{id}")
        {:ok, datos} = Libremarket.Envios.agendar_envio(id, costo)
        s2 = put_in(s.envios[datos.id_envio], datos)

        resp = Jason.encode!(%{
          id_compra: id,
          envio_agendado: true,
          id_envio: datos.id_envio
        })

        Basic.publish(chan, "", @resp_q, resp,
          correlation_id: cid, content_type: "application/json"
        )

        Logger.info("Envío #{datos.id_envio} agendado correctamente (compra #{id})")
        {:noreply, s2}

      other ->
        Logger.warning("Payload inválido recibido en #{@req_q}: #{inspect(other)}")
        {:noreply, s}
    end
  end

  def handle_info({:basic_deliver, _p, _m}, s) do
    Logger.debug("Mensaje recibido pero sin canal activo; descartado.")
    {:noreply, s}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, s) do
    Logger.debug("Consumo AMQP confirmado.")
    {:noreply, s}
  end

  @impl true
  def handle_info({:basic_cancel, _}, s) do
    Logger.warning("Consumo AMQP cancelado.")
    {:stop, :normal, s}
  end

  @impl true
  def handle_info({:basic_cancel_ok, _}, s) do
    Logger.debug("Confirmación de cancelación de consumo AMQP recibida.")
    {:noreply, s}
  end

  @impl true
  def handle_info(msg, s) do
    Logger.debug("Envios.Server ignoró mensaje no reconocido: #{inspect(msg)}")
    {:noreply, s}
  end

  # ===== Helpers AMQP =====
  defp connect_amqp() do
    with {:ok, url} <- fetch_amqp_url() do
      insecure? = System.get_env("INSECURE_AMQPS") in ~w[1 true TRUE]
      opts = [connection_timeout: 15_000, requested_heartbeat: 30]
      opts = maybe_insecure_ssl(opts, url, insecure?)

      case Connection.open(url, opts) do
        {:ok, conn} ->
          Logger.debug("Conexión AMQP abierta exitosamente.")
          case Channel.open(conn) do
            {:ok, chan} ->
              Logger.debug("Canal AMQP abierto correctamente.")
              {:ok, conn, chan}

            {:error, r} ->
              Logger.error("Error al abrir canal AMQP: #{inspect(r)}")
              safe_close(conn)
              {:error, r}
          end

        {:error, r} ->
          Logger.error("Error al abrir conexión AMQP: #{inspect(r)}")
          {:error, r}
      end
    end
  end

  @impl true
  def handle_call({:replicar_resultado, id_compra, costo}, _from, state) do
    Logger.info("[Envios.Server] 🔁 Resultado replicado recibido id_compra=#{id_compra} costo=#{costo}")
    nuevo = Map.update(state, :envios, %{id_compra => costo}, fn map ->
      Map.put(map, id_compra, costo)
    end)
    {:reply, :ok, nuevo}
  end

  defp maybe_insecure_ssl(opts, url, true) do
    if String.starts_with?(url, "amqps://") do
      Logger.warning("Modo INSECURE_AMQPS activado: SSL sin verificación")
      Keyword.merge(opts, ssl_options: [verify: :verify_none, server_name_indication: :disable])
    else
      opts
    end
  end
  defp maybe_insecure_ssl(opts, _url, _), do: opts

  defp fetch_amqp_url() do
    case System.get_env("AMQP_URL") |> to_string() |> String.trim() do
      "" ->
        Logger.error("Variable de entorno AMQP_URL no encontrada.")
        {:error, :missing_amqp_url}

      url ->
        Logger.debug("Usando AMQP_URL: #{url}")
        {:ok, url}
    end
  end

  defp safe_close(%Channel{} = ch) do
    Logger.debug("Cerrando canal AMQP.")
    Channel.close(ch)
  end

  defp safe_close(%Connection{} = con) do
    Logger.debug("Cerrando conexión AMQP.")
    Connection.close(con)
  end

  defp safe_close(_), do: :ok
end
