defmodule Libremarket.Infracciones.Leader do
  use GenServer

  @base_path "/libremarket/infracciones"
  @leader_path "/libremarket/infracciones/leader"

  ## API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Pregunta dinámica: ¿este nodo es líder *ahora*?
  def leader? do
    GenServer.call(__MODULE__, :leader?)
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    {:ok, zk} = Libremarket.ZK.connect()

    # Aseguramos jerarquía
    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    # Creamos znode efímero secuencial
    {:ok, my_znode} =
      :erlzk.create(
        zk,
        @leader_path <> "/nodo-",
        :ephemeral_sequential
      )

    IO.puts("🟣 Infracciones.Leader iniciado como #{List.to_string(my_znode)}")

    # OJO: NO guardamos leader? en el estado, solo zk + my_znode
    {:ok, %{zk: zk, my_znode: my_znode}}
  end

  defp wait_for_zk(zk, path, retries \\ 5)
  defp wait_for_zk(_zk, path, 0), do: raise("ZooKeeper no respondió creando #{path}")

  defp wait_for_zk(zk, path, retries) do
    case Libremarket.ZK.ensure_path(zk, path) do
      :ok ->
        :ok

      {:error, _} ->
        IO.puts("⚠️ reintentando crear #{path}…")
        :timer.sleep(1_000)
        wait_for_zk(zk, path, retries - 1)
    end
  end

  @impl true
  def handle_call(:leader?, _from, %{zk: zk, my_znode: my_znode} = state) do
    {:reply, compute_leader?(zk, my_znode), state}
  end

  # === lógica de elección ===
  defp compute_leader?(zk, my_znode) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children
      |> Enum.map(&List.to_string/1)
      |> Enum.sort()

    my_name = Path.basename(List.to_string(my_znode))
    [first | _] = sorted

    my_name == first
  end
end # Fin Infracciones.Leader












defmodule Libremarket.Infracciones do
  require Logger

  @doc """
  Genera una infracción aleatoria (~30%).

  Si el nodo actual es líder (según ZooKeeper), replica el resultado
  a los otros nodos de infracciones mediante RPC GenServer.call/3.

  Retorna el resultado (true/false).
  """
  def detectar_infraccion(id_compra) do
    infraccion? = :rand.uniform(100) <= 30
    Logger.info("[Infracciones] id_compra=#{id_compra} → infraccion?=#{infraccion?}")

    # Solo el líder replica; las réplicas nunca deberían entrar acá
    case safe_leader_check() do
      {:ok, true} ->
        replicate_to_replicas(id_compra, infraccion?)

      {:ok, false} ->
        Logger.info("[Infracciones] Soy réplica, no replico el resultado")

      {:error, reason} ->
        Logger.warning("[Infracciones] No pude consultar leader? (#{inspect(reason)}), no replico")
    end

    infraccion?
  end

  # === Helpers internos ===

  # Pregunta al proceso Leader en forma segura
  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Infracciones.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # Obtiene los nombres de otras réplicas registradas globalmente.
  # Asume que cada nodo registra su GenServer con NOMBRE = "infracciones-1|2|3"
  defp replica_names do
    {:global, my_name_atom} = Libremarket.Infracciones.Server.global_name()

    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "infracciones-")
    end)
    |> Enum.reject(&(&1 == my_name_atom))
  end

  # Envía el resultado a cada réplica mediante GenServer.call
  defp replicate_to_replicas(id_compra, infraccion?) do
    replicas = replica_names()

    if replicas == [] do
      Logger.warning("[Infracciones] ⚠️ No hay réplicas registradas globalmente para sincronizar")
    else
      Enum.each(replicas, fn replica_name ->
        Logger.info(
          "[Infracciones] GenServer.call → #{replica_name} id_compra=#{id_compra} infraccion=#{infraccion?}"
        )

        try do
          GenServer.call({:global, replica_name}, {:replicar_resultado, id_compra, infraccion?})
        catch
          kind, reason ->
            Logger.error(
              "[Infracciones] Error replicando en #{replica_name}: #{inspect({kind, reason})}"
            )
        end
      end)
    end
  end
end











defmodule Libremarket.Infracciones.Server do
  @moduledoc "Infracciones"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name nil

  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  @leader_check_interval 2_000  # ms

  # ===== API cliente =====
  def start_link(opts \\ %{}) do
    nombre =
      System.get_env("NOMBRE")
      |> to_string()
      |> String.trim()
      |> String.to_atom()

    global_name = {:global, nombre}

    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info(
      "[Infracciones.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}"
    )

    # Nos aseguramos que el Leader esté levantado
    wait_for_leader()

    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

  def listarInfracciones(pid \\ @global_name) do
    Logger.info("[Infracciones.Server] listarInfracciones/1 → call")
    GenServer.call(global_name(), :listarInfracciones)
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Infracciones.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Infracciones.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Infracciones.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # ===== Callbacks =====
  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    base_state = %{
      infracciones: %{},
      conn: nil,
      chan: nil,
      backoff: @min_backoff,
      role: :unknown
    }

    # Arrancamos el loop de verificación de rol
    send(self(), :ensure_role)

    {:ok, base_state}
  end

  @impl true
  def handle_call(:listarInfracciones, _from, state) do
    Logger.info(
      "[Infracciones.Server] handle_call(:listarInfracciones) → reply node=#{inspect(node())}"
    )

    {:reply, state.infracciones, state}
  end

  @impl true
  def handle_call({:replicar_resultado, id_compra, infraccion?}, _from, state) do
    Logger.info(
      "[Infracciones.Server] 🔁 Resultado replicado recibido id_compra=#{id_compra} infraccion=#{infraccion?}"
    )

    nuevo =
      Map.update(state, :infracciones, %{id_compra => infraccion?}, fn map ->
        Map.put(map, id_compra, infraccion?)
      end)

    {:reply, :ok, nuevo}
  end

  # ===== Loop de rol (líder / réplica) =====

  @impl true
  def handle_info(:ensure_role, state) do
    new_role =
      case safe_leader_check() do
        {:ok, true} -> :leader
        {:ok, false} -> :replica
        {:error, _} -> state.role  # no sabemos, nos quedamos como estábamos
      end

    state2 =
      case {state.role, new_role} do
        {r, r} ->
          # no cambia el rol
          state

        {_, :leader} ->
          Logger.info("[Infracciones.Server] 🔼 Cambio de rol → ahora soy LÍDER")
          # si no hay conexión AMQP, iniciamos
          send(self(), :connect)
          %{state | role: :leader}

        {:leader, :replica} ->
          Logger.info("[Infracciones.Server] 🔽 Cambio de rol → ahora soy RÉPLICA (cierro AMQP)")
          safe_close(state.chan)
          safe_close(state.conn)
          %{state | role: :replica, conn: nil, chan: nil, backoff: @min_backoff}

        {:unknown, :replica} ->
          Logger.info("[Infracciones.Server] Rol inicial detectado: RÉPLICA")
          %{state | role: :replica}

        {:unknown, :leader} ->
          Logger.info("[Infracciones.Server] Rol inicial detectado: LÍDER")
          send(self(), :connect)
          %{state | role: :leader}
      end

    # volvemos a chequear dentro de un rato
    Process.send_after(self(), :ensure_role, @leader_check_interval)
    {:noreply, state2}
  end

  # ===== AMQP =====

  @impl true
  def handle_info(:connect, %{backoff: backoff, role: :leader} = state) do
    Logger.info(
      "[Infracciones.Server] handle_info(:connect) → intento de conexión AMQP (backoff=#{backoff}ms, role=leader)"
    )

    case connect_amqp() do
      {:ok, conn, chan} ->
        Logger.info("[Infracciones.Server] Conectado a AMQP ✔️")
        {:ok, _} = Queue.declare(chan, @req_q, durable: false)
        {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
        Logger.info("[Infracciones.Server] Declaradas colas req='#{@req_q}' resp='#{@resp_q}'")
        {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: true)
        Logger.info("[Infracciones.Server] Consumiendo de '#{@req_q}' (no_ack=true)")
        Process.monitor(conn.pid)
        {:noreply, %{state | conn: conn, chan: chan, backoff: @min_backoff}}

      {:error, reason} ->
        Logger.warning(
          "[Infracciones.Server] AMQP no conectado (#{inspect(reason)}). Reintento en #{backoff} ms"
        )

        Process.send_after(self(), :connect, backoff)
        {:noreply, %{state | conn: nil, chan: nil, backoff: min(backoff * 2, @max_backoff)}}
    end
  end

  # Si recibimos :connect pero ya no somos líder → lo ignoramos
  @impl true
  def handle_info(:connect, state) do
    Logger.info(
      "[Infracciones.Server] handle_info(:connect) recibido pero role=#{inspect(state.role)} → ignorado"
    )

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _mref, :process, pid, reason}, %{role: :leader} = state) do
    Logger.warning(
      "[Infracciones.Server] AMQP DOWN desde pid=#{inspect(pid)} reason=#{inspect(reason)}. Reintentando…"
    )

    safe_close(state.chan)
    safe_close(state.conn)
    Process.send_after(self(), :connect, @min_backoff)
    {:noreply, %{state | conn: nil, chan: nil, backoff: @min_backoff}}
  end

  # Si se cae AMQP pero ya somos réplica, solo limpiamos recursos
  @impl true
  def handle_info({:DOWN, _mref, :process, pid, reason}, state) do
    Logger.warning(
      "[Infracciones.Server] AMQP DOWN (role=#{inspect(state.role)}) pid=#{inspect(pid)} reason=#{inspect(reason)}."
    )

    safe_close(state.chan)
    safe_close(state.conn)
    {:noreply, %{state | conn: nil, chan: nil}}
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
