defmodule Libremarket.Pagos.Leader do
  use GenServer

  @base_path "/libremarket/pagos"
  @leader_path "/libremarket/pagos/leader"

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

    IO.puts("🟣 Pagos.Leader iniciado como #{List.to_string(my_znode)}")

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
end # Fin Pagos.Leader












defmodule Libremarket.Pagos do
  require Logger

  def autorizarPago(id_compra) do
    # ~70% autoriza
    pago_autorizado? = :rand.uniform(100) < 70

    Logger.info("[Pagos] id_compra=#{id_compra} → pago_autorizado?=#{pago_autorizado?}")

    # Solo el líder replica; las réplicas nunca deberían entrar acá
    case safe_leader_check() do
      {:ok, true} ->
        replicate_to_replicas(id_compra, pago_autorizado?)

      {:ok, false} ->
        Logger.info("[Pagos] Soy réplica, no replico el resultado")

      {:error, reason} ->
        Logger.warning("[Pagos] No pude consultar leader? (#{inspect(reason)}), no replico")

      end

    pago_autorizado?
  end

  # === Helpers internos ===

  # Pregunta al proceso Leader en forma segura
  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Pagos.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # Obtiene los nombres de otras réplicas registradas globalmente.
  # Asume que cada nodo registra su GenServer con NOMBRE = "pagos-1|2|3"
  defp replica_names do
    {:global, my_name_atom} = Libremarket.Pagos.Server.global_name()

    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "pagos-")
    end)
    |> Enum.reject(&(&1 == my_name_atom))
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

end # Fin Libremarket.Pagos










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

  @leader_check_interval 2_000  # ms


  # ========= API =========
  def start_link(opts \\ %{}) do

    nombre = System.get_env("NOMBRE")
    |> to_string()
    |> String.trim()
    |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Pagos.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")

    wait_for_leader()


    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

  def listarAutorizaciones(pid \\ @global_name) do
    Logger.info("[pagos.Server] listarAutorizaciones/1 → call")
    GenServer.call(global_name(), :listarAutorizaciones)
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Pagos.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Pagos.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Pagos.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # Opcionales para pruebas manuales
  def publicarResultado(pid \\ @global_name, id_compra, opts \\ []),
    do: GenServer.call(global_name(), {:publicarResultado, id_compra, opts})


  defp request_state_from_leader() do
    replicas =
      :global.registered_names()
      |> Enum.filter(&(String.starts_with?(Atom.to_string(&1), "pagos-")))

    case Enum.find(replicas, fn name ->
          try do
            :rpc.call(name, Libremarket.Pagos.Leader, :leader?, [], 2000)
          catch
            _, _ -> false
          end
        end) do
      nil ->
        :no_leader

      leader_name ->
        try do
          case GenServer.call({:global, leader_name}, :get_full_state, 2000) do
            productos when is_map(productos) ->
              {:ok, productos}

            _ ->
              {:error, :timeout}
          end
        catch
          :exit, _ ->
            {:error, :timeout}
        end
    end
  end

  # ========= Callbacks =========


  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    base_state = %{
      autorizacion_pagos: %{},
      conn: nil,
      chan: nil,
      backoff: @min_backoff,
      role: :unknown
    }

    Logger.info("[Pagos.Server] llegue hasta aca bien")

    # Arrancamos el loop de verificación de rol
    send(self(), :ensure_role)

    Logger.info("[Pagos.Server] YA SALI DEL ENSURE ROLE")

    {:ok, base_state}
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

  def handle_call(:get_full_state, _from, %{autorizacion_pagos: autorizacion_pagos} = state) do
    {:reply, autorizacion_pagos, state}
  end
  # ===== Loop de rol (líder / réplica) =====

  @impl true
  def handle_info(:ensure_role, state) do
    new_role =
      case safe_leader_check() do
        {:ok, true} -> :leader
        {:ok, false} -> :replica
        {:error, _} -> state.role
      end

    state2 =
      case {state.role, new_role} do

        # ───────────────────────────────────────────────
        # 0) No cambia el rol
        # ───────────────────────────────────────────────
        {r, r} ->
          state


        # ───────────────────────────────────────────────
        # 1) Cambio → ahora soy LÍDER
        # ───────────────────────────────────────────────
        {_, :leader} ->
          Logger.info("[Pagos.Server] 🔼 Cambio de rol → ahora soy LÍDER")

          cond do
            # CASO 2 — Ya tengo estado replicado
            map_size(state.autorizacion_pagos) > 0 ->
              Logger.info("[Pagos] Conservo estado local (réplica ya sincronizada)")
              send(self(), :connect)
              %{state | role: :leader}

            # CASO 1 — Primer líder del sistema
            true ->
              Logger.info("[Pagos] 🆕 Primer líder → inicializando estado vacío")

              initial = %{}

              send(self(), :connect)
              %{state | role: :leader, autorizacion_pagos: initial}
          end


        # ───────────────────────────────────────────────
        # 2) Cambio líder → réplica
        # ───────────────────────────────────────────────
        {:leader, :replica} ->
          Logger.info("[Pagos.Server] 🔽 Cambio de rol → ahora soy RÉPLICA (cierro AMQP)")
          safe_close(state.chan)
          safe_close(state.conn)

          %{state |
            role: :replica,
            conn: nil,
            chan: nil,
            backoff: @min_backoff
          }


        # ───────────────────────────────────────────────
        # 3) Inicial → réplica → sincronizar estado del líder
        # ───────────────────────────────────────────────
        {:unknown, :replica} ->
          case request_state_from_leader() do
            {:ok, pagos} ->
              Logger.info("[Pagos] Estado inicial recibido del líder")
              %{state | role: :replica, autorizacion_pagos: pagos}

            {:error, :timeout} ->
              Logger.warning("[Pagos] líder no responde todavía → reintentando…")
              %{state | role: :replica}

            :no_leader ->
              Logger.warning("[Pagos] aún no hay líder → reintentando…")
              %{state | role: :replica}
          end


        # ───────────────────────────────────────────────
        # 4) Inicial → líder (caso poco común pero válido)
        # ───────────────────────────────────────────────
        {:unknown, :leader} ->
          Logger.info("[Pagos.Server] Rol inicial detectado: LÍDER")
          send(self(), :connect)
          %{state | role: :leader}
      end

    Process.send_after(self(), :ensure_role, @leader_check_interval)
    {:noreply, state2}
  end

 # SOLO LÍDER CONECTA
  @impl true
  def handle_info(:connect, %{backoff: b, role: :leader} = s) do
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


    # Si recibimos :connect pero ya no somos líder → lo ignoramos
  @impl true
  def handle_info(:connect, state) do
    Logger.info(
      "[Pagos.Server] handle_info(:connect) recibido pero role=#{inspect(state.role)} → ignorado"
    )

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _mref, :process, pid, reason}, %{role: :leader} = state) do
    Logger.warning(
      "[Pagos.Server] AMQP DOWN desde pid=#{inspect(pid)} reason=#{inspect(reason)}. Reintentando…"
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
      "[Pagos.Server] AMQP DOWN (role=#{inspect(state.role)}) pid=#{inspect(pid)} reason=#{inspect(reason)}."
    )

    safe_close(state.chan)
    safe_close(state.conn)
    {:noreply, %{state | conn: nil, chan: nil}}
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
