  defmodule Libremarket.Compras.Leader do
    use GenServer

    @base_path "/libremarket/compras"
    @leader_path "/libremarket/compras/leader"

    ## API

    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    # Pregunta dinámica: ¿este nodo es líder *ahora*?
    def leader? do
      GenServer.call(__MODULE__, :leader?)
    end


    def current_leader_name() do
    {:ok, zk} = Libremarket.ZK.connect()

    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children
      |> Enum.map(&List.to_string/1)
      |> Enum.sort()

    [first | _] = sorted

    # znode = "nodo-0000000002"
    # Debemos buscar cuál nodo tiene ese nombre
    first
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

      IO.puts("🟣 Compras.Leader iniciado como #{List.to_string(my_znode)}")

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

      # === NUEVO: devuelve {:ok, {:global, :"compras-N"}} del líder ===

  def leader_node() do
    nodes =
      [node() | Node.list()]
      |> Enum.filter(fn n -> Atom.to_string(n) |> String.starts_with?("compras-") end)

    case Enum.find(nodes, fn n ->
          :rpc.call(n, __MODULE__, :leader?, []) == true
        end) do
      nil ->
        {:error, :no_leader}

      leader_node ->
        {:ok, leader_node}
    end
  end



  end # Fin Compras.Leader














  defmodule Libremarket.Compras do

    require Logger

    def confirmarCompra() do
      compra_confirmada = :rand.uniform(100) < 80
      IO.puts("Compra confirmada: #{compra_confirmada}")
      compra_confirmada
    end

    def inicializar_compra_state(state) do
      id_compra = :erlang.unique_integer([:positive])

      datos_compra = {
        :en_proceso,
        %{
          id_compra: id_compra,
          infraccion: nil,
          precio_envio: nil,
          autorizacionPago: nil,
          forma_entrega: nil,
          id_producto: nil,
          medio_pago: nil,
          precio_producto: nil,
          precio_total: nil
        }
      }

      new_state =
        Map.update(state, :compras, %{id_compra => datos_compra}, fn compras ->
          Map.put(compras, id_compra, datos_compra)
        end)

      {id_compra, new_state}
    end


    def detectar_infraccion_state(state, id_compra) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, compra}} ->
          compra2 = Map.put(compra, :infraccion, nil)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}

          _ = Libremarket.Compras.Server.detectar_infraccion_amqp(id_compra, 5_000)

          {{:ok, {status, compra2}}, new_state}

        :error ->
          {{:error, :not_found}, state}
      end
    end




    def autorizar_pago_state(state, id_compra) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, compra}} ->
          # limpiamos el campo y disparamos AMQP
          compra2   = Map.put(compra, :autorizacionPago, nil)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}

          _ = Libremarket.Compras.Server.autorizar_pago_amqp(id_compra, 5_000)

          {{:ok, {status, compra2}}, new_state}

        :error ->
          {{:error, :not_found}, state}
      end
    end



    def seleccionar_producto_state(state, {id_compra, id_producto}) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, compra}} ->
          compra2 =
            compra
            |> Map.put(:id_producto, id_producto)
            |> Map.put(:precio_producto, nil)
            |> Map.put(:reservado, nil)    # <- aún no sabemos si reservó

          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}

          # fire-and-forget hacia Ventas
          _ = Libremarket.Compras.Server.reservar_producto_amqp(id_compra, id_producto, 5_000)

          {{:ok, {status, compra2}}, new_state}

        :error ->
          {{:error, :not_found}, state}
      end
    end


    def seleccionar_medio_pago_state(state, {id_compra, medio_pago}) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, compra}} ->
          compra2 = Map.put(compra, :medio_pago, medio_pago)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
          {{:ok, {status, compra2}}, new_state}

        :error ->
          {{:error, :not_found}, state}
      end
    end

    def seleccionar_forma_entrega_state(state, {id_compra, forma_entrega}) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, compra}} ->
          case forma_entrega do
            :correo ->
              compra2 =
                compra
                |> Map.put(:forma_entrega, :correo)
                |> Map.put(:precio_envio, nil)    # ← lo completa la respuesta AMQP

              # 🔸 Disparo asíncrono a Envios (calcular)
              _ = Libremarket.Compras.Server.calcular_envio_amqp(id_compra, :correo, 5_000)

              new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
              {{:ok, {status, compra2}}, new_state}

            :retira ->
              # si querés que retira sea inmediato y sin AMQP (0)
              compra2 =
                compra
                |> Map.put(:forma_entrega, :retira)
                |> Map.put(:precio_envio, 0)

              new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
              {{:ok, {status, compra2}}, new_state}

            _ ->
              {{:error, :forma_entrega_invalida}, state}
          end

        :error ->
          {{:error, :not_found}, state}
      end
    end

    defp replica_names do
      {:global, my_name_atom} = Libremarket.Compras.Server.global_name()

      :global.registered_names()
      |> Enum.filter(fn name ->
        name_str = Atom.to_string(name)
        String.starts_with?(name_str, "compras-")
      end)
      |> Enum.reject(&(&1 == my_name_atom))
    end


      # Envia el resultado a cada réplica mediante GenServer.call
    def replicate_to_replicas(id_compra, nuevo_estado_compra) do
      case safe_leader_check() do
        {:ok, true} ->
          replicas = replica_names()

          if replicas == [] do
            Logger.warning("[Compras] ⚠️ No hay réplicas registradas globalmente para sincronizar")
          else
            Enum.each(replicas, fn replica_name ->
              Logger.info("[Compras] Replicando compra #{id_compra} actualizada en réplica #{replica_name}")

              try do
                GenServer.call({:global, replica_name}, {:replicar_resultado, id_compra, nuevo_estado_compra})
              catch
                kind, reason ->
                  Logger.error("[Compras] Error replicando en #{replica_name}: #{inspect({kind, reason})}")
              end
            end)
          end

        {:ok, false} ->
          Logger.info("[Compras] Soy RÉPLICA → no replico estado de compra #{id_compra}")

        {:error, reason} ->
          Logger.warning("[Compras] No pude consultar leader? (#{inspect(reason)}), no replico compra #{id_compra}")
      end
    end





    # === Helpers liderazgo (igual que Infracciones / Ventas) ===
    defp safe_leader_check() do
      try do
        {:ok, Libremarket.Compras.Leader.leader?()}
      catch
        :exit, _ -> {:error, :not_alive}
      end
    end

    defp replica_names do
      {:global, my_name_atom} = Libremarket.Compras.Server.global_name()

      :global.registered_names()
      |> Enum.filter(fn name ->
        name_str = Atom.to_string(name)
        String.starts_with?(name_str, "compras-")
      end)
      |> Enum.reject(&(&1 == my_name_atom))
    end
  end # FIN DE MODULO DE COMPRAS


  defmodule Libremarket.Compras.Server do
    use GenServer
    require Logger
    alias AMQP.{Connection, Channel, Queue, Basic}

    @global_name nil

    @req_infr_q  "infracciones.req"
    @req_pago_q  "pagos.req"
    @req_ventas_q "ventas.req"
    @req_env_q   "envios.req"
    @resp_q      "compras.resp"

    @required_for_close [:precio_producto, :precio_envio, :autorizacionPago, :id_producto, :infraccion, :forma_entrega, :medio_pago]

    @min_backoff 500
    @max_backoff 10_000

    @leader_check_interval 2_000  # ms

    # ========== API ==========
    def start_link(opts \\ %{}) do
      nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

      global_name = {:global, nombre}

      # Guardamos el valor en :persistent_term (global en el VM)
      :persistent_term.put({__MODULE__, :global_name}, global_name)

      Logger.info("[Compras.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
      wait_for_leader()
      GenServer.start_link(__MODULE__, opts, name: global_name)
    end

    def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

    def confirmarCompra(pid \\ @global_name),
      do: GenServer.call(global_name(), :confirmarCompra)

    def inicializarCompra(pid \\ @global_name),
      do: GenServer.call(global_name(), :inicializarCompra)

    def seleccionarProducto(pid \\ @global_name, datos),
      do: GenServer.call(global_name(), {:seleccionarProducto, datos})

    def seleccionarMedioPago(pid \\ @global_name, datos),
      do: GenServer.call(global_name(), {:seleccionarMedioPago, datos})

    def seleccionarFormaEntrega(pid \\ @global_name, datos),
      do: GenServer.call(global_name(), {:seleccionarFormaEntrega, datos})

    def detectarInfraccion(pid \\ @global_name, id_compra),
      do: GenServer.call(global_name(), {:detectarInfraccion, id_compra})

    def autorizarPago(pid \\ @global_name, id_compra),
      do: GenServer.call(global_name(), {:autorizarPago, id_compra})

    def listarCompras(pid \\ @global_name),
      do: GenServer.call(global_name(), :listarCompras)

    def obtenerCompra(pid \\ @global_name, id_compra),
      do: GenServer.call(global_name(), {:obtenerCompra, id_compra})


    # ============== API AMQP ===================

    defp wait_for_leader() do
      if Process.whereis(Libremarket.Compras.Leader) == nil do
        IO.puts("⏳ Esperando a que arranque Libremarket.Compras.Leader...")
        :timer.sleep(500)
        wait_for_leader()
      else
        :ok
      end
    end

    defp safe_leader_check() do
      try do
        {:ok, Libremarket.Compras.Leader.leader?()}
      catch
        :exit, _ -> {:error, :not_alive}
      end
    end

  # 👇 aridad 2 (la que vas a llamar desde Compras.detectar_infraccion_state/2)
    def detectar_infraccion_amqp(id_compra, timeout_ms \\ 5_000) do
      GenServer.cast(global_name(), {:rpc_infraccion_async, id_compra, timeout_ms})
    end

    # # 👇 aridad 3 por si alguna vez querés pasar un pid/nombre distinto
    # def detectar_infraccion_amqp(pid, id_compra, timeout_ms) do
    #   GenServer.cast(pid, {:rpc_infraccion_async, id_compra, timeout_ms})
    # end


    def autorizar_pago_amqp(id_compra, timeout_ms \\ 5_000),
      do: GenServer.cast(global_name(), {:rpc_pago_async, id_compra, timeout_ms})

    # def autorizar_pago_amqp(pid, id_compra, timeout_ms),
    #   do: GenServer.cast(pid, {:rpc_pago_async, id_compra, timeout_ms})


    def reservar_producto_amqp(id_compra, id_producto, timeout_ms \\ 5_000) do
      GenServer.cast(global_name(), {:rpc_reserva_async, id_compra, id_producto, timeout_ms})
    end

    # def reservar_producto_amqp(pid, id_compra, id_producto, timeout_ms) do
    #   GenServer.cast(pid, {:rpc_reserva_async, id_compra, id_producto, timeout_ms})
    # end

      # Publicar "calcular" (correo o retira)
    def calcular_envio_amqp(id_compra, forma_entrega, timeout_ms \\ 5_000),
      do: GenServer.cast(global_name(), {:rpc_envio_calcular_async, id_compra, forma_entrega, timeout_ms})

    # Publicar "agendar"
    def agendar_envio_amqp(id_compra, costo, timeout_ms \\ 5_000),
      do: GenServer.cast(global_name(), {:rpc_envio_agendar_async, id_compra, costo, timeout_ms})


    # ============== HANDLE CALL ===================

    @impl true
    def handle_call(:inicializarCompra, _from, state) do
      {id_compra, new_state} = Libremarket.Compras.inicializar_compra_state(state)
      Libremarket.Compras.replicate_to_replicas(id_compra, Map.get(new_state.compras, id_compra))
      {:reply, id_compra, new_state}
    end

      @impl true
      def handle_call({:detectarInfraccion, id_compra}, _from, state) do
        {reply, new_state} = Libremarket.Compras.detectar_infraccion_state(state, id_compra)
        {:reply, reply, new_state}
      end

    @impl true
    def handle_call({:autorizarPago, id_compra}, _from, state) do
      {reply, new_state} = Libremarket.Compras.autorizar_pago_state(state, id_compra)
      {:reply, reply, new_state}
    end

    @impl true
    def handle_call({:seleccionarProducto, {id_compra, id_producto}}, _from, state) do
      {reply, new_state} = Libremarket.Compras.seleccionar_producto_state(state, {id_compra, id_producto})
      new_state = replicar_estado_compra(new_state, id_compra)
      {:reply, reply, new_state}
    end


    @impl true
    def handle_call({:seleccionarMedioPago, {id_compra, medio_pago}}, _from, state) do
      {reply, new_state} = Libremarket.Compras.seleccionar_medio_pago_state(state, {id_compra, medio_pago})
      new_state = replicar_estado_compra(new_state, id_compra)
      {:reply, reply, new_state}
    end

    @impl true
    def handle_call({:seleccionarFormaEntrega, {id_compra, forma_entrega}}, _from, state) do
      {reply, new_state} = Libremarket.Compras.seleccionar_forma_entrega_state(state, {id_compra, forma_entrega})
      new_state = replicar_estado_compra(new_state, id_compra)
      {:reply, reply, new_state}
    end

    @impl true
    def handle_call(:confirmarCompra, _from, state) do
      resultado = Libremarket.Compras.confirmarCompra()
      {:reply, resultado, state}
    end

    @impl true
    def handle_call(:listarCompras, _from, state) do
      {:reply, state.compras, state}
    end

    @impl true
    def handle_call({:obtenerCompra, id_compra}, _from, state) do
      case Map.fetch(state.compras, id_compra) do
        {:ok, {status, info}} ->
          {:reply, {:ok, {status, info}}, state}

        :error ->
          {:reply, {:error, :not_found}, state}
      end
    end



    # ============== HANDLE CALL AMQP ===================

  #   @impl true
  # def init(_opts) do
  #   Logger.info("[COMPRAS] 🛒 Iniciando servidor Compras.Server...")

  #   base_state = %{compras: %{}}

  #   if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
  #     {:ok, chan} = connect_amqp!()
  #     Logger.info("[COMPRAS] Canal AMQP abierto (pid=#{inspect(chan.pid)})")

  #     # Declarar colas (idempotente)
  #     Queue.declare(chan, @req_infr_q,  durable: false)
  #     Queue.declare(chan, @req_pago_q,  durable: false)
  #     Queue.declare(chan, @resp_q,      durable: false)
  #     Queue.declare(chan, @req_env_q,   durable: false)
  #     Queue.declare(chan, @req_ventas_q, durable: false)

  #     # Consumir respuestas (no_ack: true porque solo reenviamos al cliente esperando)
  #     {:ok, _ctag} = Basic.consume(chan, @resp_q, nil, no_ack: true)

  #     Logger.info("[COMPRAS] Escuchando cola #{@resp_q}")

  #     # Estado del primario: canal + mapas de espera y pendientes
  #     state = %{
  #       chan: chan,
  #       waiting: %{},
  #       compras: %{},
  #       pending_infraccion: %{},
  #       pending_pago: %{},
  #       pending_reserva: %{},
  #       pending_envio: %{}
  #     }

  #     {:ok, state}
  #     else
  #       Logger.info("[COMPRAS] Nodo réplica iniciado (modo lectura, sin AMQP).")
  #       {:ok, base_state}
  #     end
  #   end # Fin de método init

    @impl true
    def init(_opts) do
      Logger.info("[COMPRAS] 🛒 Iniciando servidor Compras.Server...")
      Process.flag(:trap_exit, true)

      base_state = %{
        compras: %{},
        waiting: %{},
        pending_infraccion: %{},
        pending_pago: %{},
        pending_reserva: %{},
        pending_envio: %{},
        conn: nil,
        chan: nil,
        backoff: @min_backoff,
        role: :unknown
      }

      # Igual que todos los demás módulos con ZK:
      send(self(), :ensure_role)

      {:ok, base_state}
    end


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
            Logger.info("[Compras.Server] 🔼 Cambio de rol → ahora soy LÍDER")
            # si no hay conexión AMQP, iniciamos
            send(self(), :connect)
            %{state | role: :leader}

          {:leader, :replica} ->
            Logger.info("[Comp.Server] 🔽 Cambio de rol → ahora soy RÉPLICA (cierro AMQP)")
            safe_close(state.chan)
            safe_close(state.conn)
            %{state | role: :replica, conn: nil, chan: nil, backoff: @min_backoff}

          {:unknown, :replica} ->
            Logger.info("[Compras.Server] Rol inicial detectado: RÉPLICA")
            %{state | role: :replica}

          {:unknown, :leader} ->
            Logger.info("[Compras.Server] Rol inicial detectado: LÍDER")
            send(self(), :connect)
            %{state | role: :leader}
        end

      # volvemos a chequear dentro de un rato
      Process.send_after(self(), :ensure_role, @leader_check_interval)
      {:noreply, state2}
    end

    @impl true
    def handle_info(:connect, %{backoff: b, role: :leader} = s) do
      case connect_amqp() do
        {:ok, conn, chan} ->
          Logger.info("[Compras.Server] ✔ AMQP conectado")

          {:ok, _} = Queue.declare(chan, @req_infr_q,  durable: false)
          {:ok, _} = Queue.declare(chan, @req_pago_q,  durable: false)
          {:ok, _} = Queue.declare(chan, @resp_q,      durable: false)
          {:ok, _} = Queue.declare(chan, @req_env_q,   durable: false)
          {:ok, _} = Queue.declare(chan, @req_ventas_q, durable: false)

          {:ok, _} = Basic.consume(chan, @resp_q, nil, no_ack: true)

          Process.monitor(conn.pid)
          {:noreply, %{s | conn: conn, chan: chan, backoff: @min_backoff}}

        {:error, reason} ->
          Logger.warning("[Compras.Server] AMQP no conectado (#{inspect(reason)}). Reintento en #{b} ms")
          Process.send_after(self(), :connect, b)
          {:noreply, %{s | conn: nil, chan: nil, backoff: min(b*2, @max_backoff)}}
      end
    end


    # Si recibimos :connect pero ya no somos líder → lo ignoramos
    @impl true
    def handle_info(:connect, state) do
      Logger.info(
        "[Compras.Server] handle_info(:connect) recibido pero role=#{inspect(state.role)} → ignorado"
      )

      {:noreply, state}
    end


    @impl true
    def handle_info({:DOWN, _mref, :process, pid, reason}, %{role: :leader} = state) do
      Logger.warning(
        "[Compras.Server] AMQP DOWN desde pid=#{inspect(pid)} reason=#{inspect(reason)}. Reintentando…"
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
        "[Compras.Server] AMQP DOWN (role=#{inspect(state.role)}) pid=#{inspect(pid)} reason=#{inspect(reason)}."
      )

      safe_close(state.chan)
      safe_close(state.conn)
      {:noreply, %{state | conn: nil, chan: nil}}
    end



    @impl true
    def handle_cast({:rpc_infraccion_async, id_compra, timeout_ms}, %{chan: chan, pending_infraccion: pend} = s) do
      payload = Jason.encode!(%{id_compra: id_compra})

      Basic.publish(
        chan, "", @req_infr_q, payload,
        content_type: "application/json"
      )

      tref = Process.send_after(self(), {:rpc_infraccion_timeout, id_compra}, timeout_ms)
      {:noreply, %{s | pending_infraccion: Map.put(pend, id_compra, tref)}}
    end

    @impl true
    def handle_cast({:rpc_pago_async, id_compra, timeout_ms}, %{chan: chan, pending_pago: pend} = s) do
      payload = Jason.encode!(%{id_compra: id_compra})
      Basic.publish(chan, "", @req_pago_q, payload, content_type: "application/json")
      tref = Process.send_after(self(), {:rpc_pago_timeout, id_compra}, timeout_ms)
      {:noreply, %{s | pending_pago: Map.put(pend, id_compra, tref)}}
    end

    @impl true
    def handle_cast({:rpc_reserva_async, id_compra, id_producto, timeout_ms}, %{chan: chan, pending_reserva: pend} = s) do
      payload = Jason.encode!(%{type: "reservar", id_compra: id_compra, id_producto: id_producto})
      Basic.publish(chan, "", @req_ventas_q, payload, content_type: "application/json")

      tref = Process.send_after(self(), {:rpc_reserva_timeout, id_compra}, timeout_ms)
      {:noreply, %{s | pending_reserva: Map.put(pend, id_compra, tref)}}
    end

    @impl true
    def handle_info({:rpc_reserva_timeout, id_compra}, %{pending_reserva: pend} = s) do
      case Map.pop(pend, id_compra) do
        {nil, _} ->
          {:noreply, s} # ya llegó la respuesta
        {_tref, pend2} ->
          # si querés, marcá error terminal por timeout:
          {s2, _} = aplicar_reserva_fail(%{s | pending_reserva: pend2}, id_compra, :timeout_reserva)
          {:noreply, s2}
      end
    end

    @impl true
    def handle_cast({:rpc_envio_calcular_async, id, forma, timeout},
                    %{chan: chan, pending_envio: pend} = s) do
      payload = Jason.encode!(%{
        accion: "calcular",
        id_compra: id,
        forma_entrega: to_string(forma) # "correo" | "retira"
      })

      Basic.publish(chan, "", @req_env_q, payload, content_type: "application/json")
      tref = Process.send_after(self(), {:rpc_envio_timeout, id}, timeout)
      {:noreply, %{s | pending_envio: Map.put(pend, id, tref)}}
    end

    @impl true
    def handle_cast({:rpc_envio_agendar_async, id, costo, timeout}, %{chan: chan, pending_envio: pend} = s) do
      payload = Jason.encode!(%{accion: "agendar", id_compra: id, costo: costo})
      Basic.publish(chan, "", @req_env_q, payload, content_type: "application/json")
      tref = Process.send_after(self(), {:rpc_envio_timeout, id}, timeout)
      {:noreply, %{s | pending_envio: Map.put(pend, id, tref)}}
    end


    @impl true
    def handle_info({:basic_deliver, payload, _meta}, s) do
      case Jason.decode(payload) do
        # infracciones
        {:ok, %{"id_compra" => id, "infraccion" => infr}} ->
          {s2, _} = actualizar_compra_por_infraccion(s, id, infr)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        # pagos
        {:ok, %{"id_compra" => id, "autorizacionPago" => ok?}} ->
          {s2, _} = actualizar_compra_por_pago(s, id, ok?)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        # reserva OK
        {:ok, %{"type" => "reservar_res", "id_compra" => id, "id_producto" => _idp, "result" => "ok", "precio" => precio}} ->
          {s2, _} = aplicar_reserva_ok(s, id, precio)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        # reserva errores
        {:ok, %{"type" => "reservar_res", "id_compra" => id, "result" => "sin_stock"}} ->
          {s2, _} = aplicar_reserva_fail(s, id, :sin_stock)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        {:ok, %{"type" => "reservar_res", "id_compra" => id, "result" => "no_existe"}} ->
          {s2, _} = aplicar_reserva_fail(s, id, :no_existe)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        # calcular envío
        {:ok, %{"id_compra" => id, "precio_envio" => costo}} ->
          {s2, _} = actualizar_compra_por_envio(s, id, costo)
          s2 = replicar_estado_compra(s2, id)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}

        # agendar envío (informativo)
        {:ok, %{"id_compra" => _id, "envio_agendado" => true, "id_envio" => id_envio}} ->
          Logger.info("Envio agendado OK: id_envio=#{id_envio}")
          {:noreply, s}

        _ ->
          {:noreply, s}
      end
    end


    @impl true
    def handle_info({:rpc_infraccion_timeout, id_compra}, %{pending_infraccion: pend} = s) do
      case Map.pop(pend, id_compra) do
        {nil, _} -> {:noreply, s}
        {_tref, pend2} ->
          {s2, _} = actualizar_compra_por_infraccion(%{s | pending_infraccion: pend2}, id_compra, :desconocido)
          {s3, _} = finalize_if_ready(s2, id_compra)
          {:noreply, s3}
      end
    end

    @impl true
    def handle_info({:rpc_pago_timeout, id_compra}, %{pending_pago: pend} = s) do
      case Map.pop(pend, id_compra) do
        {nil, _} -> {:noreply, s}
        {_tref, pend2} ->
          {s2, _} = actualizar_compra_por_pago(%{s | pending_pago: pend2}, id_compra, :desconocido)
          {s3, _} = finalize_if_ready(s2, id_compra)
          {:noreply, s3}
      end
    end

    @impl true
    def handle_info({:rpc_envio_timeout, id}, %{pending_envio: pend} = s) do
      case Map.pop(pend, id) do
        {nil, _} -> {:noreply, s}
        {_tref, pend2} ->
          {s2, _} = actualizar_compra_por_envio(%{s | pending_envio: pend2}, id, :desconocido)
          {s3, _} = finalize_if_ready(s2, id)
          {:noreply, s3}
      end
    end


    # @impl true
    # def handle_call({:replicar_resultado, id_compra, nuevo_estado_compra}, _from, %{productos: productos} = state) do
    #   Logger.info("[Compras] Replicando producto #{id_compra} actualizado: #{inspect(nuevo_estado_compra)}")

    #   # Actualizamos solo el producto específico en el mapa
    #   mapa_productos = Map.put(productos, id_compra, nuevo_estado_compra)

    #   {:reply, :ok, %{state | productos: mapa_productos}}
    # end

    @impl true
    def handle_call({:replicar_resultado, id_compra, nuevo_estado_compra}, _from, %{compras: compras} = state) do
      Logger.info("[Compras] 🔁 Replicando compra #{id_compra} actualizada: #{inspect(nuevo_estado_compra)}")

      # Actualizamos solo la compra específica en el mapa
      mapa_compras = Map.put(compras, id_compra, nuevo_estado_compra)

      {:reply, :ok, %{state | compras: mapa_compras}}
    end

    defp actualizar_compra_por_infraccion(%{compras: compras} = s, id_compra, infr) do
      Logger.info("[ACTUALIZAR COMPRA POR INFRACCION] Procesando infracción para id_compra=#{inspect(id_compra)}, infracción=#{inspect(infr)}")

      case Map.fetch(compras, id_compra) do
        :error ->
          Logger.warn("[ACTUALIZAR COMPRA POR INFRACCION] No se encontró la compra con id_compra=#{inspect(id_compra)}")
          {s, false}

        # Si ya quedó :ok (decidido por obtenerCompra/2), ignoramos una infracción tardía
        {:ok, {:ok, _compra}} ->
          Logger.info("[ACTUALIZAR COMPRA POR INFRACCION] La compra con id_compra=#{inspect(id_compra)} ya está en estado :ok, ignorando infracción.")
          s2 = cancel_pending_infraccion_timer(s, id_compra)
          {s2, false}

        {:ok, {status, compra}} ->
          Logger.info("[ACTUALIZAR COMPRA POR INFRACCION] Compra encontrada (status=#{inspect(status)}).")

          # Actualizamos el campo :infraccion en la compra
          compra1 = Map.put(compra, :infraccion, infr)
          compras2 = Map.put(compras, id_compra, {status, compra1})

          # Cancelamos el timer pendiente (si existía)
          s2 = cancel_pending_infraccion_timer(%{s | compras: compras2}, id_compra)

          # ✅ Nuevo: chequeamos si hay que liberar el producto
          s3 = checkear_estado_producto(s2, id_compra)

          Logger.info("[ACTUALIZAR COMPRA POR INFRACCION] Infracción #{inspect(infr)} aplicada y estado verificado.")
          {s3, true}
      end
    end



    defp cancel_pending_infraccion_timer(%{pending_infraccion: pend} = s, id_compra) do
      case Map.pop(pend, id_compra) do
        {nil, _} -> s
        {tref, pend2} ->
          Process.cancel_timer(tref)
          %{s | pending_infraccion: pend2}
      end
    end

    defp actualizar_compra_por_pago(%{compras: compras} = s, id_compra, ok?) do
      case Map.fetch(compras, id_compra) do
        :error ->
          {s, false}

        {:ok, {:ok, _compra}} ->
          # si ya quedó :ok definitivo, ignoramos late replies
          s2 = cancel_pending_pago_timer(s, id_compra)
          {s2, false}

        {:ok, {status, compra}} ->
          Logger.info("[ACTUALIZAR COMPRA POR PAGO] Actualizando pago para id_compra=#{id_compra}, autorizacion=#{inspect(ok?)}")

          # Actualizamos el campo :autorizacionPago
          compra1 = Map.put(compra, :autorizacionPago, ok?)
          compras2 = Map.put(compras, id_compra, {status, compra1})

          # Cancelamos el timer pendiente
          s2 = cancel_pending_pago_timer(%{s | compras: compras2}, id_compra)

          # 🟢 Si el pago fue exitoso, publicamos solicitud de envío
          if ok? == true do
            _ = publicar_agendar_envio(s2, id_compra, compra[:precio_envio])
          end

          # ✅ Nuevo: chequeamos si hay que liberar el producto
          s3 = checkear_estado_producto(s2, id_compra)

          {s3, true}
      end
    end


    defp cancel_pending_pago_timer(%{pending_pago: pend} = s, id_compra) do
      case Map.pop(pend, id_compra) do
        {nil, _} -> s
        {tref, pend2} ->
          Process.cancel_timer(tref)
          %{s | pending_pago: pend2}
      end
    end


    defp actualizar_compra_por_envio(%{compras: compras} = s, id, costo) do
      case Map.fetch(compras, id) do
        :error ->
          {s, false}

        {:ok, {:ok, _compra}} ->
          # Ya cerrada OK: ignorar actualización de costo tardía, pero cancelá el timer
          s2 = cancel_pending_envio_timer(s, id)
          {s2, false}

        {:ok, {status, compra}} ->
          compra1 = Map.put(compra, :precio_envio, costo)
          compras2 = Map.put(compras, id, {status, compra1})
          s2 = cancel_pending_envio_timer(%{s | compras: compras2}, id)
          {s2, true}
      end
    end


    defp cancel_pending_envio_timer(%{pending_envio: pend} = s, id) do
      case Map.pop(pend, id) do
        {nil, _} -> s
        {tref, pend2} ->
          Process.cancel_timer(tref)
          %{s | pending_envio: pend2}
      end
    end








    defp aplicar_reserva_ok(%{compras: compras} = s, id_compra, precio) do
      case Map.fetch(compras, id_compra) do
        :error ->
          {s, false}

        {:ok, {status, compra}} ->
          Logger.info("[APLICAR RESERVA OK] Compra #{id_compra}: reserva exitosa con precio #{precio}")

          compra1 =
            compra
            |> Map.put(:precio_producto, precio)
            |> Map.put(:reservado, true)

          compras2 = Map.put(compras, id_compra, {status, compra1})
          s2 = cancel_pending_reserva_timer(%{s | compras: compras2}, id_compra)

          # ✅ chequeo global del estado del producto
          s3 = checkear_estado_producto(s2, id_compra)

          {s3, true}
      end
    end

    defp aplicar_reserva_fail(%{compras: compras} = s, id_compra, motivo) do
      case Map.fetch(compras, id_compra) do
        :error ->
          {s, false}

        {:ok, {_status, compra}} ->
          Logger.info("[APLICAR RESERVA FAIL] Compra #{id_compra}: reserva fallida por #{inspect(motivo)}")

          compra1 =
            compra
            |> Map.put(:reservado, false)
            |> Map.put(:motivo_reserva, motivo)

          # error terminal por reserva fallida
          compras2 = Map.put(compras, id_compra, {:error, compra1})
          s2 = cancel_pending_reserva_timer(%{s | compras: compras2}, id_compra)

          # ✅ chequeo global del estado del producto
          s3 = checkear_estado_producto(s2, id_compra)

          {s3, true}
      end
    end


    defp cancel_pending_reserva_timer(%{pending_reserva: pend} = s, id_compra) do
      case Map.pop(pend, id_compra) do
        {nil, _} -> s
        {tref, pend2} ->
          Process.cancel_timer(tref)
          %{s | pending_reserva: pend2}
      end
    end



    defp publicar_liberar_producto(%{chan: chan}, id_producto) when not is_nil(id_producto) do
      payload = Jason.encode!(%{type: "liberar", id_producto: id_producto})
      Basic.publish(chan, "", @req_ventas_q, payload, content_type: "application/json")
      :ok
    end
    defp publicar_liberar_producto(_s, _), do: :ok

    defp publicar_agendar_envio(%{chan: chan}, id_compra, costo)
        when not is_nil(id_compra) and not is_nil(costo) do
      payload = Jason.encode!(%{accion: "agendar", id_compra: id_compra, costo: costo})
      Basic.publish(chan, "", @req_env_q, payload, content_type: "application/json")
      Logger.info("Solicitud de agendado de envío publicada para compra #{id_compra} (costo #{costo})")
      :ok
    end

    defp publicar_agendar_envio(_s, _id_compra, _costo), do: :ok

    defp checkear_estado_producto(%{chan: chan} = s, id_compra) do
      case Map.fetch(s.compras, id_compra) do
        :error ->
          s

        {:ok, {_status, compra}} ->
          reservado = compra[:reservado] == true
          infraccion = compra[:infraccion]
          pago = compra[:autorizacionPago]

          if reservado do
            cond do
              infraccion in [true, :desconocido] ->
                Logger.info("[CHECK] Liberando producto #{compra[:id_producto]} por infracciÃ³n=#{inspect(infraccion)}")
                _ = publicar_liberar_producto(s, compra[:id_producto])
                put_in(s, [:compras, id_compra, Access.elem(1), :reservado], false)

              pago in [false, :desconocido] ->
                Logger.info("[CHECK] Liberando producto #{compra[:id_producto]} por pago=#{inspect(pago)}")
                _ = publicar_liberar_producto(s, compra[:id_producto])
                put_in(s, [:compras, id_compra, Access.elem(1), :reservado], false)

              true ->
                s
            end
          else
            s
          end
      end
    end


    defp replicar_estado_compra(%{compras: compras} = state, id_compra) do
      case Map.fetch(compras, id_compra) do
        {:ok, datos_compra} ->
          Libremarket.Compras.replicate_to_replicas(id_compra, datos_compra)
        :error ->
          :noop
      end

      state
    end



    # ===== AMQP administración y fallback =====
    @impl true
    def handle_info({:basic_consume_ok, _}, state), do: {:noreply, state}

    @impl true
    def handle_info({:basic_cancel, _}, state), do: {:stop, :normal, state}

    @impl true
    def handle_info({:basic_cancel_ok, _}, state), do: {:noreply, state}

    @impl true
    def handle_info(_msg, state), do: {:noreply, state}

    # ===== Helpers AMQP =====
    defp connect_amqp() do
      with {:ok, url} <- fetch_amqp_url() do
        insecure? = System.get_env("INSECURE_AMQPS") in ["1", "true", "TRUE"]
        if insecure? and String.starts_with?(url, "amqps://") do
          Logger.info("[Compras.Server] INSECURE_AMQPS activo → SSL sin verificación (solo dev)")
        end

        opts =
          [connection_timeout: 15_000, requested_heartbeat: 30]
          |> maybe_insecure_ssl(url, insecure?)

        Logger.info("[Compras.Server] Abriendo conexión a #{redact(url)} opts=#{inspect(opts)}")

        case Connection.open(url, opts) do
          {:ok, conn} ->
            Logger.info("[Compras.Server] Connection.open ✔️, abriendo canal…")
            case Channel.open(conn) do
              {:ok, chan} ->
                Logger.info("[Compras.Server] Channel.open ✔️")
                {:ok, conn, chan}

              {:error, reason} ->
                Logger.warning("[Compras.Server] Channel.open ✖️ reason=#{inspect(reason)} → cerrando conexión")
                safe_close(conn)
                {:error, reason}
            end

          {:error, reason} ->
            Logger.warning("[Compras.Server] Connection.open ✖️ reason=#{inspect(reason)}")
            {:error, reason}
        end
      end
    end

    # defp reconnect_amqp!() do
    #   :timer.sleep(1000)
    #   connect_amqp!()
    # end

    defp make_cid(),
      do: :erlang.unique_integer([:monotonic, :positive]) |> Integer.to_string()


    defp finalize_if_ready(%{compras: compras} = s, id_compra) do
      case Map.fetch(compras, id_compra) do
        :error ->
          {s, false}

        # Si ya quedó cerrada (ok/error), no tocar
        {:ok, {status, _compra}} when status in [:ok, :error] ->
          {s, false}

        {:ok, {status, compra}} ->
          # ¿Todavía falta algún dato obligatorio para poder cerrar?
          if Enum.any?(@required_for_close, fn k -> is_nil(Map.get(compra, k)) end) do
            {s, false}
          else
            total = (compra[:precio_producto] || 0) + (compra[:precio_envio] || 0)
            compra2 = Map.put(compra, :precio_total, total)

            new_status =
              cond do
                compra2[:infraccion] in [true, :desconocido] -> :error
                compra2[:autorizacionPago] in [false, :desconocido] -> :error
                compra2[:infraccion] == false and compra2[:autorizacionPago] == true -> :ok
                true -> :en_proceso
              end

            # Actualizamos el mapa de compras con el nuevo estado
            compras2  = Map.put(compras, id_compra, {new_status, compra2})
            new_state = %{s | compras: compras2}

            ## SI EL ESTADO DE LA COMPRA DEJA DE ESTAR :en_proceso DUPLICAR DATOS EN LAS REPLICAS
            new_state =
              if new_status in [:ok, :error] do
                # replicamos la compra cerrada a todas las réplicas
                replicar_estado_compra(new_state, id_compra)
              else
                new_state
              end

            {new_state, new_status in [:ok, :error]}
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
          Logger.warning("[Compras.Server] AMQP_URL faltante")
          {:error, :missing_amqp_url}

        url ->
          {:ok, url}
      end
    end

    defp safe_close(%Channel{} = ch) do
      Logger.info("[Compras.Server] Cerrando canal…")
      Channel.close(ch)
    end

    defp safe_close(%Connection{} = c) do
      Logger.info("[Compras.Server] Cerrando conexión…")
      Connection.close(c)
    end

    defp safe_close(_), do: :ok

    defp redact(url) when is_binary(url) do
      case URI.parse(url) do
        %URI{userinfo: nil} -> url
        %URI{} = uri -> %{uri | userinfo: "***:***"} |> URI.to_string()
      end
    end

  end
