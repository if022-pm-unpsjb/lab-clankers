defmodule Libremarket.Ventas.Leader do
  use GenServer

  @base_path "/libremarket/ventas"
  @leader_path "/libremarket/ventas/leader"

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

    IO.puts("🟣 Ventas.Leader iniciado como #{List.to_string(my_znode)}")

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

defmodule Libremarket.Ventas do
  require Logger

  @moduledoc """
  Lógica de negocio de Ventas con replicación mediante ZooKeeper.
  Solo el líder replica los cambios.
  """

  # ============================================================
  # API PRINCIPAL
  # ============================================================

  def reservarProducto(id_producto, map_productos) do
    Logger.debug("[VENTAS] reservarProducto id_producto=#{inspect(id_producto)}")

    case Map.get(map_productos, id_producto) do
      nil ->
        Logger.warn("[VENTAS] Producto #{id_producto} no existe")
        :no_existe

      %{stock: 0} ->
        Logger.warn("[VENTAS] Producto #{id_producto} sin stock (stock=0)")
        :sin_stock

      %{stock: stock, precio: precio} ->
        nuevo_estado_producto = %{precio: precio, stock: stock - 1}
        nuevo_state = Map.put(map_productos, id_producto, nuevo_estado_producto)

        Logger.info("[VENTAS] Producto #{id_producto} reservado → nuevo stock=#{nuevo_estado_producto.stock}")

        replicate_if_leader(id_producto, nuevo_estado_producto)

        nuevo_state

      otro ->
        Logger.error("[VENTAS] formato inesperado de producto #{id_producto}: #{inspect(otro)}")
        :error
    end
  end


  def liberarProducto(id_producto, map_productos) do
    Logger.debug("[VENTAS] liberarProducto id_producto=#{inspect(id_producto)}")

    case Map.get(map_productos, id_producto) do
      nil ->
        Logger.warn("[VENTAS] Producto #{id_producto} no existe (liberar)")
        map_productos

      %{stock: stock, precio: precio} ->
        nuevo_estado_producto = %{precio: precio, stock: stock + 1}
        nuevo_state = Map.put(map_productos, id_producto, nuevo_estado_producto)

        Logger.info("[VENTAS] Producto #{id_producto} liberado → nuevo stock=#{nuevo_estado_producto.stock}")

        replicate_if_leader(id_producto, nuevo_estado_producto)

        nuevo_state

      otro ->
        Logger.error("[VENTAS] formato inesperado de producto #{id_producto}: #{inspect(otro)} (liberar)")
        map_productos
    end
  end


  # ============================================================
  # LÓGICA DE LÍDER → SOLO EL LÍDER REPLICA
  # ============================================================

  defp replicate_if_leader(id_producto, nuevo_estado_producto) do
    case safe_leader_check() do
      {:ok, true} ->
        replicate_to_replicas(id_producto, nuevo_estado_producto)

      {:ok, false} ->
        Logger.info("[VENTAS] Soy réplica → NO replico actualizaciones")

      {:error, reason} ->
        Logger.warning("[VENTAS] No pude consultar leader? (#{inspect(reason)}), no replico")
    end
  end

  # Igual al de Pagos
  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Ventas.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end


  # ============================================================
  # REPLICACIÓN A LAS RÉPLICAS
  # ============================================================

  defp replica_names do
    {:global, my_name_atom} = Libremarket.Ventas.Server.global_name()

    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "ventas-")
    end)
    |> Enum.reject(&(&1 == my_name_atom))
  end


  defp replicate_to_replicas(id_producto, nuevo_estado_producto) do
    replicas = replica_names()

    if replicas == [] do
      Logger.warning("[VENTAS] ⚠️ No hay réplicas registradas globalmente para sincronizar")
    else
      Enum.each(replicas, fn replica_name ->
        Logger.info("[VENTAS] Replicando producto #{id_producto} → réplica #{replica_name}")

        try do
          GenServer.call({:global, replica_name}, {:replicar_resultado, id_producto, nuevo_estado_producto})
        catch
          kind, reason ->
            Logger.error("[VENTAS] Error replicando en #{replica_name}: #{inspect({kind, reason})}")
        end
      end)
    end
  end


  # ============================================================
  # INICIALIZACIÓN DE RÉPLICAS
  # ============================================================

  def inicializar_estado_replicas(productos) do
    replicas = replica_names()

    Logger.debug("[VENTAS] Réplicas encontradas: #{inspect(replicas)}")

    Enum.each(replicas, fn replica_name ->
      Logger.info("[VENTAS] Inicializando estado de productos en réplica #{replica_name}")

      try do
        GenServer.call({:global, replica_name}, {:inicializar_estado, productos})
      catch
        kind, reason ->
          Logger.error("[VENTAS] Error al inicializar estado en #{replica_name}: #{inspect({kind, reason})}")
      end
    end)
  end



end



defmodule Libremarket.Ventas.Server do
  @moduledoc """
  Ventas (in-memory) + worker AMQP.
  """
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name nil

  # --- NUEVO: colas ---
  @req_q  "ventas.req"
  @resp_q "compras.resp"

  @min_backoff 500
  @max_backoff 10_000

  @leader_check_interval 2_000  # ms

  # ========= API =========
  def start_link(opts \\ %{}) do
    nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Ventas.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
    wait_for_leader()

    GenServer.start_link(__MODULE__, opts, name: global_name)
  end

  def global_name(), do: :persistent_term.get({__MODULE__, :global_name})

  # API in-memory (seguís pudiendo usarlas si querés):
  def reservarProducto(pid \\ @global_name, id_producto),
    do: GenServer.call(global_name(), {:reservarProducto, id_producto})

  def listarProductos(pid \\ @global_name), do: GenServer.call(global_name(), :listarProductos)

  def liberarProducto(pid \\ @global_name, id_producto),
    do: GenServer.call(global_name(), {:liberarProducto, id_producto})

  def get_precio(pid \\ @global_name, id_producto),
    do: GenServer.call(global_name(), {:get_precio, id_producto})

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Ventas.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Ventas.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Ventas.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # ========= Callbacks =========
  @impl true
  def init(_opts) do
    Logger.info("[VENTAS] iniciando servidor Ventas.Server...")
    Process.flag(:trap_exit, true)

    base_state = %{
      productos: %{},
      conn: nil,
      chan: nil,
      backoff: @min_backoff,
      role: :unknown
    }

    send(self(), :ensure_role)


    {:ok, base_state}
    # if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
    #   productos = 1..10 |> Enum.map(fn id -> {id, %{precio: :rand.uniform(1000), stock: :rand.uniform(10)}} end) |> Enum.into(%{})

    #   Libremarket.Ventas.inicializar_estado_replicas(productos) # para que todas las replicas tengan los mismos productos con las mismas cantidades

    #   Logger.debug("[VENTAS] productos inicializados: #{inspect(productos)}")
    #   {:ok, chan} = connect_amqp!()
    #   Logger.info("[VENTAS] canal AMQP abierto (pid=#{inspect(chan.pid)})")

    #   Queue.declare(chan, @req_q,  durable: false)
    #   Queue.declare(chan, @resp_q, durable: false)
    #   {:ok, _} = Basic.consume(chan, @req_q, nil, no_ack: true)

    #   Logger.info("[VENTAS] escuchando cola #{@req_q}, responderá en #{@resp_q}")

    #   {:ok, %{productos: productos, chan: chan}}
    # else
    # end
  end

  # ====== NEGOCIO in-memory ======
  @impl true
  def handle_call({:reservarProducto, id_producto}, _from, %{productos: state} = s) do
    Logger.debug("[VENTAS] handle_call reservarProducto #{inspect(id_producto)}")

    case Libremarket.Ventas.reservarProducto(id_producto, state) do
      :no_existe ->
        Logger.warn("[VENTAS] handle_call → :no_existe")
        {:reply, :no_existe, s}

      :sin_stock ->
        Logger.warn("[VENTAS] handle_call → :sin_stock")
        {:reply, :sin_stock, s}

      new_state ->
        Logger.info("[VENTAS] handle_call → OK, stock actualizado")
        {:reply, new_state, %{s | productos: new_state}}
    end
  end

  @impl true
  def handle_call({:liberarProducto, id_producto}, _from, %{productos: state} = s) do
    Logger.debug("[VENTAS] handle_call liberarProducto #{inspect(id_producto)}")
    new_state = Libremarket.Ventas.liberarProducto(id_producto, state)
    {:reply, new_state, %{s | productos: new_state}}
  end

  @impl true
  def handle_call(:listarProductos, _from, %{productos: p} = s) do
    Logger.debug("[VENTAS] listarProductos solicitado (#{map_size(p)} productos)")
    {:reply, p, s}
  end

  @impl true
  def handle_call({:get_precio, id_producto}, _from, %{productos: p} = s) do
    precio =
      case Map.get(p, id_producto) do
        nil -> 0
        %{precio: precio} -> precio
      end

    Logger.debug("[VENTAS] get_precio(#{id_producto}) -> #{precio}")
    {:reply, precio, s}
  end

  @impl true
  def handle_call({:replicar_resultado, id_producto, nuevo_estado_producto}, _from, %{productos: productos} = state) do
    Logger.info("[VENTAS] Replicando producto #{id_producto} actualizado: #{inspect(nuevo_estado_producto)}")

    # Actualizamos solo el producto específico en el mapa
    mapa_productos = Map.put(productos, id_producto, nuevo_estado_producto)

    {:reply, :ok, %{state | productos: mapa_productos}}
  end

  @impl true
  def handle_call({:inicializar_estado, productos}, _from, _state) do
    Logger.info("[VENTAS] Inicializando estado con productos: #{inspect(productos)}")
    {:reply, :ok, %{productos: productos}}
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

          Logger.info("[Ventas.Server] 🔼 Cambio de rol → ahora soy LÍDER")

          {:ok, zk} = Libremarket.ZK.connect()

          case zk_read_products(zk) do
            {:ok, productos} ->
              Logger.info("[VENTAS] Productos cargados desde ZooKeeper (persistentes)")
              Libremarket.Ventas.inicializar_estado_replicas(productos)
              send(self(), :connect)
              %{state | role: :leader, productos: productos}

            {:error, :not_initialized} ->
              # PRIMER LÍDER DEL SISTEMA → generar productos
              productos =
                1..10
                |> Enum.map(fn id ->
                  {id, %{precio: :rand.uniform(1000), stock: :rand.uniform(10)}}
                end)
                |> Enum.into(%{})

              Logger.info("[VENTAS] 🆕 Inicializando productos por primera vez: #{inspect(productos)}")

              zk_store_initial_products_if_absent(zk, productos)
              Libremarket.Ventas.inicializar_estado_replicas(productos)
              send(self(), :connect)
              %{state | role: :leader, productos: productos}
          end

        {:leader, :replica} ->
          Logger.info("[Ventas.Server] 🔽 Cambio de rol → ahora soy RÉPLICA (cierro AMQP)")
          safe_close(state.chan)
          safe_close(state.conn)
          %{state | role: :replica, conn: nil, chan: nil, backoff: @min_backoff}

        {:unknown, :replica} ->
          Logger.info("[Ventas.Server] Rol inicial detectado: RÉPLICA")
          %{state | role: :replica}

        {:unknown, :leader} ->
          Logger.info("[Ventas.Server] Rol inicial detectado: LÍDER")
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
        Logger.info("Ventas.Server conectado a AMQP")
        {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
        {:ok, _} = Queue.declare(chan, @resp_q, durable: false)
        {:ok, _} = Basic.consume(chan, @req_q, nil, no_ack: true)
        Process.monitor(conn.pid)
        {:noreply, %{s | conn: conn, chan: chan, backoff: @min_backoff}}

      {:error, reason} ->
        Logger.warning("Ventas AMQP no conectado (#{inspect(reason)}). Reintento en #{b} ms")
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
      "[Ventas.Server] AMQP DOWN desde pid=#{inspect(pid)} reason=#{inspect(reason)}. Reintentando…"
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
      "[Ventas.Server] AMQP DOWN (role=#{inspect(state.role)}) pid=#{inspect(pid)} reason=#{inspect(reason)}."
    )

    safe_close(state.chan)
    safe_close(state.conn)
    {:noreply, %{state | conn: nil, chan: nil}}
  end


  # ====== AMQP worker ======
  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{productos: prods, chan: ch} = s) do
    Logger.debug("[VENTAS] mensaje recibido de AMQP: #{inspect(payload)} meta=#{inspect(meta)}")

    cid = Map.get(meta, :correlation_id)

    case Jason.decode(payload) do
      {:ok, %{"type" => "reservar", "id_compra" => idc, "id_producto" => idp}} ->
        Logger.info("[VENTAS] procesando 'reservar' id_compra=#{idc} id_producto=#{idp}")

        case Libremarket.Ventas.reservarProducto(idp, prods) do
          :no_existe ->
            Logger.warn("[VENTAS] Producto #{idp} no existe (respuesta no_existe)")
            reply(ch, cid, %{type: "reservar_res", id_compra: idc, id_producto: idp, result: "no_existe"})
            {:noreply, s}

          :sin_stock ->
            Logger.warn("[VENTAS] Producto #{idp} sin stock (respuesta sin_stock)")
            reply(ch, cid, %{type: "reservar_res", id_compra: idc, id_producto: idp, result: "sin_stock"})
            {:noreply, s}

          new_prods ->
            precio = new_prods[idp].precio
            Logger.info("[VENTAS] Producto #{idp} reservado, precio=#{precio}")
            reply(ch, cid, %{type: "reservar_res", id_compra: idc, id_producto: idp, result: "ok", precio: precio})
            {:noreply, %{s | productos: new_prods}}
        end

      {:ok, %{"type" => "liberar", "id_producto" => idp}} ->
        Logger.info("[VENTAS] procesando 'liberar' id_producto=#{idp}")
        new_prods = Libremarket.Ventas.liberarProducto(idp, prods)
        {:noreply, %{s | productos: new_prods}}

      {:ok, msg} ->
        Logger.warn("[VENTAS] mensaje desconocido: #{inspect(msg)}")
        {:noreply, s}

      {:error, reason} ->
        Logger.error("[VENTAS] error al decodificar JSON: #{inspect(reason)} payload=#{inspect(payload)}")
        {:noreply, s}
    end
  end

  defp reply(chan, cid, map) do
    Logger.debug("[VENTAS] publicando respuesta a #{@resp_q}: #{inspect(map)}")
    Basic.publish(chan, "", @resp_q, Jason.encode!(map),
      correlation_id: cid, content_type: "application/json"
    )
    :ok
  end


  defp zk_store_initial_products_if_absent(zk, productos) do
    path = "/libremarket/ventas/productos"

    case :erlzk.get_data(zk, path) do
      {:ok, {_data, _stat}} ->
        :exists

      {:error, :no_node} ->
        :erlzk.create(zk, path, Jason.encode!(productos))
        :created
    end
  end


  defp zk_read_products(zk) do
    path = "/libremarket/ventas/productos"

    case :erlzk.get_data(zk, path) do
      {:ok, {data, _stat}} ->
        {:ok, Jason.decode!(data)}

      {:error, :no_node} ->
        {:error, :not_initialized}
    end
  end



  @impl true
  def handle_info({:basic_consume_ok, _}, s), do: {:noreply, s}

  @impl true
  def handle_info({:basic_cancel, _}, s), do: {:stop, :normal, s}

  @impl true
  def handle_info({:basic_cancel_ok, _}, s), do: {:noreply, s}

  @impl true
  def handle_info(_msg, s), do: {:noreply, s}
   #===== Helpers AMQP =====


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

  defp connect_amqp!() do
    url = System.fetch_env!("AMQP_URL")
    uri = URI.parse(url)
    safe = "#{uri.scheme}://#{uri.host}/#{String.trim_leading(uri.path || "", "/")}"
    Logger.info("[VENTAS] conectando a RabbitMQ #{safe}")
    {:ok, conn} = Connection.open(url, ssl_options: [verify: :verify_none])
    Process.monitor(conn.pid)
    Logger.debug("[VENTAS] conexión AMQP abierta pid=#{inspect(conn.pid)}")
    Channel.open(conn)
  end

  @impl true
  def handle_info(msg, s) do
    Logger.debug("[VENTAS] handle_info desconocido: #{inspect(msg)}")
    {:noreply, s}
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
