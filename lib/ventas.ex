defmodule Libremarket.Ventas do
  require Logger

  @moduledoc """
  Lógica de negocio de Ventas, con logs agregados.
  """

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

        Logger.info("[VENTAS] Producto #{id_producto} reservado → nuevo stock=#{nuevo_estado_producto.stock}, precio=#{precio}")

        replicate_to_replicas(id_producto, nuevo_estado_producto)  # Mandar solo el producto actualizado

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

        replicate_to_replicas(id_producto, nuevo_estado_producto)  # Mandar solo el producto actualizado

        nuevo_state

      otro ->
        Logger.error("[VENTAS] formato inesperado de producto #{id_producto}: #{inspect(otro)} (liberar)")
        map_productos
    end
  end

   defp replica_names do
    :global.registered_names()
    |> Enum.filter(fn name ->
      name_str = Atom.to_string(name)
      String.starts_with?(name_str, "ventas-replica-")
    end)
  end

  # Envia el resultado a cada réplica mediante GenServer.call
  defp replicate_to_replicas(id_producto, nuevo_estado_producto) do
    replicas = replica_names()

    if replicas == [] do
      Logger.warning("[Ventas] ⚠️ No hay réplicas registradas globalmente para sincronizar")
    else
      Enum.each(replicas, fn replica_name ->
        Logger.info("[Ventas] Replicando producto #{id_producto} actualizado en réplica #{replica_name}")

        try do
          GenServer.call({:global, replica_name}, {:replicar_resultado, id_producto, nuevo_estado_producto})
        catch
          kind, reason ->
            Logger.error("[Ventas] Error replicando en #{replica_name}: #{inspect({kind, reason})}")
        end
      end)
    end
  end


  def inicializar_estado_replicas(productos) do
    replicas = replica_names()

    Logger.debug("[VENTAS] Réplicas encontradas para inicializar: #{inspect(replicas)}")
    if replicas == [] do
      Logger.warn("[VENTAS] ⚠️ No hay réplicas registradas globalmente para inicializar estado")
    else
      names = replicas |> Enum.map(&Atom.to_string/1) |> Enum.join(", ")
      Logger.info("[VENTAS] Inicializando estado en #{length(replicas)} réplica(s): #{names}")
      Logger.info("[VENTAS] Productos a inicializar: #{inspect(productos)}")
    end

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

  # ========= API =========
  def start_link(opts \\ %{}) do
    nombre = System.get_env("NOMBRE") |> to_string() |> String.trim() |> String.to_atom()

    global_name = {:global, nombre}

    # Guardamos el valor en :persistent_term (global en el VM)
    :persistent_term.put({__MODULE__, :global_name}, global_name)

    Logger.info("[Ventas.Server] Registrando global_name=#{inspect(global_name)} nodo=#{inspect(node())}")
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

  # ========= Callbacks =========
  @impl true
  def init(_opts) do
    Logger.info("[VENTAS] iniciando servidor Ventas.Server...")

    base_state = %{productos: %{}}

    if System.get_env("ES_PRIMARIO") in ["1", "true", "TRUE"] do
      productos = 1..10 |> Enum.map(fn id -> {id, %{precio: :rand.uniform(1000), stock: :rand.uniform(10)}} end) |> Enum.into(%{})

      inicializar_estado_replicas(productos) # para que todas las replicas tengan los mismos productos con las mismas cantidades

      Logger.debug("[VENTAS] productos inicializados: #{inspect(productos)}")
      {:ok, chan} = connect_amqp!()
      Logger.info("[VENTAS] canal AMQP abierto (pid=#{inspect(chan.pid)})")

      Queue.declare(chan, @req_q,  durable: false)
      Queue.declare(chan, @resp_q, durable: false)
      {:ok, _} = Basic.consume(chan, @req_q, nil, no_ack: true)

      Logger.info("[VENTAS] escuchando cola #{@req_q}, responderá en #{@resp_q}")

      {:ok, %{productos: productos, chan: chan}}
    else
      {:ok, base_state}
    end
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

  # ===== Helpers AMQP =====
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
end
