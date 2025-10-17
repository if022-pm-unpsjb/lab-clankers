defmodule Libremarket.Compras do

  #def node_ventas(), do: :"ventas@equipoalan"
  #def node_pagos(), do: :"pagos@equipoalan"
  #def node_envios(), do: :"envios@equipoalan"
  #def node_infracciones(), do: :"infracciones@equipoalan"

  # ==== API principal ====
  def comprar(id_compra, id_producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{id_producto}")

    # Paso 1: inicialización de datos de compra
    resultado_base = inicializar_compra(id_compra, id_producto, medio_pago, forma_entrega)

    # Paso 2: reservar producto
    case Libremarket.Ventas.Server.reservarProducto(id_producto) do  # Ventas.Server.reservarProducto(id_producto)
      :sin_stock ->
        IO.puts("Producto #{id_producto} no disponible. Cancelando compra...")
        {:error, resultado_base}

      _ ->
        # Paso 3: chequeo de infracción
        case chequear_infraccion(id_compra, id_producto, resultado_base) do
          {:error, resultado} ->
            {:error, resultado}

          {:ok, resultado_infraccion} ->
              # Paso 4: autorización de pago
              resultado_pago = autorizar_pago(id_compra, id_producto, forma_entrega, resultado_infraccion)

              if Map.get(resultado_pago, :autorizacionPago) == false do
                {:error, resultado_pago}
              else
                {:ok, resultado_pago}
          end
        end
    end
  end

  def comprar_timed(id_compra, id_producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{id_producto}")
    :timer.sleep(:rand.uniform(1000))

    # Paso 1: inicialización de datos de compra
    resultado_base = inicializar_compra(id_compra, id_producto, medio_pago, forma_entrega)
    :timer.sleep(:rand.uniform(1000))

    # Paso 2: reservar producto
    case Libremarket.Ventas.Server.reservarProducto(id_producto) do  # Libremarket.Ventas.Server.reservarProducto(id_producto) do
      :sin_stock ->
        IO.puts("Producto #{id_producto} no disponible. Cancelando compra...")
        {:error, resultado_base}

      _ ->
        # Paso 3: chequeo de infracción
        case chequear_infraccion(id_compra, id_producto, resultado_base) do
          {:error, resultado} ->
            {:error, resultado}

          {:ok, resultado_infraccion} ->
              # Paso 4: autorización de pago
              resultado_pago = autorizar_pago(id_compra, id_producto, forma_entrega, resultado_infraccion)

              if Map.get(resultado_pago, :autorizacionPago) == false do
                {:error, resultado_pago}
              else
                {:ok, resultado_pago}
          end
      end
    end
  end

  # ==== Paso 1: inicialización ====
  defp inicializar_compra(id_compra, id_producto, medio_pago, forma_entrega) do
    precio_producto = Libremarket.Ventas.Server.get_precio(id_producto) # precio_producto = Libremarket.Ventas.Server.get_precio(id_producto) #
    costo_envio = Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega}) # costo_envio = Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega}) #
    precio_total = precio_producto + costo_envio

    %{
      id_producto: id_producto,
      medio_pago: medio_pago,
      forma_entrega: forma_entrega,
      precio_producto: precio_producto,
      precio_envio: costo_envio,
      precio_total: precio_total,
      infraccion: nil,
      autorizacionPago: nil
    }
  end

  # ==== Paso 2: chequeo de infracción ====
  defp chequear_infraccion(id_compra, id_producto, resultado) do
    case Libremarket.Compras.Server.detectar_infraccion_amqp(id_compra, 5_000) do
      {:ok, %{id_compra: ^id_compra, infraccion: true}} ->
        IO.puts("Infracción detectada. Cancelando compra...")
        Libremarket.Ventas.Server.liberarProducto(id_producto)
        {:error, Map.put(resultado, :infraccion, true)}

      {:ok, %{id_compra: ^id_compra, infraccion: false}} ->
        {:ok, Map.put(resultado, :infraccion, false)}

      {:error, :timeout} ->
        IO.puts("Timeout esperando respuesta de Infracciones.")
        Libremarket.Ventas.Server.liberarProducto(id_producto)
        {:error, Map.put(resultado, :infraccion, :desconocido)}
    end
  end

  # ==== Paso 3: autorización de pago ====
  defp autorizar_pago(id_compra, id_producto, forma_entrega, resultado) do
    autorizacionPago = Libremarket.Pagos.Server.autorizarPago(id_compra)  # Libremarket.Pagos.Server.autorizarPago(id_compra)

    cond do
      autorizacionPago == false ->
        IO.puts("Pago no autorizado. Cancelando compra...")
        Libremarket.Ventas.Server.liberarProducto(id_producto) # Libremarket.Ventas.Server.liberarProducto(id_producto)
        resultado
        |> Map.put(:autorizacionPago, false)

      autorizacionPago == true ->
        if forma_entrega == :correo do
          Libremarket.Envios.Server.agendarEnvio({id_compra, resultado.precio_envio})
        end

        resultado
        |> Map.put(:autorizacionPago, true)

      true ->
        resultado
    end
  end

  # ==== Confirmación aleatoria ====
  def confirmarCompra() do
    compra_confirmada = :rand.uniform(100) < 80
    IO.puts("Compra confirmada: #{compra_confirmada}")
    compra_confirmada
  end

end

defmodule Libremarket.Compras.Server do
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}

  # Colas fijas
  @req_q  "infracciones.req"   # hacia Infracciones
  @resp_q "compras.resp"       # respuestas para Compras

  # ========== API ==========
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def comprar(pid \\ @global_name, datos_compra),
    do: GenServer.call(pid, {:comprar, datos_compra})

  def comprar_timed(pid \\ @global_name, datos_compra),
    do: GenServer.call(pid, {:comprar_timed, datos_compra}, 15_000)

  def confirmarCompra(pid \\ @global_name),
    do: GenServer.call(pid, :confirmarCompra)

  def listarCompras(pid \\ @global_name),
    do: GenServer.call(pid, :listarCompras)

  def obtener(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:obtener_compra, id_compra})

  @doc """
  Publica pedido en #{@req_q} y espera respuesta en #{@resp_q}.
  Retorna {:ok, %{id_compra: id, infraccion: bool}} o {:error, :timeout}
  """
  def detectar_infraccion_amqp(id_compra, timeout_ms \\ 5_000) do
    detectar_infraccion_amqp(@global_name, id_compra, timeout_ms)
  end

  def detectar_infraccion_amqp(pid, id_compra, timeout_ms) do
    GenServer.call(pid, {:rpc_infraccion, id_compra, timeout_ms}, timeout_ms + 1000)
  end

  # ========== Callbacks ==========
  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    {:ok, chan} = connect_amqp!()

    # Declarar colas (idempotente)
    Queue.declare(chan, @req_q,  durable: false)
    Queue.declare(chan, @resp_q, durable: false)

    # Consumir respuestas (no_ack: true porque solo reenviamos al cliente esperando)
    {:ok, _ctag} = Basic.consume(chan, @resp_q, nil, no_ack: true)

    # Estado: canal amqp + mapa de esperas por correlation_id + compras
    {:ok, %{chan: chan, waiting: %{}, compras: %{}}}
  end

  @impl true
  def handle_call({:comprar, {id_producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])
    datos_compra = Libremarket.Compras.comprar(id_compra, id_producto, medio_pago, forma_entrega)

    # ✅ guardar bajo :compras (no en la raíz)
    new_state = %{state | compras: Map.put(state.compras, id_compra, datos_compra)}
    {:reply, datos_compra, new_state}
  end

  @impl true
  def handle_call({:comprar_timed, {id_producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])
    datos_compra = Libremarket.Compras.comprar_timed(id_compra, id_producto, medio_pago, forma_entrega)

    # ✅ idem
    new_state = %{state | compras: Map.put(state.compras, id_compra, datos_compra)}
    {:reply, datos_compra, new_state}
  end

  @impl true
  def handle_call(:listarCompras, _from, state) do
    # ✅ devolver solo el mapa de compras
    {:reply, state.compras, state}
  end

  @impl true
  def handle_call(:confirmarCompra, _from, state) do
    resultado = Libremarket.Compras.confirmarCompra()
    {:reply, resultado, state}
  end

  @impl true
  def handle_call({:obtener_compra, id_compra}, _from, state) do
    # ✅ buscar en state.compras
    case Map.fetch(state.compras, id_compra) do
      {:ok, datos} -> {:reply, {:ok, %{id_compra: id_compra, resultado: datos}}, state}
      :error       -> {:reply, {:error, :not_found}, state}
    end
  end

  # ===== Publicar pedido y registrar espera (RPC) =====
  @impl true
  def handle_call({:rpc_infraccion, id_compra, timeout_ms}, from, %{chan: chan, waiting: waiting} = s) do
    corr = make_cid()
    payload = Jason.encode!(%{id_compra: id_compra})

    Basic.publish(
      chan, "", @req_q, payload,
      correlation_id: corr,
      content_type: "application/json"
    )

    tref = Process.send_after(self(), {:rpc_timeout, corr}, timeout_ms)
    {:noreply, %{s | waiting: Map.put(waiting, corr, {from, tref})}}
  end

  # ===== Recibir respuestas desde compras.resp =====
  @impl true
  def handle_info({:basic_deliver, payload, %{correlation_id: cid}}, %{waiting: waiting} = s) do
    case Map.pop(waiting, cid) do
      {nil, _} ->
        # respuesta desconocida/antigua
        {:noreply, s}

      {{from, tref}, waiting2} ->
        Process.cancel_timer(tref)

        reply =
          case Jason.decode(payload) do
            {:ok, %{"id_compra" => id, "infraccion" => infr}} ->
              {:ok, %{id_compra: id, infraccion: infr}}
            _ ->
              {:error, :bad_payload}
          end

        GenServer.reply(from, reply)
        {:noreply, %{s | waiting: waiting2}}
    end
  end

  # ===== Timeout del RPC =====
  @impl true
  def handle_info({:rpc_timeout, cid}, %{waiting: waiting} = s) do
    case Map.pop(waiting, cid) do
      {nil, _} -> {:noreply, s}
      {{from, _tref}, waiting2} ->
        GenServer.reply(from, {:error, :timeout})
        {:noreply, %{s | waiting: waiting2}}
    end
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
  defp connect_amqp!() do
    url = System.fetch_env!("AMQP_URL")
    {:ok, conn} = Connection.open(url,ssl_options: [verify: :verify_none])
    Process.monitor(conn.pid)
    Channel.open(conn)
  end

  defp reconnect_amqp!() do
    :timer.sleep(1000)
    connect_amqp!()
  end

  defp make_cid(),
    do: :erlang.unique_integer([:monotonic, :positive]) |> Integer.to_string()
end
