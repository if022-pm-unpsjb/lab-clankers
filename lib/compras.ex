defmodule Libremarket.Compras do

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
      {:ok, {_status, compra}} ->
        id_producto = compra[:id_producto]



        detectar_infraccion_amqp





        case Libremarket.Infracciones.Server.detectarInfraccion(id_compra) do
          true ->
            compra2 = Map.put(compra, :infraccion, true)
            if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:error, compra2})}
            {{:error, compra2}, new_state}   # <<-- antes devolvías {:error, {:error, compra2}}

          false ->
            compra2 = Map.put(compra, :infraccion, false)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:en_proceso, compra2})}
            {{:ok, {:en_proceso, compra2}}, new_state}

          :desconocido ->
            compra2 = Map.put(compra, :infraccion, :desconocido)
            if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:error, compra2})}
            {{:error, compra2}, new_state}   # <<-- sin el error extra

          {:error, reason} ->
            {{:error, reason}, state}

          other ->
            {{:error, other}, state}
        end

      :error ->
        {{:error, :not_found}, state}
    end
  end


  def autorizar_pago_state(state, id_compra) do
    case Map.fetch(state.compras, id_compra) do
      {:ok, {_status, compra}} ->
        case Libremarket.Pagos.Server.autorizarPago(id_compra) do
          true ->
            compra2 = Map.put(compra, :autorizacionPago, true)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:ok, compra2})}
            {{:ok, {:ok, compra2}}, new_state}

          false ->
            compra2 = Map.put(compra, :autorizacionPago, false)
            if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:error, compra2})}
            {{:error, compra2}, new_state}   # <<-- sin anidar otro {:error, ...}

          :desconocido ->
            compra2 = Map.put(compra, :autorizacionPago, :desconocido)
            if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
            new_state = %{state | compras: Map.put(state.compras, id_compra, {:error, compra2})}
            {{:error, compra2}, new_state}   # <<-- idem

          {:error, reason} ->
            {{:error, reason}, state}

          other ->
            {{:error, other}, state}
        end

      :error ->
        {{:error, :not_found}, state}
    end
  end


  def seleccionar_producto_state(state, {id_compra, id_producto}) do
    with {:ok, {status, compra}} <- Map.fetch(state.compras, id_compra),
         %{} <- Libremarket.Ventas.Server.reservarProducto(id_producto) do
      precio = Libremarket.Ventas.Server.get_precio(id_producto)

      compra2 =
        compra
        |> Map.put(:id_producto, id_producto)
        |> Map.put(:precio_producto, precio)

      new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
      {{:ok, {status, compra2}}, new_state}
    else
      :error     -> {{:error, :not_found}, state}
      :no_existe -> {{:error, :no_existe}, state}
      :sin_stock -> {{:error, :sin_stock}, state}
      other      -> {{:error, other}, state}
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
            costo_envio = Libremarket.Envios.Server.calcularEnvio({id_compra, :correo})

            compra2 =
              compra
              |> Map.put(:forma_entrega, :correo)
              |> Map.put(:precio_envio, costo_envio)

            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {{:ok, {status, compra2}}, new_state}

          :retira ->
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
end



defmodule Libremarket.Compras.Server do
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}

  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  # ========== API ==========
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def confirmarCompra(pid \\ @global_name),
    do: GenServer.call(pid, :confirmarCompra)

  def inicializarCompra(pid \\ @global_name),
    do: GenServer.call(pid, :inicializarCompra)

  def seleccionarProducto(pid \\ @global_name, datos),
    do: GenServer.call(pid, {:seleccionarProducto, datos})

  def seleccionarMedioPago(pid \\ @global_name, datos),
    do: GenServer.call(pid, {:seleccionarMedioPago, datos})

  def seleccionarFormaEntrega(pid \\ @global_name, datos),
    do: GenServer.call(pid, {:seleccionarFormaEntrega, datos})

  def detectarInfraccion(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:detectarInfraccion, id_compra})

  def autorizarPago(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:autorizarPago, id_compra})

  def listarCompras(pid \\ @global_name),
    do: GenServer.call(pid, :listarCompras)

  def obtenerCompra(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:obtenerCompra, id_compra})


  # ============== API AMQP ===================
  def detectar_infraccion_amqp(id_compra, timeout_ms \\ 5_000) do
    detectar_infraccion_amqp(@global_name, id_compra, timeout_ms)
  end

  def detectar_infraccion_amqp(pid, id_compra, timeout_ms) do
    GenServer.call(pid, {:rpc_infraccion, id_compra, timeout_ms}, timeout_ms + 1000)
  end

  # ============== HANDLE CALL ===================

  @impl true
  def handle_call(:inicializarCompra, _from, state) do
    {id_compra, new_state} = Libremarket.Compras.inicializar_compra_state(state)
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
  def handle_call({:seleccionarProducto, datos}, _from, state) do
    {reply, new_state} = Libremarket.Compras.seleccionar_producto_state(state, datos)
    {:reply, reply, new_state}
  end

  @impl true
  def handle_call({:seleccionarMedioPago, datos}, _from, state) do
    {reply, new_state} = Libremarket.Compras.seleccionar_medio_pago_state(state, datos)
    {:reply, reply, new_state}
  end

  @impl true
  def handle_call({:seleccionarFormaEntrega, datos}, _from, state) do
    {reply, new_state} = Libremarket.Compras.seleccionar_forma_entrega_state(state, datos)
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
      {:ok, {_estado_guardado, info}} ->
        info1 = Map.put_new(info, :id_compra, id_compra)

        # Si falta algún campo => sigue en proceso
        required = [:id_compra, :precio_producto, :precio_envio, :autorizacionPago, :id_producto, :infraccion, :forma_entrega, :medio_pago]
        if Enum.any?(required, fn k -> is_nil(Map.get(info1, k)) end) do
          {:reply, :en_proceso, state}
        else
          # calcular precio_total también para error/ok
          total = (info1[:precio_producto] || 0) + (info1[:precio_envio] || 0)
          info2 = Map.put(info1, :precio_total, total)

          status =
            cond do
              info2[:infraccion] == true -> :error
              info2[:autorizacionPago] == false -> :error
              info2[:infraccion] == :desconocido -> :error
              info2[:autorizacionPago] == :desconocido -> :error
              info2[:infraccion] == false and info2[:autorizacionPago] == true -> :ok
              true -> :en_proceso
            end

          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, info2})}

          reply =
            case status do
              :ok         -> {:ok, info2}
              :error      -> {:error, info2}
              :en_proceso -> :en_proceso
            end

          {:reply, reply, new_state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end



  # ============== HANDLE CALL AMQP ===================

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
