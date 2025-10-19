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
        case Libremarket.Pagos.Server.autorizarPago(id_compra) do
          true ->
            compra2   = Map.put(compra, :autorizacionPago, true)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {{:ok, {status, compra2}}, new_state}

          false ->
            compra2 = Map.put(compra, :autorizacionPago, false)
            if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            # seguimos devolviendo :ok para que el flujo continúe hasta obtenerCompra/2
            {{:ok, {status, compra2}}, new_state}

          :desconocido ->
            compra2 = Map.put(compra, :autorizacionPago, :desconocido)
            if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {{:ok, {status, compra2}}, new_state}

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

# 👇 aridad 2 (la que vas a llamar desde Compras.detectar_infraccion_state/2)
  def detectar_infraccion_amqp(id_compra, timeout_ms \\ 5_000) do
    GenServer.cast(@global_name, {:rpc_infraccion_async, id_compra, timeout_ms})
  end

  # 👇 aridad 3 por si alguna vez querés pasar un pid/nombre distinto
  def detectar_infraccion_amqp(pid, id_compra, timeout_ms) do
    GenServer.cast(pid, {:rpc_infraccion_async, id_compra, timeout_ms})
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
    {:ok, %{chan: chan, waiting: %{}, compras: %{}, pending_infraccion: %{}}}
  end


  @impl true
  def handle_cast({:rpc_infraccion_async, id_compra, timeout_ms}, %{chan: chan, pending_infraccion: pend} = s) do
    payload = Jason.encode!(%{id_compra: id_compra})

    Basic.publish(
      chan, "", @req_q, payload,
      content_type: "application/json"
    )

    tref = Process.send_after(self(), {:rpc_infraccion_timeout, id_compra}, timeout_ms)
    {:noreply, %{s | pending_infraccion: Map.put(pend, id_compra, tref)}}
  end


  @impl true
  def handle_info({:basic_deliver, payload, %{correlation_id: cid}}, s) when not is_nil(cid) do
    case Map.pop(s.waiting, cid) do
      {nil, _} ->
        # No era un RPC síncrono pendiente: procesamos como asíncrono por id_compra
        case Jason.decode(payload) do
          {:ok, %{"id_compra" => id, "infraccion" => infr}} ->
            {s2, _} = actualizar_compra_por_infraccion(s, id, infr)
            {:noreply, s2}

          _ ->
            {:noreply, s}
        end

      {{from, tref}, waiting2} ->
        Process.cancel_timer(tref)
        reply =
          case Jason.decode(payload) do
            {:ok, %{"id_compra" => id, "infraccion" => infr}} -> {:ok, %{id_compra: id, infraccion: infr}}
            _ -> {:error, :bad_payload}
          end

        GenServer.reply(from, reply)
        {:noreply, %{s | waiting: waiting2}}
    end
  end

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, s) do
    # Camino asíncrono (sin correlation_id): enrutamos por id_compra del payload
    case Jason.decode(payload) do
      {:ok, %{"id_compra" => id, "infraccion" => infr}} ->
        {s2, _} = actualizar_compra_por_infraccion(s, id, infr)
        {:noreply, s2}

      _ ->
        {:noreply, s}
    end
  end

  @impl true
  def handle_info({:rpc_infraccion_timeout, id_compra}, %{pending_infraccion: pend} = s) do
    case Map.pop(pend, id_compra) do
      {nil, _} ->
        {:noreply, s} # ya llegó la respuesta, nada que hacer
      {_tref, pend2} ->
        {s2, _} = actualizar_compra_por_infraccion(s, id_compra, :desconocido)
        {:noreply, %{s2 | pending_infraccion: pend2}}
    end
  end


  defp actualizar_compra_por_infraccion(%{compras: compras} = s, id_compra, infr) do
    case Map.fetch(compras, id_compra) do
      :error ->
        {s, false}

      # Si ya quedó :ok (decidido por obtenerCompra/2), ignoramos una infracción tardía
      {:ok, {:ok, _compra}} ->
        s2 = cancel_pending_infraccion_timer(s, id_compra)
        {s2, false}

      {:ok, {status, compra}} ->
        compra1 = Map.put(compra, :infraccion, infr)

        # Reglas de negocio inmediatas (sin cambiar status):
        if infr in [true, :desconocido] do
          if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
        end

        compras2 = Map.put(compras, id_compra, {status, compra1})
        s2 = cancel_pending_infraccion_timer(%{s | compras: compras2}, id_compra)
        {s2, true}
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



  # defp actualizar_compra_por_infraccion(%{compras: compras, pending_infraccion: pend} = s, id_compra, infr) do
  #   case Map.fetch(compras, id_compra) do
  #     :error ->
  #       {s, false}

  #     {:ok, {_status, compra}} ->
  #       compra1 = Map.put(compra, :infraccion, infr)

  #       {compra2, status2} =
  #         case infr do
  #           true ->
  #             if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
  #             {compra1, :error}

  #           false ->
  #             {compra1, :en_proceso}

  #           :desconocido ->
  #             if compra[:id_producto], do: Libremarket.Ventas.Server.liberarProducto(compra[:id_producto])
  #             {compra1, :error}
  #         end

  #       compra3 =
  #         compra2
  #         |> Map.put(:precio_total, (compra2[:precio_producto] || 0) + (compra2[:precio_envio] || 0))

  #       compras2 = Map.put(compras, id_compra, {status2, compra3})

  #       # cancelar timeout si existía
  #       s2 =
  #         case Map.pop(pend, id_compra) do
  #           {nil, _} -> s
  #           {tref, pend2} ->
  #             Process.cancel_timer(tref)
  #             %{s | pending_infraccion: pend2}
  #         end

  #       {%{s2 | compras: compras2}, true}
  #   end
  # end



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
