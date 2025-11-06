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

end





defmodule Libremarket.Compras.Server do
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}

  @req_infr_q  "infracciones.req"
  @req_pago_q  "pagos.req"
  @req_ventas_q "ventas.req"
  @req_env_q   "envios.req"
  @resp_q      "compras.resp"

  @required_for_close [:precio_producto, :precio_envio, :autorizacionPago, :id_producto, :infraccion, :forma_entrega, :medio_pago]

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

  # # 👇 aridad 3 por si alguna vez querés pasar un pid/nombre distinto
  # def detectar_infraccion_amqp(pid, id_compra, timeout_ms) do
  #   GenServer.cast(pid, {:rpc_infraccion_async, id_compra, timeout_ms})
  # end


  def autorizar_pago_amqp(id_compra, timeout_ms \\ 5_000),
    do: GenServer.cast(@global_name, {:rpc_pago_async, id_compra, timeout_ms})

  # def autorizar_pago_amqp(pid, id_compra, timeout_ms),
  #   do: GenServer.cast(pid, {:rpc_pago_async, id_compra, timeout_ms})


  def reservar_producto_amqp(id_compra, id_producto, timeout_ms \\ 5_000) do
    GenServer.cast(@global_name, {:rpc_reserva_async, id_compra, id_producto, timeout_ms})
  end

  # def reservar_producto_amqp(pid, id_compra, id_producto, timeout_ms) do
  #   GenServer.cast(pid, {:rpc_reserva_async, id_compra, id_producto, timeout_ms})
  # end

    # Publicar "calcular" (correo o retira)
  def calcular_envio_amqp(id_compra, forma_entrega, timeout_ms \\ 5_000),
    do: GenServer.cast(@global_name, {:rpc_envio_calcular_async, id_compra, forma_entrega, timeout_ms})

  # Publicar "agendar"
  def agendar_envio_amqp(id_compra, costo, timeout_ms \\ 5_000),
    do: GenServer.cast(@global_name, {:rpc_envio_agendar_async, id_compra, costo, timeout_ms})


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
      {:ok, {status, info}} ->
        {:reply, {:ok, {status, info}}, state}

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
    Queue.declare(chan, @req_infr_q,  durable: false)
    Queue.declare(chan, @req_pago_q, durable: false)
    Queue.declare(chan, @resp_q, durable: false)
    Queue.declare(chan, @req_env_q,   durable: false)
    Queue.declare(chan, @req_ventas_q, durable: false)

    # Consumir respuestas (no_ack: true porque solo reenviamos al cliente esperando)
    {:ok, _ctag} = Basic.consume(chan, @resp_q, nil, no_ack: true)

    # Estado: canal amqp + mapa de esperas por correlation_id + compras
    {:ok, %{chan: chan, waiting: %{}, compras: %{}, pending_infraccion: %{}, pending_pago: %{}, pending_reserva: %{}, pending_envio: %{}}}
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
        {s3, _} = finalize_if_ready(s2, id)
        {:noreply, s3}

      # pagos
      {:ok, %{"id_compra" => id, "autorizacionPago" => ok?}} ->
        {s2, _} = actualizar_compra_por_pago(s, id, ok?)
        {s3, _} = finalize_if_ready(s2, id)
        {:noreply, s3}

      # reserva OK
      {:ok, %{"type" => "reservar_res", "id_compra" => id, "id_producto" => _idp, "result" => "ok", "precio" => precio}} ->
        {s2, _} = aplicar_reserva_ok(s, id, precio)
        {s3, _} = finalize_if_ready(s2, id)
        {:noreply, s3}

      # reserva errores
      {:ok, %{"type" => "reservar_res", "id_compra" => id, "result" => "sin_stock"}} ->
        {s2, _} = aplicar_reserva_fail(s, id, :sin_stock)
        {s3, _} = finalize_if_ready(s2, id)
        {:noreply, s3}

      {:ok, %{"type" => "reservar_res", "id_compra" => id, "result" => "no_existe"}} ->
        {s2, _} = aplicar_reserva_fail(s, id, :no_existe)
        {s3, _} = finalize_if_ready(s2, id)
        {:noreply, s3}

      # calcular envío
      {:ok, %{"id_compra" => id, "precio_envio" => costo}} ->
        {s2, _} = actualizar_compra_por_envio(s, id, costo)
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


  defp finalize_if_ready(%{compras: compras} = s, id_compra) do
    case Map.fetch(compras, id_compra) do
      :error ->
        {s, false}

      # Si ya quedó cerrada (ok/error), no tocar
      {:ok, {status, _compra}} when status in [:ok, :error] ->
        {s, false}

      {:ok, {status, compra}} ->
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

          compras2 = Map.put(compras, id_compra, {new_status, compra2})
          { %{s | compras: compras2}, new_status in [:ok, :error] }
        end
    end
  end

end
