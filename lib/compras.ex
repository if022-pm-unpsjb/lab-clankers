defmodule Libremarket.Compras do

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





# ============== HANLDE CALL ===================


  @impl true
  def handle_call(:inicializarCompra, _from, state) do
    id_compra = :erlang.unique_integer([:positive])

    datos_compra = {
      :en_proceso,
      %{
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

    {:reply, id_compra, new_state}
  end


  @impl true
  def handle_call({:detectarInfraccion, id_compra}, _from, state) do
    case Map.fetch(state.compras, id_compra) do
      {:ok, {status, compra}} ->
        id_producto = compra[:id_producto]

        case Libremarket.Infracciones.Server.detectarInfraccion(id_compra) do
          true ->
            compra2 = Map.put(compra, :infraccion, true)
            if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:error, {status, compra2}}, new_state}

          false ->
            compra2 = Map.put(compra, :infraccion, false)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:ok, {status, compra2}}, new_state}

          :desconocido ->
            compra2 = Map.put(compra, :infraccion, :desconocido)
            if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:error, {status, compra2}}, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}

          other ->
            {:reply, {:error, other}, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

@impl true
def handle_call({:autorizarPago, id_compra}, _from, state) do
  case Map.fetch(state.compras, id_compra) do
    {:ok, {status, compra}} ->
      id_producto = compra[:id_producto]

      case Libremarket.Pagos.Server.autorizarPago(id_compra) do
        true ->
          compra2 = Map.put(compra, :autorizacionPago, true)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
          {:reply, {:ok, {status, compra2}}, new_state}

        false ->
          compra2 = Map.put(compra, :autorizacionPago, false)
          if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
          {:reply, {:ok, {status, compra2}}, new_state}

        :desconocido ->
          compra2 = Map.put(compra, :autorizacionPago, :desconocido)
          if id_producto, do: Libremarket.Ventas.Server.liberarProducto(id_producto)
          new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
          {:reply, {:ok, {status, compra2}}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}

        other ->
          {:reply, {:error, other}, state}
      end

    :error ->
      {:reply, {:error, :not_found}, state}
  end
end



  @impl true
  def handle_call({:seleccionarProducto, {id_compra, id_producto}}, _from, state) do
    with {:ok, {status, compra}} <- Map.fetch(state.compras, id_compra),
        %{} <- Libremarket.Ventas.Server.reservarProducto(id_producto) do
      precio = Libremarket.Ventas.Server.get_precio(id_producto)

      compra2 =
        compra
        |> Map.put(:id_producto, id_producto)
        |> Map.put(:precio_producto, precio)

      new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
      {:reply, {:ok, {status, compra2}}, new_state}
    else
      :error     -> {:reply, {:error, :not_found}, state}
      :no_existe -> {:reply, {:error, :no_existe}, state}
      :sin_stock -> {:reply, {:error, :sin_stock}, state}
      other      -> {:reply, {:error, other}, state}
    end
  end

  @impl true
  def handle_call({:seleccionarMedioPago, {id_compra, medio_pago}}, _from, state) do
    case Map.fetch(state.compras, id_compra) do
      {:ok, {status, compra}} ->
        compra2 = Map.put(compra, :medio_pago, medio_pago)
        new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
        {:reply, {:ok, {status, compra2}}, new_state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end


  @impl true
  def handle_call({:seleccionarFormaEntrega, {id_compra, forma_entrega}}, _from, state) do
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
            {:reply, {:ok, {status, compra2}}, new_state}

          :retira ->
            compra2 =
              compra
              |> Map.put(:forma_entrega, :retira)
              |> Map.put(:precio_envio, 0)

            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:ok, {status, compra2}}, new_state}

          _ ->
            {:reply, {:error, :forma_entrega_invalida}, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:autorizarPago, id_compra}, _from, state) do
    case Map.fetch(state.compras, id_compra) do
      {:ok, {status, compra}} ->
        case Libremarket.Pagos.Server.autorizarPago(id_compra) do
          true ->
            compra2 = Map.put(compra, :autorizacionPago, true)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:ok, {status, compra2}}, new_state}

          false ->
            compra2 = Map.put(compra, :autorizacionPago, false)
            Libremarket.Ventas.Server.liberarProducto(id_compra)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:ok, {status, compra2}}, new_state}

          :desconocido ->
            compra2 = Map.put(compra, :autorizacionPago, :desconocido)
            Libremarket.Ventas.Server.liberarProducto(id_compra)
            new_state = %{state | compras: Map.put(state.compras, id_compra, {status, compra2})}
            {:reply, {:ok, {status, compra2}}, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}

          other ->
            {:reply, {:error, other}, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:confirmarCompra, _from, state) do
    resultado = Libremarket.Compras.confirmarCompra()
    {:reply, resultado, state}
  end

  @impl true
  def handle_call(:listarCompras, _from, state) do
    # ✅ devolver solo el mapa de compras
    {:reply, state.compras, state}
  end


  @impl true
  def handle_call({:obtenerCompra, id_compra}, _from, state) do
    # ✅ buscar en state.compras
    case Map.fetch(state.compras, id_compra) do
      {:ok, datos} -> {:reply, {:ok, %{id_compra: id_compra, resultado: datos}}, state}
      :error       -> {:reply, {:error, :not_found}, state}
    end
  end
end
