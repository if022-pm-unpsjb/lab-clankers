defmodule Libremarket.Compras do
  # ==== API principal ====
  def comprar(id_compra, id_producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{id_producto}")

    # Paso 1: inicialización de datos de compra
    resultado_base = inicializar_compra(id_compra, id_producto, medio_pago, forma_entrega)

    # Paso 2: reservar producto
    case Libremarket.Ventas.Server.reservarProducto(id_producto) do
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
            {:ok, autorizar_pago(id_compra, id_producto, forma_entrega, resultado_infraccion)}
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
    case Libremarket.Ventas.Server.reservarProducto(id_producto) do
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
            {:ok, autorizar_pago(id_compra, id_producto, forma_entrega, resultado_infraccion)}
        end
    end
  end

  # ==== Paso 1: inicialización ====
  defp inicializar_compra(id_compra, id_producto, medio_pago, forma_entrega) do
    precio_producto = Libremarket.Ventas.Server.get_precio(id_producto)
    costo_envio = Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega})
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
    infraccion = Libremarket.Infracciones.Server.detectarInfraccion(id_compra)

    if infraccion do
      IO.puts("Infracción detectada. Cancelando compra...")
      Libremarket.Ventas.Server.liberarProducto(id_producto)
      {:error, Map.put(resultado, :infraccion, true)}
    else
      {:ok, Map.put(resultado, :infraccion, false)}
    end
  end

  # ==== Paso 3: autorización de pago ====
  defp autorizar_pago(id_compra, id_producto, forma_entrega, resultado) do
    autorizacionPago = Libremarket.Pagos.Server.autorizarPago(id_compra)

    cond do
      autorizacionPago == false ->
        IO.puts("Pago no autorizado. Cancelando compra...")
        Libremarket.Ventas.Server.liberarProducto(id_producto)
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

  # API
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, datos_compra) do
    GenServer.call(pid, {:comprar, datos_compra})
  end

  def comprar_timed(pid \\ __MODULE__, datos_compra) do
    GenServer.call(pid, {:comprar_timed, datos_compra}, 15_000)
  end

  def confirmarCompra(pid \\ __MODULE__) do
    GenServer.call(pid, :confirmarCompra)
  end

  def listarCompras(pid \\ __MODULE__) do
    GenServer.call(pid, :listarCompras)
  end

  # Callbacks
  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:comprar, {id_producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])

    datos_compra = Libremarket.Compras.comprar(id_compra, id_producto, medio_pago, forma_entrega)

    new_state = Map.put(state, id_compra, datos_compra)
    {:reply, datos_compra, new_state}
  end

  @impl true
  def handle_call({:comprar_timed, {id_producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])

    datos_compra = Libremarket.Compras.comprar_timed(id_compra, id_producto, medio_pago, forma_entrega)

    new_state = Map.put(state, id_compra, datos_compra)
    {:reply, datos_compra, new_state}
  end

  @impl true
  def handle_call(:listarCompras, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:confirmarCompra, _from, state) do
    resultado = Libremarket.Compras.confirmarCompra()
    {:reply, resultado, state}
  end

end
