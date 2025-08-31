defmodule Libremarket.Compras do
  
  def confirmarCompra(id_compra) do

    compra_confirmada = :rand.uniform(100) < 80

    IO.puts("Compra confirmada: #{compra_confirmada}")

    compra_confirmada
  end
  
  def comprar(id_compra, id_producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{id_producto}")

    costo = Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega})

    confirmacion = confirmarCompra(id_compra)
    
    estado_reserva = Libremarket.Ventas.Server.reservarProducto(id_producto)
    if estado_reserva == :sin_stock do
      IO.puts("Producto #{id_producto} no disponible. Cancelando compra...") 
      # no liberamos el producto porque nunca se reservó
      # pkill
    end

    infraccion = Libremarket.Infracciones.Server.detectarInfraccion(id_compra)

    if infraccion do
      IO.puts("Infraccion detectada. Cancelando compra...")
      Libremarket.Ventas.Server.liberarProducto(id_producto)
      # pkill
    end

    autorizacionPago = Libremarket.Pagos.Server.autorizarPago(id_compra)
    if !autorizacionPago do
      IO.puts("Pago no autorizado. Cancelando compra...")
      Libremarket.Ventas.Server.liberarProducto(id_producto)
      # pkill
    end

    if forma_entrega == :correo do
      Libremarket.Envios.Server.agendarEnvio({id_compra, costo})
      # enviar producto (preguntar)
    end

    {:ok, %{id_producto: id_producto, medio_pago: medio_pago, forma_entrega: forma_entrega, confirmacion: confirmacion, infraccion: infraccion, autorizacionPago: autorizacionPago}}

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
  def handle_call(:listarCompras, _from, state) do
    {:reply, state, state}
  end
end
