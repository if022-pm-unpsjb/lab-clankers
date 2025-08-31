defmodule Libremarket.Compras do
  
  def confirmarCompra(id_compra) do

    compra_confirmada = :rand.uniform(100) < 80

    IO.puts("Compra confirmada: #{compra_confirmada}")

    compra_confirmada
  end
  
  def comprar(id_compra, nro_producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{nro_producto}")

    Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega})

    confirmacion = confirmarCompra(id_compra)
    Libremarket.Ventas.Server.reservarProducto(nro_producto)

    infraccion = Libremarket.Infracciones.Server.detectarInfraccion(id_compra)
    #si hay infraccion
      # Informar infraccion
      #Libremarket.Ventas.Server.liberarProducto(producto)
      # pkill

    autorizacionPago = Libremarket.Pagos.Server.autorizarPago(id_compra)
    #si no se autoriza el pago
      #informar pago rechazado      
      # liberar reserva del producto
      # pkill
    
    #si forma_entrega == :correo
      # agendar envio
      # enviar producto

    # Confirmar compra exitosa
    {:ok, %{nro_producto: nro_producto, medio_pago: medio_pago, forma_entrega: forma_entrega, confirmacion: confirmacion, infraccion: infraccion, autorizacionPago: autorizacionPago}}

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
  def handle_call({:comprar, {nro_producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])

    datos_compra = Libremarket.Compras.comprar(id_compra, nro_producto, medio_pago, forma_entrega)

    new_state = Map.put(state, id_compra, datos_compra)
    {:reply, datos_compra, new_state}
  end

  @impl true
  def handle_call(:listarCompras, _from, state) do
    {:reply, state, state}
  end
end
