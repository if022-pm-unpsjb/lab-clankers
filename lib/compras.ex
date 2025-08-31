defmodule Libremarket.Compras do
  def comprar(id_compra, producto, medio_pago, forma_entrega) do
    IO.puts("Se eligió el producto: #{producto}")

    Libremarket.Envios.Server.calcularEnvio(forma_entrega)
    ## Libremarket.Compras.Server.confirmarCompra(id_compra)
    ## Libremarket.Ventas.Server.reservarProducto(producto) # agregar PRINT cuando se reserve un producto en reservarProducto(producto)!

    Libremarket.Infracciones.Server.detectarInfraccion(id_compra)

    #si hay infraccion
    #Libremarket.Ventas.Server.liberarProducto(producto)
    Libremarket.Pagos.Server.autorizarPago(id_compra)

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

  # Callbacks
  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:comprar, {producto, medio_pago, forma_entrega}}, _from, state) do
    id_compra = :erlang.unique_integer([:positive])

    compra = Libremarket.Compras.comprar(id_compra, producto, medio_pago, forma_entrega)

    new_state = Map.put(state, id_compra, compra)
    {:reply, compra, new_state}
  end
end
