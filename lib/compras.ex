defmodule Libremarket.Compras do

  def confirmarCompra() do

    compra_confirmada = :rand.uniform(100) < 80

    IO.puts("Compra confirmada: #{compra_confirmada}")

    compra_confirmada
  end

def comprar(id_compra, id_producto, medio_pago, forma_entrega) do
  IO.puts("Se eligió el producto: #{id_producto}")

  # obtengo el precio del producto desde Ventas.Server
  precio_producto = Libremarket.Ventas.Server.get_precio(id_producto)

  costo_envio = Libremarket.Envios.Server.calcularEnvio({id_compra, forma_entrega})

  estado_reserva = Libremarket.Ventas.Server.reservarProducto(id_producto)

  precio_total = precio_producto + costo_envio

  # base del resultado con todas las claves
  resultado = %{
    id_producto: id_producto,
    medio_pago: medio_pago,
    forma_entrega: forma_entrega,
    precio_producto: precio_producto,
    precio_envio: costo_envio,
    precio_total: precio_total,
    infraccion: nil,
    autorizacionPago: nil
  }

  if estado_reserva == :sin_stock do
    IO.puts("Producto #{id_producto} no disponible. Cancelando compra...")
    {:ok, resultado}

  else
    infraccion = Libremarket.Infracciones.Server.detectarInfraccion(id_compra)

    if infraccion do
      IO.puts("Infracción detectada. Cancelando compra...")
      Libremarket.Ventas.Server.liberarProducto(id_producto)

      {:ok, Map.put(resultado, :infraccion, true)}

    else
      autorizacionPago = Libremarket.Pagos.Server.autorizarPago(id_compra)

      if !autorizacionPago do
        IO.puts("Pago no autorizado. Cancelando compra...")
        Libremarket.Ventas.Server.liberarProducto(id_producto)

        {:ok,
         resultado
         |> Map.put(:infraccion, false)
         |> Map.put(:autorizacionPago, false)}

      else
        if forma_entrega == :correo do
          Libremarket.Envios.Server.agendarEnvio({id_compra, costo_envio})
        end

        {:ok,
         resultado
         |> Map.put(:infraccion, false)
         |> Map.put(:autorizacionPago, true)}
      end
    end
  end
end



end #end def_module

defmodule Libremarket.Compras.Server do
  use GenServer

  # API
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, datos_compra) do
    GenServer.call(pid, {:comprar, datos_compra})
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
  def handle_call(:listarCompras, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:confirmarCompra, _from, state) do
    resultado = Libremarket.Compras.confirmarCompra()
    {:reply, resultado, state}
  end

end
