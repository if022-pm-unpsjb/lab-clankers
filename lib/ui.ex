defmodule Libremarket.Ui do

  def node_compras(), do: :"compras@equipoalan"



  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()

    if confirma_compra do
      # Generamos el id_compra acá
      id_compra = :erlang.unique_integer([:positive])

      # Llamamos directamente al flujo lógico, sin GenServer
      Libremarket.Compras.comprar(id_compra, id_producto, medio_de_pago, forma_de_entrega)
    else
      {:error, :compra_no_confirmada}
    end
  end

  def comprar_timed(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega}) # Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

end
