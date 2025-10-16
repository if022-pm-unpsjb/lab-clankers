defmodule Libremarket.Ui do

  def node_compras(), do: :"compras@equipoalan"

  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      Libremarket.Compras.comprar({id_producto, medio_de_pago, forma_de_entrega}) # Libremarket.Compras.Server.comprar({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

  def comprar_timed(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega}) # Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

end
