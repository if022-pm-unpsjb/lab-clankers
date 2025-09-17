defmodule Libremarket.Ui do

  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      :erpc.call(:"compras@equipoalan", Libremarket.Compras.Server, :comprar, [{id_producto, medio_de_pago, forma_de_entrega}]) # Libremarket.Compras.Server.comprar({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

  def comprar_timed(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      :erpc.call(:"compras@equipoalan", Libremarket.Compras.Server, :comprar_timed, [id_producto, medio_de_pago, forma_de_entrega]) # Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

end
