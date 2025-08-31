defmodule Libremarket.Ui do

  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    Libremarket.Compras.Server.comprar(id_producto, medio_de_pago, forma_de_entrega)
  end

end
