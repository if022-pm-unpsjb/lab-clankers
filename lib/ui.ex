defmodule Libremarket.Ui do

 def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    if Libremarket.Compras.Server.confirmarCompra() do
      id_compra = Libremarket.Compras.Server.inicializarCompra()

      with  {:ok, _} <- Libremarket.Compras.Server.seleccionarProducto(id_compra, id_producto),
            {:ok, _} <- Libremarket.Compras.Server.seleccionarMedioPago(id_compra, medio_de_pago),
            {:ok, _} <- Libremarket.Compras.Server.seleccionarFormaEntrega(id_compra, forma_de_entrega)
      do
        with
          {:ok, _} <- Libremarket.Compras.Server.verificarInfraccion(id_compra)
          {:ok, _} <- Libremarket.Compras.Server.autorizarPago(id_compra)
        do
          funcionfina(id_compra, 25, 200)
        else
          {:error, razon} -> {:error, razon}
          otro -> {:error, {:unexpected_return, otro}}
        end
      else
        {:error, razon} -> {:error, razon}
        otro -> {:error, {:unexpected_return, otro}}
      end
    else
      {:error, :compra_no_confirmada}
    end
  end

# Reintenta si recibe :en_proceso (timeout total = max_intentos * delay_ms)
  defp funcionfinal(id_compra, max_intentos, delay_ms) when max_intentos >= 0 do
    case Libremarket.Compras.comprar.(id_compra) do
      {:ok, compra}   -> {:ok, compra}
      {:error, razon} -> {:error, razon}
      :en_proceso     ->
        if max_intentos == 0, do: {:error, :timeout_compra}, else: (
          Process.sleep(delay_ms)
          await_compra(id_compra, fun, max_intentos - 1, delay_ms)
        )
      otro -> {:error, {:unexpected_return, otro}}
    end
  end


  def comprar_timed(id_producto, medio_de_pago, forma_de_entrega) do
    confirma_compra = Libremarket.Compras.Server.confirmarCompra()
    if(confirma_compra) do
      Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega}) # Libremarket.Compras.Server.comprar_timed({id_producto, medio_de_pago, forma_de_entrega})
    end
  end

end
