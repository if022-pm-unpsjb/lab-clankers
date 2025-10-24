defmodule Libremarket.Ui do
  @default_max_intentos 25
  @default_delay_ms 200

  def comprar(id_producto, medio_de_pago, forma_de_entrega) do
    if Libremarket.Compras.Server.confirmarCompra() do
      id_compra = Libremarket.Compras.Server.inicializarCompra()

      with  {:ok, _} <- Libremarket.Compras.Server.seleccionarProducto({id_compra, id_producto}),
            {:ok, _} <- Libremarket.Compras.Server.seleccionarMedioPago({id_compra, medio_de_pago}),
            {:ok, _} <- Libremarket.Compras.Server.seleccionarFormaEntrega({id_compra, forma_de_entrega}),
            {:ok, _} <- Libremarket.Compras.Server.detectarInfraccion(id_compra),
            {:ok, _} <- Libremarket.Compras.Server.autorizarPago(id_compra)
      do
        obtenerCompra(id_compra, @default_max_intentos, @default_delay_ms)
      else
        {:error, razon} -> {:error, razon}
        otro            -> {:error, {:unexpected_return, otro}}
      end
    else
      {:error, :compra_no_confirmada}
    end
  end

  # Reintenta si el estado es :en_proceso (timeout total = max_intentos * delay_ms)
  # Nuevo contrato: Libremarket.Compras.Server.obtenerCompra/1 -> {:ok, {status, info}} | {:error, :not_found}
  defp obtenerCompra(id_compra, max_intentos, delay_ms) when max_intentos >= 0 do
    case Libremarket.Compras.Server.obtenerCompra(id_compra) do
      {:ok, {:ok, info}} ->
        {:ok, info}

      {:ok, {:error, info}} ->
        {:error, info}

      {:ok, {:en_proceso, _info}} ->
        if max_intentos == 0 do
          {:error, :timeout_compra}
        else
          Process.sleep(delay_ms)
          obtenerCompra(id_compra, max_intentos - 1, delay_ms)
        end

      {:error, :not_found} ->
        {:error, :not_found}

      otro ->
        {:error, {:unexpected_return, otro}}
    end
  end

  defp obtenerCompra(_id_compra, _max_intentos, _delay_ms),
    do: {:error, :timeout_compra}
  end
