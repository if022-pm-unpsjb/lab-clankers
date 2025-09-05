defmodule Simulador do

  def simular_compra() do
    envio = Enum.random([:retira, :correo])
    pago = Enum.random([:efectivo, :transferencia, :td, :tc])
    id_producto = :rand.uniform(10)
    Libremarket.Ui.comprar(id_producto, pago, envio)
  end

  def simular_compra_timed() do
    envio = Enum.random([:retira, :correo])
    pago = Enum.random([:efectivo, :transferencia, :td, :tc])
    id_producto = :rand.uniform(10)
    Libremarket.Ui.comprar_timed(id_producto, pago, envio)
  end

  def simular_compras_secuencial(cantidad \\ 1) do
    for _n <- 1 .. cantidad do
      simular_compra()
    end
  end

  def simular_compras_async(cantidad \\ 1) do
    compras = for _n <- 1 .. cantidad do
      Task.async(fn -> simular_compra() end)
    end
    Task.await_many(compras)
  end

  def simular_compras_async_wait(cantidad \\ 1) do
    compras = for _n <- 1 .. cantidad do
      Task.async(fn -> simular_compra_timed() end)
    end
    Task.await_many(compras, 15_000)
  end

end
