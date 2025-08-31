defmodule Libremarket.Compras do

  def comprar(id_compra) do
    Libremarket.Infracciones.Server.detectarInfraccion(id_compra)
    Libremarket.Pagos.Server.autorizarPago(id_compra)
  end

end

defmodule Libremarket.Compras.Server do
  @moduledoc """
  Compras
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Compras
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:comprar, id_compra})
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, %{}}
  end

  @doc """
  Callback para un call :comprar
  """
  @impl true
  def handle_call({:comprar, id_compra}, _from, state) do
    compra = Libremarket.Compras.comprar(id_compra)
    new_state = Map.put(state, id_compra, compra)
    {:reply, compra, new_state}
  end

end
