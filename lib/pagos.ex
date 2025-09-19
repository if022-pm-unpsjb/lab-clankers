defmodule Libremarket.Pagos do

  def autorizarPago() do

  pago_autorizado = :rand.uniform(100) < 70

  IO.puts("Pago autorizado: #{pago_autorizado}")

  pago_autorizado
  end

end

defmodule Libremarket.Pagos.Server do
  @moduledoc """
  Pagos
  """

  use GenServer

  @global_name {:global, __MODULE__}

  # API del cliente

  @doc """
  Crea un nuevo servidor de Pagos
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def autorizarPago(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:autorizarPago, id_compra})
  end

  def listarPagos(pid \\ @global_name) do
    GenServer.call(pid, :listarPagos)
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
  Callback para un call :autorizarPago
  """
  @impl true
  def handle_call({:autorizarPago, id_compra}, _from, state) do
    autorizacion = Libremarket.Pagos.autorizarPago()
    new_state = Map.put(state, id_compra, autorizacion)
    {:reply, autorizacion, new_state}
  end

  @impl true
  def handle_call(:listarPagos, _from, state) do
    {:reply, state, state}
  end



end
