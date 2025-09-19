defmodule Libremarket.Envios do

def calcularEnvio(:retira) do
  IO.puts("Forma de entrega: retira")
  0
end

def calcularEnvio(:correo) do
  IO.puts("Forma de entrega: correo")
  costo_envio = :rand.uniform(10000)
  IO.puts("Costo del envio: #{costo_envio}")
  costo_envio
end

end

defmodule Libremarket.Envios.Server do
  @moduledoc """
  Envios
  """

  use GenServer

  @global_name {:global, __MODULE__}

  # API del cliente

  @doc """
  Crea un nuevo servidor de Envios
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def calcularEnvio(pid \\ @global_name, datos_compra) do
    GenServer.call(pid, {:calcularEnvio, datos_compra})
  end

  def listarEnvios(pid \\ @global_name) do
    GenServer.call(pid, :listarEnvios)
  end

  def agendarEnvio(pid \\ @global_name, datos_compra) do
    GenServer.call(pid, {:agendarEnvio, datos_compra})
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
  def handle_call({:calcularEnvio, {id_compra, :correo}}, _from, state) do
    costo = Libremarket.Envios.calcularEnvio(:correo)
    {:reply, costo, state}
  end

  @impl true
  def handle_call({:calcularEnvio, {id_compra, :retira}}, _from, state) do
    {:reply, 0, state}
  end

  @impl true
  def handle_call({:agendarEnvio, {id_compra, costo}}, _from, state) do
    id_envio = :erlang.unique_integer([:positive])

    new_state = Map.put(state, id_envio, %{id_compra: id_compra, costo: costo})

    {:reply, new_state, new_state}
  end

  @impl true
  def handle_call(:listarEnvios, _from, state) do
    {:reply, state, state}
  end

end
