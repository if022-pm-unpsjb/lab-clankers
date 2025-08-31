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

  # API del cliente

  @doc """
  Crea un nuevo servidor de Envios
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def calcularEnvio(pid \\ __MODULE__, forma_entrega) do
    GenServer.call(pid, {:calcularEnvio, forma_entrega})
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
  def handle_call({:calcularEnvio, forma_entrega}, _from, state) do
    envio = Libremarket.Envios.calcularEnvio(forma_entrega)
    new_state = Map.put(state, forma_entrega, envio)
    {:reply, envio, new_state}
  end

end
