defmodule Libremarket.Infracciones do

  def detectarInfraccion() do

    hay_infraccion = :rand.uniform(100) > 50

    IO.puts("Hay infraccion: #{hay_infraccion}")

    hay_infraccion
  end

end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectarInfraccion(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:detectarInfraccion, id_compra})
  end

  def listarInfracciones(pid \\ __MODULE__) do
    GenServer.call(pid, :listarInfracciones)
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
  Callback para un call :detectarInfraccion
  """
  @impl true
  def handle_call({:detectarInfraccion, id_compra}, _from, state) do
    infraccion = Libremarket.Infracciones.detectarInfraccion()
    new_state = Map.put(state, id_compra, infraccion)
    {:reply, infraccion, new_state}
  end

  @impl true
  def handle_call(:listarInfracciones, _from, state) do
    {:reply, state, state}
  end

end
