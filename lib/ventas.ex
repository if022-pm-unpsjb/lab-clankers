defmodule Libremarket.Ventas do

end

defmodule Libremarket.Ventas.Server do
  @moduledoc """
  Ventas
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Ventas
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end


  def listarProductos(pid \\ __MODULE__) do
    GenServer.call(pid, :listarProductos)
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
  productos =
    1..10
    |> Enum.map(fn id -> {id, :rand.uniform(10)} end)
    |> Enum.into(%{})

  {:ok, productos}
  end


  @impl true
  def handle_call(:listarProductos, _from, state) do
    {:reply, state, state}
  end
end
