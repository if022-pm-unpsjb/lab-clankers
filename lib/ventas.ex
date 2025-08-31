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

  def reservarProducto(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:reservarProducto, id_producto})
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

  IO.puts("Productos iniciales: #{inspect(productos)}")
  {:ok, productos}
  end

 @impl true
  def handle_call({:reservarProducto, id_producto}, _from, state) do
    case Map.get(state, id_producto) do
      0 ->
        {:reply, {:error, :sin_stock}, state}

      stock ->
        nuevo_state = Map.update!(state, id_producto, &(&1 - 1))
        IO.puts("Producto #{id_producto} reservado. Stock restante: #{nuevo_state[id_producto]}")
        {:reply, {:ok, nuevo_state}, nuevo_state}
    end
  end

  @impl true
  def handle_call(:listarProductos, _from, state) do
    {:reply, state, state}
  end
end
