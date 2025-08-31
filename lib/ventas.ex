defmodule Libremarket.Ventas do

  def reservarProducto(id_producto, map_productos) do
    case Map.get(map_productos, id_producto) do
      0 ->
        IO.puts("Producto #{id_producto} sin stock.--------------------------------------------------")
        :sin_stock

      stock ->
        nuevo_state = Map.update!(map_productos, id_producto, &(&1 - 1))
        IO.puts("Producto #{id_producto} reservado. Stock restante: #{nuevo_state[id_producto]}")
        nuevo_state
    end
  end

  def liberarProducto(id_producto, state) do
    new_state = Map.update!(state, id_producto, &(&1 + 1))
    IO.puts("Producto #{id_producto} liberado. Stock actual: #{new_state[id_producto]}")
    new_state
  end

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

  def liberarProducto(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:liberarProducto, id_producto})
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
  def handle_call({:reservarProducto, id_producto}, _from, state) do
    case Libremarket.Ventas.reservarProducto(id_producto, state) do
      :sin_stock ->
        {:reply, :sin_stock, state}

      new_state ->
        {:reply, new_state, new_state}
    end
  end

    @impl true
  def handle_call({:liberarProducto, id_producto}, _from, state) do
    new_state = Libremarket.Ventas.liberarProducto(id_producto, state)
    {:reply, new_state, new_state}
  end

  @impl true
  def handle_call(:listarProductos, _from, state) do
    {:reply, state, state}
  end
end
