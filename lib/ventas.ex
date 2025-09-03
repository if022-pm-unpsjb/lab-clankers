defmodule Libremarket.Ventas do
  def reservarProducto(id_producto, map_productos) do
    case Map.get(map_productos, id_producto) do
      nil ->
        IO.puts("Producto #{id_producto} no existe.")
        :no_existe

      %{stock: 0} = producto ->
        IO.puts("Producto #{id_producto} sin stock.")
        :sin_stock

      %{stock: stock} = producto ->
        nuevo_producto = %{producto | stock: stock - 1}
        nuevo_state = Map.put(map_productos, id_producto, nuevo_producto)

        IO.puts(
          "Producto #{id_producto} reservado. Stock restante: #{nuevo_producto.stock}"
        )

        nuevo_state
    end
  end

  def liberarProducto(id_producto, map_productos) do
    case Map.get(map_productos, id_producto) do
      nil ->
        IO.puts("Producto #{id_producto} no existe.")
        map_productos

      %{stock: stock} = producto ->
        nuevo_producto = %{producto | stock: stock + 1}
        nuevo_state = Map.put(map_productos, id_producto, nuevo_producto)

        IO.puts("Producto #{id_producto} liberado. Stock actual: #{nuevo_producto.stock}")

        nuevo_state
    end
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

  def get_precio(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:get_precio, id_producto})
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(_state) do
    productos =
      1..10
      |> Enum.map(fn id ->
        {id,
         %{
           precio: :rand.uniform(1000),
           stock: :rand.uniform(10)
         }}
      end)
      |> Enum.into(%{})

    {:ok, productos}
  end

  @impl true
  def handle_call({:reservarProducto, id_producto}, _from, state) do
    case Libremarket.Ventas.reservarProducto(id_producto, state) do
      :no_existe ->
        {:reply, :no_existe, state}

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

  @impl true
  def handle_call({:get_precio, id_producto}, _from, state) do
    case Map.get(state, id_producto) do
      nil ->
        {:reply, 0, state}  # si no existe, devuelvo 0 o podés tirar un error
      %{precio: precio} ->
        {:reply, precio, state}
    end
  end

end
