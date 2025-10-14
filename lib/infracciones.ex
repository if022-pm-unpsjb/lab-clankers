defmodule Libremarket.Infracciones do
  @moduledoc "Lógica de infracciones"

  @doc "Retorna true ~30% de las veces; usa el id_compra solo para trazas si querés."
  def detectar_infraccion(_id_compra) do
    infraccion? = :rand.uniform(100) <= 30
    IO.puts("Hay infracción: #{infraccion?}")
    infraccion?
  end
end


defmodule Libremarket.Infracciones.Server do
  @moduledoc "Infracciones"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}
  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  # ===== API cliente =====
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def detectarInfraccion(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:detectarInfraccion, id_compra})

  def listarInfracciones(pid \\ @global_name),
    do: GenServer.call(pid, :listarInfracciones)

  # ===== Callbacks =====
  @impl true
  def init(_opts) do
    {:ok, chan} = connect_amqp!()

    # Declaramos colas (idempotente)
    {:ok, _} = Queue.declare(chan, @req_q,  durable: false)
    {:ok, _} = Queue.declare(chan, @resp_q, durable: false)

    # Consumimos pedidos que llegan a infracciones.req
    {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: true)

    {:ok, %{chan: chan}}
  end

  # Llega un mensaje con el pedido: {:basic_deliver, payload, meta}
  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = state)
      when is_binary(payload) and is_map(meta) do
    # meta.correlation_id lo re-enviamos tal cual
    cid = Map.get(meta, :correlation_id)

    case Jason.decode(payload) do
      {:ok, %{"id_compra" => id_compra}} ->
        infr = Libremarket.Infracciones.detectar_infraccion(id_compra)
        resp = Jason.encode!(%{id_compra: id_compra, infraccion: infr})

        Basic.publish(
          chan, "", @resp_q, resp,
          correlation_id: cid,
          content_type: "application/json"
        )

      other ->
        Logger.warn("Payload inválido en #{@req_q}: #{inspect(other)}")
    end

    {:noreply, state}
  end

  # Mensajes administrativos del consumer
  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: _}}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_cancel, _}, state), do: {:stop, :normal, state}

  @impl true
  def handle_info({:basic_cancel_ok, _}, state), do: {:noreply, state}

  # Fallback para no romper si llega otra cosa
  @impl true
  def handle_info(msg, state) do
    Logger.debug("Infracciones.Server ignoró: #{inspect(msg)}")
    {:noreply, state}
  end

  # ===== Helpers AMQP =====
  defp connect_amqp!() do
    url = System.fetch_env!("AMQP_URL")
    with {:ok, conn} <- Connection.open(url),
         {:ok, chan} <- Channel.open(conn) do
      {:ok, chan}
    else
      error ->
        raise "No se pudo conectar a AMQP: #{inspect(error)}"
    end
  end
end
