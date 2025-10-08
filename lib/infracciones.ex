defmodule Libremarket.Infracciones do

  def detectarInfraccion() do

    hay_infraccion = :rand.uniform(100) < 30

    IO.puts("Hay infraccion: #{hay_infraccion}")

    hay_infraccion
  end

end

defmodule Libremarket.Infracciones.Server do
  @moduledoc "Infracciones"
  use GenServer
  alias AMQP.{Connection, Channel, Queue, Basic}

  @global_name {:global, __MODULE__}
  @req_q  "infracciones.req"
  @resp_q "compras.resp"

  # ===== API cliente =====
  def start_link(opts \\ %{}), do: GenServer.start_link(__MODULE__, opts, name: @global_name)
  def detectarInfraccion(pid \\ @global_name, id_compra),
    do: GenServer.call(pid, {:detectarInfraccion, id_compra})
  def listarInfracciones(pid \\ @global_name), do: GenServer.call(pid, :listarInfracciones)

  # ===== Callbacks =====
  @impl true
  def init(_state) do
    Process.flag(:trap_exit, true)
    {:ok, chan} = connect_amqp!()
    # Declaración de colas (idempotente)
    Queue.declare(chan, @req_q,  durable: false)
    Queue.declare(chan, @resp_q, durable: false)
    Basic.qos(chan, prefetch_count: 16)
    # Consumir pedidos
    {:ok, _ctag} = Basic.consume(chan, @req_q, nil, no_ack: false)
    {:ok, %{chan: chan, compras_infracciones: %{}}}
  end

  @impl true
  def handle_call({:detectarInfraccion, id_compra}, _from, state) do
    infr = Libremarket.Infracciones.detectarInfraccion()
    new_state = put_in(state[:compras_infracciones][id_compra], infr)
    {:reply, infr, new_state}
  end

  @impl true
  def handle_call(:listarInfracciones, _from, state) do
    {:reply, state.compras_infracciones, state}
  end

  # === Mensajes AMQP entrantes: pedidos desde Compras ===
  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = state) do
    %AMQP.Basic.Deliver{delivery_tag: tag, correlation_id: cid} = meta
    spawn(fn ->
      resp_payload =
        case Jason.decode(payload) do
          {:ok, %{"id_compra" => id}} ->
            infr = Libremarket.Infracciones.detectarInfraccion()
            Jason.encode!(%{id_compra: id, infraccion: infr})
          _ ->
            Jason.encode!(%{error: "bad_request"})
        end

      # Responder en la cola de respuestas de Compras usando el mismo correlation_id
      Basic.publish(chan, "", @resp_q, resp_payload,
        correlation_id: cid, content_type: "application/json"
      )

      Basic.ack(chan, tag)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _mref, :process, _pid, _reason}, _state) do
    {:ok, chan} = reconnect_amqp!()
    {:noreply, %{chan: chan, compras_infracciones: %{}}}
  end

  # ===== Helpers conexión =====
  defp connect_amqp!() do
    url = System.fetch_env!("AMQP_URL")
    {:ok, conn} = Connection.open(url)
    Process.monitor(conn.pid)
    Channel.open(conn)
  end

  defp reconnect_amqp!() do
    :timer.sleep(1000)
    connect_amqp!()
  end
end
