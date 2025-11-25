defmodule Libremarket.Supervisor do
  use Supervisor

  @doc """
  Inicia el supervisor
  """
  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Módulo principal (Server) a arrancar, pasado por env SERVER_TO_RUN
    server_children =
      case System.get_env("SERVER_TO_RUN") do
        nil ->
          []

        server_to_run ->
          [{String.to_existing_atom(server_to_run), %{}}]
      end

    # Módulo de Leader a arrancar, pasado por env LEADER_TO_RUN
    leader_children =
      case System.get_env("LEADER_TO_RUN") do
        nil ->
          []

        leader_to_run ->
          [{String.to_existing_atom(leader_to_run), %{}}]
      end

    topologies = [
      gossip: [
        strategy: Cluster.Strategy.Gossip,
        config: [
          port: 45892,
          if_addr: "0.0.0.0",
          multicast_addr: "172.21.255.255",
          broadcast_only: true,
          secret: "secret"
        ]
      ]
    ]

    children =
      [
        {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]}
      ] ++ leader_children ++ server_children

    Supervisor.init(children, strategy: :one_for_one)
  end
end
