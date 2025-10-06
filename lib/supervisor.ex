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

    server_to_run = case System.get_env("SERVER_TO_RUN") do
      nil -> []
      server_to_run -> [{String.to_existing_atom(server_to_run), %{}}]
    end

    topologies = [
      gossip: [
        strategy: Cluster.Strategy.Gossip,
        config: [
          port: 45892,
          if_addr: "0.0.0.0",
          multicast_addr: "192.168.0.255",
          broadcast_only: true,
          secret: "secret"
        ]
      ]
    ]

    # server_to_run = [String.to_existing_atom(System.get_env("SERVER_TO_RUN"))]


    rest_child =
      case System.get_env("ENABLE_REST") do
        "true" -> [Rest.Server]
        _ -> []
      end

    childrens =
      [
        {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]}
      ] ++ server_to_run ++ rest_child

    Supervisor.init(childrens, strategy: :one_for_one)
  end
end
