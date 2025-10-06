defmodule Rest.Server do
  @moduledoc false
  def child_spec(_arg) do
    port = String.to_integer(System.get_env("REST_PORT") || "4000")

    Plug.Cowboy.child_spec(
      scheme: :http,
      plug: Rest,             # router abajo
      options: [port: port]
    )
  end
end

defmodule Rest do
  @moduledoc """
  API REST (Plug) para orquestar compras sin tocar el módulo Compras.
  """

  use Plug.Router
  use Plug.ErrorHandler

  plug Plug.Logger

  plug Plug.Parsers,
    parsers: [:json, :urlencoded, :multipart],
    pass: ["*/*"],
    json_decoder: Jason

  plug :match
  plug :dispatch

  # ---- RUTAS ----

  get "/" do
    send_resp(conn, 200, "Módulo REST activo 🚀")
  end

  get "/health" do
    send_json(conn, 200, %{status: "ok", module: "rest"})
  end

  # POST /compras
  # Body esperado (ejemplo):
  # {"id_producto": 123, "medio_pago":"tarjeta", "forma_entrega":"correo"}
  post "/compras" do
    params = conn.body_params

    with {:ok, id_producto}   <- require_integer(params, "id_producto"),
        {:ok, medio_pago}    <- require_atom(params, "medio_pago"),
        {:ok, forma_entrega} <- require_atom(params, "forma_entrega") do

      case Libremarket.Compras.Server.comprar({id_producto, medio_pago, forma_entrega}) do
        # cuando tu server devuelve el id y el resultado
        %{id_compra: id, resultado: res} ->
          send_json(conn, 201, %{id_compra: id, resultado: normalize_result(res)})

        # cuando devuelve {:ok, map} / {:error, map}
        {:ok, map} ->
          send_json(conn, 201, %{
            resultado: normalize_result({:ok, map})
          })

        {:error, map} ->
          send_json(conn, 422, %{
            resultado: normalize_result({:error, map})
          })

        other ->
          # fallback: igual lo saneamos para JSON
          send_json(conn, 200, other)
      end
    else
      {:error, {:missing, f}}  -> send_json(conn, 400, %{error: "Falta #{f}"})
      {:error, {:bad_type, f}} -> send_json(conn, 400, %{error: "Tipo inválido en #{f}"})
    end
  end

  # GET /compras/:id
  get "/compras/:id" do
    case Integer.parse(id) do
      {int_id, ""} ->
        case Libremarket.Compras.Server.obtener(int_id) do
          {:ok, %{id_compra: ^int_id, resultado: res}} ->
            send_json(conn, 200, %{id_compra: int_id, resultado: normalize_result(res)})

          {:error, :not_found} ->
            send_json(conn, 404, %{error: "Compra #{int_id} no encontrada"})

          other ->
            send_json(conn, 502, %{error: inspect(other)})
        end

      _ ->
        send_json(conn, 400, %{error: "id debe ser entero"})
    end
  end

  # 404 genérico
  match _ do
    send_json(conn, 404, %{error: "Ruta no encontrada"})
  end

  # ---- HELPERS (fuera de rutas) ----


  defp send_json(conn, status, data) do
    data = json_safe(data)
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(data))
  end

  defp json_safe(%{} = map) do
    map
    |> Enum.map(fn {k, v} -> {json_key(k), json_safe(v)} end)
    |> Enum.into(%{})
  end

  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)

  defp json_safe({:ok, v}),    do: %{"status" => "ok",    "data" => json_safe(v)}
  defp json_safe({:error, v}), do: %{"status" => "error", "data" => json_safe(v)}

  defp json_safe(a) when is_atom(a) do
    case a do
      true -> true
      false -> false
      nil -> nil
      _ -> Atom.to_string(a)
    end
  end

  defp json_safe(other), do: other

  defp json_key(k) when is_atom(k), do: Atom.to_string(k)
  defp json_key(k), do: k
  # valida entero en el mapa de params
  defp require_integer(m, k) do
    case m[k] do
      i when is_integer(i) -> {:ok, i}
      nil -> {:error, {:missing, k}}
      _ -> {:error, {:bad_type, k}}
    end
  end

  # acepta "tarjeta" -> :tarjeta, o átomo directo
  defp require_atom(m, k) do
    case m[k] do
      s when is_binary(s) -> {:ok, String.to_atom(s)}
      a when is_atom(a)   -> {:ok, a}
      nil -> {:error, {:missing, k}}
      _ -> {:error, {:bad_type, k}}
    end
  end

  # tu pipeline Libremarket.Compras.* devuelve {:ok, map} | {:error, map}
  defp normalize_result({:ok, map})   when is_map(map), do: %{status: "ok",    data: map}
  defp normalize_result({:error, map}) when is_map(map), do: %{status: "error", data: map}
  defp normalize_result(other), do: other

  @impl true
  def handle_errors(conn, %{reason: reason}) do
    send_json(conn, conn.status || 500, %{error: inspect(reason)})
  end
end
