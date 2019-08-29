defmodule Reaper.Router do
  use Plug.Router
  require Logger

  plug(:match)
  plug(:dispatch)

  post "/upload/:dataset_id" do
    Reaper.Controller.Upload.handle(conn)
  end

  match _ do
    Logger.warn("Failed connection attempt: #{inspect(conn)}")
    send_resp(conn, 404, "Not Found")
  end
end
