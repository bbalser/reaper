defmodule Reaper.Router do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  post "/upload/:dataset_id" do
    Reaper.Controller.Upload.handle(conn)
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
