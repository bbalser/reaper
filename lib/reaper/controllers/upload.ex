defmodule Reaper.Controller.Upload do
  @moduledoc false
  require Logger
  import Plug.Conn

  alias Reaper.DataFeed.SchemaFiller

  def handle(%{params: %{"dataset_id" => dataset_id}} = conn) do
    with {:ok, body, conn} <- read_body(conn),
         {:ok, dataset} <- SmartCity.Dataset.get(dataset_id),
         {:ok, decoded_body} <- Jason.decode(body) do
      Logger.info("Message received: #{inspect(decoded_body)}")

      messages =
        decoded_body
        |> Map.get("time_points")
        |> List.wrap()
        |> Enum.map(&SchemaFiller.fill(dataset.technical.schema, &1))
        |> Enum.map(&to_data_message(dataset, &1))

      topic = topic(dataset)
      Elsa.produce(endpoints(), topic, messages, partition: 0)

      send_resp(conn, 200, "Dataset_id: #{dataset_id}")
    else
      e ->
        Logger.error("Unable to process request: conn(#{inspect(conn)}), error reason: #{inspect(e)}")
        send_resp(conn, 500, "Internal Server Error")
    end
  rescue
    e ->
      Logger.error("Unable to process request: conn(#{inspect(conn)}), error reason: #{inspect(e)}")
      send_resp(conn, 500, "Internal Server Error")
  end

  defp topic(dataset) do
    "#{topic_prefix()}-#{dataset.id}"
  end

  defp topic_prefix() do
    Application.get_env(:reaper, :output_topic_prefix)
  end

  defp endpoints() do
    Application.get_env(:reaper, :elsa_brokers)
  end

  defp to_data_message(dataset, payload) do
    data = %{
      dataset_id: dataset.id,
      operational: %{timing: []},
      payload: payload,
      _metadata: %{}
    }

    {:ok, data} = SmartCity.Data.new(data)
    data
  end
end
