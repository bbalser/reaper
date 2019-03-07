defmodule Reaper.FeedSupervisor do
  @moduledoc """
  Supervises feed ETL processes (`Reaper.DataFeed`) and their caches.
  """

  use Supervisor
  require Keyword
  require Logger
  require Cachex.Spec
  alias Cachex.{Policy, Spec}

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  def init(state) do
    children = create_child_spec(state[:dataset])

    Logger.debug(fn -> "Starting #{__MODULE__} with children: #{inspect(children, pretty: true)}" end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  def update_data_feed(supervisor_pid, %{id: id} = dataset) do
    "#{id}_feed"
    |> String.to_atom()
    |> find_child_by_id(supervisor_pid)
    |> Reaper.DataFeed.update(dataset)
  end

  def create_child_spec(%{id: id} = dataset) do
    feed_name = String.to_atom("#{id}_feed")
    cache_name = String.to_atom("#{id}_cache")
    cache_limit = Spec.limit(size: 2000, policy: Policy.LRW, reclaim: 0.2)

    [
      %{
        id: cache_name,
        start: {Cachex, :start_link, [cache_name, [limit: cache_limit]]}
      },
      %{
        id: feed_name,
        start: {
          Reaper.DataFeed,
          :start_link,
          [
            %{
              dataset: dataset,
              pids: %{
                name: feed_name,
                cache: cache_name
              }
            }
          ]
        }
      }
    ]
  end

  defp find_child_by_id(id, supervisor) do
    {_child_id, pid, _type, _modules} =
      supervisor
      |> Supervisor.which_children()
      |> Enum.find(fn {child_id, _pid, _type, _modules} -> child_id == id end)

    pid
  end
end
