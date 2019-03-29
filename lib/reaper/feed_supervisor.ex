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
    children = create_child_spec(state[:reaper_config])

    Logger.debug(fn -> "Starting #{__MODULE__} with children: #{inspect(children, pretty: true)}" end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  def update_data_feed(supervisor_pid, %{dataset_id: id} = reaper_config) do
    "#{id}_feed"
    |> String.to_atom()
    |> find_child_by_id(supervisor_pid)
    |> Reaper.DataFeed.update(reaper_config)
  end

  def create_child_spec(%{dataset_id: id} = reaper_config) do
    feed_name = String.to_atom("#{id}_feed")
    cache_name = String.to_atom("#{id}_cache")
    cache_limit = Spec.limit(size: 2000, policy: Policy.LRW, reclaim: 0.2)

    restart_policy =
      case Map.get(reaper_config, :cadence) do
        "once" -> :transient
        _ -> :permanent
      end

    [
      %{
        id: cache_name,
        start: {Cachex, :start_link, [cache_name, [limit: cache_limit]]}
      },
      %{
        id: feed_name,
        restart: restart_policy,
        start: {
          Reaper.DataFeed,
          :start_link,
          [
            %{
              reaper_config: reaper_config,
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
