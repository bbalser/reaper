defmodule Reaper.UrlBuilderTest do
  use ExUnit.Case
  use Placebo
  import Checkov
  alias Reaper.UrlBuilder

  data_test "builds #{result}" do
    assert result == UrlBuilder.build(reaper_config)

    where([
      [:reaper_config, :result],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          sourceQueryParams: %{start_date: "19700101", end_date: "19700102"}
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          sourceQueryParams: %{
            start_date: "<%= Date.to_iso8601(~D[1970-01-01], :basic) %>",
            end_date: "<%= Date.to_iso8601(~D[1970-01-02], :basic) %>"
          }
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com/date/<%= Date.to_iso8601(~D[1941-12-07], :basic) %>/stuff"
        }),
        "https://my-url.com/date/19411207/stuff"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl:
            "https://my-url.com/date/<%= Date.to_iso8601(~D[1941-12-07], :basic) %>/stuff/<%= Date.to_iso8601(~D[1999-12-31], :basic) %>/other",
          sourceQueryParams: %{something: "value"}
        }),
        "https://my-url.com/date/19411207/stuff/19991231/other?something=value"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          sourceQueryParams: %{}
        }),
        "https://my-url.com"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          sourceQueryParams: %{
            start_date:
              "<%= Date.to_iso8601(last_success_time || DateTime.from_unix!(0) |> DateTime.to_date(), :basic) %>",
            end_date: "<%= Date.to_iso8601(~D[1970-01-02], :basic) %>"
          }
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ]
    ])
  end
end
