defmodule Reaper.UrlBuilderTest do
  use ExUnit.Case
  use Placebo
  import Checkov
  alias Reaper.UrlBuilder

  data_test "builds #{result}" do
    assert result == UrlBuilder.build(dataset)

    where([
      [:dataset, :result],
      [
        FixtureHelper.new_sickle(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{start_date: "19700101", end_date: "19700102"}
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_sickle(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{
            start_date: "<%= Date.to_iso8601(~D[1970-01-01], :basic) %>",
            end_date: "<%= Date.to_iso8601(~D[1970-01-02], :basic) %>"
          }
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_sickle(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{}
        }),
        "https://my-url.com"
      ],
      [
        FixtureHelper.new_sickle(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{
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
