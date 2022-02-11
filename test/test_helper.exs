ExUnit.start(timeout: :infinity, seed: 0)

defmodule ExAliyunOts.SearchTestHelper do
  use ExUnit.Case
  use ExAliyunOts,
    instance: EDCEXTestInstance
  require Logger

  def assert_search(table, index, opts, expected_total_hits, loop \\ 0)
  def assert_search(_table, _index, opts, _expected_total_hits, loop) when loop > 51 do
    Logger.error "timeout to query: #{inspect opts[:query]}, since always not found any matched data"
  end
  def assert_search(table, index, opts, expected_total_hits, loop) do
    {:ok, response} = search(table, index, opts)
    total_hits = response.total_hits
    if total_hits == expected_total_hits do
      assert true, "search matched"
      response
    else
      if total_hits == 0 do
        Process.sleep(3_000)
        assert_search(table, index, opts, expected_total_hits, loop + 1)
      else
        assert total_hits == expected_total_hits
      end
    end
  end

  def assert_search_request(instance, request, expected_total_hits, loop \\ 0)
  def assert_search_request(_, request, _, loop) when loop > 51 do
    Logger.error "timeout to query with request: #{inspect request}, since always not found any matched data"
  end
  def assert_search_request(instance, request, expected_total_hits, loop) do
    case ExAliyunOts.Client.search(instance, request) do
      {:ok, response} ->
        total_hits = response.total_hits
        if total_hits == expected_total_hits do
          assert true, "search matched"
          {:ok, response}
        else
          if total_hits == 0 do
            Process.sleep(3_000)
            assert_search_request(instance, request, expected_total_hits, loop + 1)
          else
            assert total_hits == expected_total_hits
          end
        end
      error ->
        error
    end
  end

end
