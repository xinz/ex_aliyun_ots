defmodule ExAliyunOtsTest.Tunnel.Backoff do
  use ExUnit.Case

  alias ExAliyunOts.Tunnel.Backoff

  test "reset backoff" do
    backoff = Backoff.new()
    Process.sleep(100)
    reset_backoff = Backoff.reset()

    assert reset_backoff.start_time_ms > backoff.start_time_ms
  end

  test "next backoff" do
    test_cases = 1..5
    test_cases_size = Enum.to_list(test_cases) |> length()

    backoff = Backoff.new()

    {result, final_backoff} =
      Enum.map_reduce(test_cases, backoff, fn _i, acc ->
        {backoff, ms} = Backoff.next_backoff_ms(acc)
        {{backoff, ms}, backoff}
      end)

    assert backoff.start_time_ms == final_backoff.start_time_ms

    interval_ms_list =
      result
      |> Enum.map(fn {backoff, _ms} ->
        backoff.current_interval_ms
      end)

    assert List.last(interval_ms_list) == 5_000

    interval_ms_set = MapSet.new(interval_ms_list)

    assert Enum.max(interval_ms_set) == 5_000
    assert MapSet.size(interval_ms_set) < test_cases_size

    result
    |> Enum.each(fn {_backoff, ms} ->
      assert is_integer(ms) == true
    end)
  end
end
