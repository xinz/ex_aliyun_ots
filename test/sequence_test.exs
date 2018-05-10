defmodule ExAliyunOtsTest.Sequence do
  use ExUnit.Case
  require Logger

  @instance_name "super-test"
  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  test "next value" do
    cur_timestamp = Timex.to_unix(Timex.now())
    sequence_name = "test_seq_#{cur_timestamp}"
    var_new_seq = %Var.NewSequence{
      name: sequence_name
    }
    result = Sequence.create(@instance_name, var_new_seq)
    assert result == :ok
    Process.sleep(3_000)

    # concurrency test
    concurrency_size = 200
    stream = Task.async_stream(1..concurrency_size, fn(_index) -> 
      var_next_val = %Var.GetSequenceNextValue{
        name: sequence_name,
      }
      Sequence.next_value(@instance_name, var_next_val)
    end, timeout: :infinity, max_concurrency: concurrency_size)

    result = Enum.map(stream, fn({:ok, item}) -> item end)
    assert length(result) == concurrency_size

    distinct_length = MapSet.new(result) |> MapSet.to_list |> length
    assert distinct_length == concurrency_size
    assert Enum.sort(result) == Enum.map(1..concurrency_size, fn(item) -> item end)

    del_result = Sequence.delete_event(@instance_name, sequence_name, "default")
    assert {:ok, _delete_response} = del_result

    result = Sequence.delete(@instance_name, sequence_name)
    assert result == :ok
  end

end
