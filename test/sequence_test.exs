defmodule ExAliyunOtsTest.Sequence do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  test "next value" do
    sequence_name = "test_sequence"
    var_new = %Var.NewSequence{
      name: sequence_name
    }
    result = Sequence.create(@instance_key, var_new)
    assert result == :ok
    Process.sleep(3_000)

    concurrency_size = 10
    stream = Task.async_stream(1..concurrency_size, fn(_index) -> 
      var_next = %Var.GetSequenceNextValue{
        name: sequence_name,
      }
      Sequence.next_value(@instance_key, var_next)
    end, timeout: :infinity, max_concurrency: concurrency_size)

    result = Enum.map(stream, fn({:ok, item}) -> item end) |> MapSet.new()
    assert MapSet.size(result) == concurrency_size
    assert Enum.sort(result) == Enum.map(1..concurrency_size, fn(item) -> item end)

    del_result = Sequence.delete_event(@instance_key, sequence_name, "default")
    assert {:ok, _delete_response} = del_result

    result = Sequence.delete(@instance_key, sequence_name)
    assert result == :ok
  end

end
