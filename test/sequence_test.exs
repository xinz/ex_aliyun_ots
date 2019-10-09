defmodule ExAliyunOtsTest.Sequence do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  @sequence_name "test_sequence"

  setup_all do
    var_new = %Var.NewSequence{
      name: @sequence_name
    }
    result = Sequence.create(@instance_key, var_new)
    assert result == :ok

    on_exit(fn ->
      del_result = Sequence.delete_event(@instance_key, @sequence_name, "default")
      assert {:ok, _delete_response} = del_result

      result = Sequence.delete(@instance_key, @sequence_name)
      assert result == :ok
    end)
  end

  test "next value" do
    concurrency_size = 30
    stream = Task.async_stream(1..concurrency_size, fn(_index) -> 
      var_next = %Var.GetSequenceNextValue{
        name: @sequence_name,
      }
      Sequence.next_value(@instance_key, var_next)
    end, timeout: :infinity)

    result = Enum.map(stream, fn({:ok, item}) -> item end) |> MapSet.new()
    assert MapSet.size(result) == concurrency_size
    assert Enum.sort(result) == Enum.map(1..concurrency_size, fn(item) -> item end)
  end

end
