defmodule ExAliyunOts.MixinTest.Integer do
  
  use ExUnit.Case

  @instance_key EDCEXTestInstance

  use ExAliyunOts,
    instance: @instance_key

  require Logger

  @table "test_int"

  setup_all do

    create_table @table, [{"id", :integer}]

    on_exit(fn ->
      delete_table @table
    end)

    Process.sleep(1000)
    :ok
  end

  test "put and get row with negative integer" do
    put_row @table, [{"id", 1}],
      [{"int", -1}],
      condition: condition(:ignore),
      return_type: :pk

    {:ok, response} = get_row @table, [{"id", 1}]
    {_, [{"int", value, _}]} = response.row
    assert value == -1
  end

  test "put and get row with negative float" do

    put_row @table, [{"id", 2}],
      [{"float1", -1.98}, {"float2", 9.89}, {"float3", -192.980}],
      condition: condition(:ignore),
      return_type: :pk

    {:ok, response} = get_row @table, [{"id", 2}]
    {_, [{"float1", f1, _}, {"float2", f2, _}, {"float3", f3, _}]} = response.row
    assert f1 == -1.98 and f2 == 9.89 and f3 == -192.98
  end

end
