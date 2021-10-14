defmodule ExAliyunOts.MixinTest.AtomicIncrement do
  use ExUnit.Case

  use ExAliyunOts,
    instance: EDCEXTestInstance

  require Logger

  test "atomic increment in mixin" do
    cur_timestamp = System.os_time(:second)
    table_name = "test_mixin_atomic_inc_#{cur_timestamp}"

    create_table_result = create_table(table_name, [{"key1", PKType.integer()}])

    assert create_table_result == :ok

    {:ok, response} =
      update_row(table_name, [{"key1", 1}],
        put: [{"attr1", "put_attr1"}],
        increment: [{"count", 1}],
        return_type: ReturnType.after_modify(),
        return_columns: ["count"],
        condition: condition(:ignore)
      )

    {nil, [{"count", value1, _timestamp}]} = response.row
    assert value1 == 1

    {:ok, response2} =
      update_row(table_name, [{"key1", 1}],
        increment: [{"count", 2}],
        return_type: ReturnType.after_modify(),
        return_columns: ["count"],
        condition: condition(:expect_exist, "count" == 1)
      )

    {nil, [{"count", value2, _timestamp}]} = response2.row
    assert value2 == 3

    {:ok, response3} =
      batch_write([
        {table_name,
         [
           write_update([{"key1", 1}],
             put: [{"attr1", "updated_in_batch"}],
             increment: [{"count", -1}, {"count2", 2}],
             return_type: ReturnType.after_modify(),
             return_columns: ["count", "count2"],
             condition: condition(:expect_exist, "count" == 3)
           ),
           write_put(
             [{"key1", 2}],
             [{"count", 0}, {"attr1", "new_attr1"}],
             return_type: ReturnType.pk(),
             condition: condition(:expect_not_exist)
           )
         ]}
      ])

    Enum.map(response3.tables, fn table ->
      Enum.map(table.rows, fn row_in_batch_write_row_response ->
        row = row_in_batch_write_row_response.row

        case row do
          {nil, [{"count", value, _timestamp}, {"count2", value2, _timestamp2}]} ->
            assert value == 2
            assert value2 == 2

          {[{"key1", key_value}], nil} ->
            assert key_value == 2
        end
      end)
    end)

    delete_table(table_name)
  end
end
