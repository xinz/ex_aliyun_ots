defmodule ExAliyunOtsTest.AtomicIncrement do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, ReturnType, RowExistence, OperationType}
  require PKType
  require ReturnType
  require RowExistence
  require OperationType

  @instance_name "edc-ex-test"

  test "atomic increment" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_atomic_inc_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.integer}]
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok
    Process.sleep(6_000)

    condition = %Var.Condition{
      row_existence: RowExistence.ignore
    }

    for i <- 1..3 do
      var_update_row = %Var.UpdateRow{
        table_name: table_name,
        primary_keys: [{"partition_key", 1}],
        updates: %{
          OperationType.put => [{"name", "namestr"}],
          OperationType.increment => [{"count", 1}],
        },
        condition: condition,
        return_type: ReturnType.after_modify,
        return_columns: ["count"]
      }
      {:ok, response} = ExAliyunOts.Client.update_row(@instance_name, var_update_row)
      {_pks, [{return_key, return_value, _timestamp}]} = response.row
      assert return_key == "count"
      assert return_value == i
    end


    batch_write_request = [%Var.BatchWriteRequest{
      table_name: table_name,
      rows: [
        %Var.RowInBatchWriteRequest{
          type: OperationType.update,
          primary_keys: [{"partition_key", 2}],
          updates: %{
            OperationType.increment => [{"count", 10}]
          },
          condition: condition,
          return_type: ReturnType.after_modify,
          return_columns: ["count"]
        },
        %Var.RowInBatchWriteRequest{
          type: OperationType.put,
          primary_keys: [{"partition_key", 3}],
          updates: [{"count", 0}, {"size", 55.1}],
          condition: condition,
          return_type: ReturnType.pk
        },
      ]
    }]

    {:ok, response} = ExAliyunOts.Client.batch_write_row(@instance_name, batch_write_request)

    tables = response.tables
    assert length(tables) == 1

    Enum.map(tables, fn(table) ->
      Enum.map(table.rows, fn(row_in_batch_write_row_response) ->
        row = row_in_batch_write_row_response.row
        case row do
          {nil, [{"count", value, _timestamp}]} ->
            assert value == 10
          {[{"partition_key", 3}], nil} ->
            :ok
        end
      end)
    end)

    result = ExAliyunOts.Client.delete_table(@instance_name, table_name)
    assert result == :ok
  end

end
