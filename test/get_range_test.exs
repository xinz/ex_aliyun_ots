defmodule ExAliyunOtsTest.GetRange do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence}
  require PKType
  require OperationType
  require ReturnType
  require RowExistence

  test "get range" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_get_range_#{cur_timestamp}"

    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [
        {"partition_key", PKType.integer()},
        {"id", PKType.integer(), PKType.auto_increment()}
      ]
    }

    result = Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    Process.sleep(10_000)

    condition = %Var.Condition{
      row_existence: RowExistence.ignore()
    }

    Logger.info("inserting test data...")

    batch_write_rows =
      for partition_key <- 1..20 do
        %Var.RowInBatchWriteRequest{
          type: OperationType.put(),
          primary_keys: [{"partition_key", partition_key}, {"id", PKType.auto_increment()}],
          updates: [{"name", "name_#{partition_key}"}, {"value", partition_key}],
          condition: condition,
          return_type: ReturnType.pk()
        }
      end

    batch_request = [
      %Var.BatchWriteRequest{
        table_name: table_name,
        rows: batch_write_rows
      }
    ]

    {:ok, _result} = Client.batch_write_row(@instance_key, batch_request)
    Logger.info("test data is ready")

    var_get_range = %Var.GetRange{
      table_name: table_name,
      inclusive_start_primary_keys: [{"partition_key", 1}, {"id", PKType.inf_min()}],
      exclusive_end_primary_keys: [{"partition_key", 10}, {"id", PKType.inf_max()}],
      limit: 3
    }

    {:ok, get_range_response} = Client.get_range(@instance_key, var_get_range)
    rows = get_range_response.rows
    assert length(rows) == 3
    next_start_primary_key = get_range_response.next_start_primary_key
    assert next_start_primary_key != nil

    var_get_range = %Var.GetRange{
      table_name: table_name,
      exclusive_end_primary_keys: [{"partition_key", 10}, {"id", PKType.inf_max()}],
      limit: 3
    }

    {:ok, get_range_response2} =
      Client.get_range(@instance_key, var_get_range, next_start_primary_key)

    assert length(get_range_response2.rows) == 3
    assert get_range_response2.next_start_primary_key != nil

    rows
    |> Enum.with_index()
    |> Enum.map(fn {{primary_keys, attribute_columns}, index} ->
      value = index + 1
      assert {"partition_key", ^value} = Enum.at(primary_keys, 0)
      name_value = "name_#{value}"
      assert {"name", ^name_value, _timestamp} = Enum.at(attribute_columns, 0)
      assert {"value", ^value, _timestamp} = Enum.at(attribute_columns, 1)
    end)

    var_get_range_unlimit = %Var.GetRange{
      table_name: table_name,
      inclusive_start_primary_keys: [{"partition_key", 1}, {"id", PKType.inf_min()}],
      exclusive_end_primary_keys: [{"partition_key", 10}, {"id", PKType.inf_max()}]
    }

    {:ok, get_range_response} = Client.get_range(@instance_key, var_get_range_unlimit)
    assert length(get_range_response.rows) == 10
    assert get_range_response.next_start_primary_key == nil

    var_iterate_get_range = %Var.GetRange{
      table_name: table_name,
      inclusive_start_primary_keys: [{"partition_key", 1}, {"id", PKType.inf_min()}],
      exclusive_end_primary_keys: [{"partition_key", 10}, {"id", PKType.inf_max()}],
      limit: 5
    }

    {:ok, get_all_range_response} =
      Client.iterate_get_all_range(@instance_key, var_iterate_get_range)

    assert length(get_all_range_response.rows) == 10

    # test `iterate_get_all_range` with the huge rows
    var_iterate_get_range = %Var.GetRange{
      table_name: table_name,
      inclusive_start_primary_keys: [{"partition_key", 1}, {"id", PKType.inf_min()}],
      exclusive_end_primary_keys: [{"partition_key", 20}, {"id", PKType.inf_max()}],
      limit: 100
    }

    {:ok, get_all_range_response} =
      Client.iterate_get_all_range(@instance_key, var_iterate_get_range)

    assert length(get_all_range_response.rows) == 20

    get_all_range_response.rows
    |> Enum.with_index()
    |> Enum.map(fn {{primary_keys, attribute_columns}, index} ->
      value = index + 1
      assert {"partition_key", ^value} = Enum.at(primary_keys, 0)
      name_value = "name_#{value}"
      assert {"name", ^name_value, _timestamp} = Enum.at(attribute_columns, 0)
      assert {"value", ^value, _timestamp} = Enum.at(attribute_columns, 1)
    end)

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name)
    assert result == :ok
  end
end
