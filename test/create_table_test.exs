defmodule ExAliyunOtsTest.CreateTableAndBasicRowOperation do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence}
  require PKType
  require OperationType
  require ReturnType
  require RowExistence

  @instance_name "super-test"

  test "create table and then delete it" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_tmp_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.string}, {"default_id", PKType.integer, PKType.auto_increment}, {"order_id", PKType.string}],
      time_to_live: 86_400
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok
    {:error, error_msg} = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert String.contains?(error_msg, "Requested table already exists") == true

    result = ExAliyunOts.Client.delete_table(@instance_name, table_name)
    assert result == :ok
  end

  test "create table, put and get row" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.string}, {"default_id", PKType.integer, PKType.auto_increment}, {"order_id", PKType.string}],
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok

    Logger.info "waiting for table created..."
    Process.sleep(5_000)

    partition_key = "c3be8617-10d7-422b-8f80-58603d1603d6"
    order_id = "order2"
    # PutRow with an auto increment primary key, the `row_existence` field should be as "IGNORE".
    condition = %Var.Condition{
      row_existence: RowExistence.ignore
    }
    var_put_row = %Var.PutRow{
      table_name: table_name,
      primary_keys: [{"partition_key", partition_key}, {"default_id", PKType.auto_increment}, {"order_id", order_id}],
      attribute_columns: [{"name", "t3_name"}, {"age", 23}, {"size", 4.2}, {"level", <<1, 2, 3, 4>>}],
      condition: condition,
      return_type: ReturnType.pk
    }
    {:ok, result} = ExAliyunOts.Client.put_row(@instance_name, var_put_row)
    {primary_keys_result, _attribute_cols_result} = result.row
    {"partition_key", return_partition_key} = Enum.at(primary_keys_result, 0)
    assert return_partition_key == partition_key
    {"default_id", new_default_id} = Enum.at(primary_keys_result, 1)
    assert is_integer(new_default_id)
    {"order_id", return_order_id} = Enum.at(primary_keys_result, 2)
    assert return_order_id == order_id

    condition = %Var.Condition{
      row_existence: RowExistence.expect_exist
    }
    var_update_row = %Var.UpdateRow{
      table_name: table_name,
      primary_keys: [{"partition_key", partition_key}, {"default_id", new_default_id}, {"order_id", order_id}],
      updates: %{
        OperationType.put => [{"new_added_field1", "v2"}, {"name", "updated_name_v2"}],
        OperationType.delete_all => ["level", "size"]
      },
      condition: condition
    }
    update_row_result = ExAliyunOts.Client.update_row(@instance_name, var_update_row)
    Logger.debug(fn -> "update_row_result >> #{inspect update_row_result}" end)
    assert {:ok, _} = update_row_result

    var_get_row = %ExAliyunOts.Var.GetRow{
      table_name: table_name,
      primary_keys: [{"partition_key", partition_key}, {"default_id", new_default_id}, {"order_id", order_id}],
      columns_to_get: ["name", "level"]
    }
    get_row_result = ExAliyunOts.Client.get_row(@instance_name, var_get_row)
    Logger.debug(fn -> "get_row_result >> #{inspect get_row_result}" end)
    {:ok, get_row_response} = get_row_result
    {primary_keys, get_columns} = get_row_response.row
    assert {"partition_key", return_partition_key_in_get} = Enum.at(primary_keys, 0)
    assert return_partition_key_in_get == partition_key
    assert {"default_id", new_default_id_in_get} = Enum.at(primary_keys, 1)
    assert new_default_id_in_get == new_default_id
    assert {"order_id", return_order_id_in_get} = Enum.at(primary_keys, 2)
    assert return_order_id_in_get == order_id

    assert length(get_columns) == 1
    assert {"name", "updated_name_v2", update_timestamp} = Enum.at(get_columns, 0)
    assert is_integer(update_timestamp)

    result = ExAliyunOts.Client.delete_table(@instance_name, table_name)
    assert result == :ok
  end

end
