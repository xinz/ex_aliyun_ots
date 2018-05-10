defmodule ExAliyunOtsTest.DeleteRow do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, ReturnType, RowExistence}
  require PKType
  require ReturnType
  require RowExistence

  @instance_name "super-test"

  test "put row" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_delete_row_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.integer}]
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok
    Process.sleep(5_000)
  
    condition = %Var.Condition{
      row_existence: RowExistence.expect_not_exist
    }
    for primary_key <- 1..3 do
      var_put_row = %Var.PutRow{
        table_name: table_name,
        primary_keys: [{"partition_key", primary_key}],
        attribute_columns: [{"name", "testname_#{primary_key}"}, {"age", 23}, {"size", 1.1}, {"is_finished", true}, {"level", <<1, 2, 3, 4>>}],
        condition: condition,
        return_type: ReturnType.pk
      }
      {:ok, _result} = ExAliyunOts.Client.put_row(@instance_name, var_put_row)
    end

    condition_exist = %Var.Condition{
      row_existence: RowExistence.expect_exist
    }
    condition_ignore = %Var.Condition{
      row_existence: RowExistence.ignore
    }

    # successfully delete a existed case
    var_delete_row = %ExAliyunOts.Var.DeleteRow{
      table_name: table_name,
      primary_keys: [{"partition_key", 1}],
      condition: condition_exist
    }
    delete_row_result = ExAliyunOts.Client.delete_row(@instance_name, var_delete_row)
    assert {:ok, _response} = delete_row_result

    # populate error condition when delete a not existed case but expect it existed
    var_delete_row_not_exist = %ExAliyunOts.Var.DeleteRow{
      table_name: table_name,
      primary_keys: [{"partition_key", 10}],
      condition: condition_exist
    }
    delete_row_result = ExAliyunOts.Client.delete_row(@instance_name, var_delete_row_not_exist)
    assert {:error, err_message} = delete_row_result
    assert String.contains?(err_message, "Condition check failed") == true

    # ignore the not existed case
    var_delete_row_not_exist = %ExAliyunOts.Var.DeleteRow{
      table_name: table_name,
      primary_keys: [{"partition_key", 10}],
      condition: condition_ignore
    }
    delete_row_result = ExAliyunOts.Client.delete_row(@instance_name, var_delete_row_not_exist)
    assert {:ok, _response} = delete_row_result

    result = ExAliyunOts.Client.delete_table(@instance_name, table_name)
    assert result == :ok
  end

end
