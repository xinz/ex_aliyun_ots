defmodule ExAliyunOtsTest.PutRow do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, ReturnType, RowExistence}
  require PKType
  require ReturnType
  require RowExistence

  @instance_key EDCEXTestInstance

  test "put row" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_put_row_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.integer}]
    }
    result = ExAliyunOts.Client.create_table(@instance_key, var_create_table)
    assert result == :ok
  
    condition = %Var.Condition{
      row_existence: RowExistence.expect_not_exist
    }
    partition_key = 3
    var_put_row = %Var.PutRow{
      table_name: table_name,
      primary_keys: [{"partition_key", partition_key}],
      attribute_columns: [{"name", "t3_name"}, {"age", 23}, {"size", 1.1}, {"level", <<1, 2, 3, 4>>}, {"content", "测试内容"}],
      condition: condition,
      return_type: ReturnType.pk
    }
    {:ok, _result} = ExAliyunOts.Client.put_row(@instance_key, var_put_row)

    var_get_row = %ExAliyunOts.Var.GetRow{
      table_name: table_name,
      primary_keys: [{"partition_key", partition_key}],
      columns_to_get: ["name", "age", "size", "level", "content"]
    }
    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)
    Logger.info ">>> #{inspect get_row_result}"
    {:ok, get_row_response} = get_row_result
    {_primary_keys, columns} = get_row_response.row
    # order by column field
    assert {"age", 23, _update_timestamp} = Enum.at(columns, 0)
    assert {"content", "测试内容", _update_timestamp} = Enum.at(columns, 1)
    assert {"level", <<1, 2, 3, 4>>, _update_timestamp} = Enum.at(columns, 2)
    assert {"name", "t3_name", _update_timestamp} = Enum.at(columns, 3)
    assert {"size", 1.1, _update_timestamp} = Enum.at(columns, 4)

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name)
    assert result == :ok
  end

end
