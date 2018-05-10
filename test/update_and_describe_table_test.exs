defmodule ExAliyunOtsTest.UpdateAndDescribeTable do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence}
  require PKType
  require OperationType
  require ReturnType
  require RowExistence

  @instance_name "super-test"

  test "create table and then update it, meanwhile describe this table" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_update_table_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.string}, {"order_id", PKType.string}],
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok

    Process.sleep(60_000)
    stream = %Var.StreamSpec{
      is_enabled: true,
      expiration_time: 1
    }
    var_update_table = %Var.UpdateTable{
      table_name: table_name,
      reserved_throughput_write: 10,
      time_to_live: 86_500,
      stream_spec: stream
    }
    result = ExAliyunOts.Client.update_table(@instance_name, var_update_table)
    Logger.info "#{inspect result}"
    # if the error message is `Your instance is forbidden to update capacity unit`, please ensure your server instance is a high-performance instance

    describe_table_result = ExAliyunOts.Client.describe_table(@instance_name, table_name)
    Logger.info "describe_table_result: #{inspect describe_table_result}"
    {:ok, table_info} = describe_table_result
    assert table_info.table_meta.table_name == table_name
    assert table_info.table_options.deviation_cell_version_in_sec == 86_400
    assert table_info.table_options.max_versions == 1
    assert table_info.table_options.time_to_live == 86_500
    primary_key_list = table_info.table_meta.primary_key
    pk1 = Enum.at(primary_key_list, 0)
    assert "partition_key" == pk1.name
    assert :'STRING' == pk1.type
    pk2 = Enum.at(primary_key_list, 1)
    assert "order_id" == pk2.name
    assert :'STRING' == pk2.type

    del_table_result = ExAliyunOts.Client.delete_table(@instance_name, table_name)
    assert del_table_result == :ok
  end

end
