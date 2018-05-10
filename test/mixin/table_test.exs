defmodule ExAliyunOts.MixinTest.Table do

  use ExUnit.Case
  use ExAliyunOts.Mixin

  require Logger
  alias ExAliyunOts.Const.PKType
  require PKType
  @instance_name "super-test"

  test "table" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_mixin_table_#{cur_timestamp}"

    create_table_result =
      create_table @instance_name, table_name, [{"key1", PKType.string}],
        reserved_throughput_write: 1,
        reserved_throughput_read: 1,
        time_to_live: 100_000,
        max_versions: 3,
        deviation_cell_version_in_sec: 6_400,
        stream_spec: [is_enabled: true, expiration_time: 2]

    assert create_table_result == :ok

    Process.sleep(30_000)

    {:ok, list_tables_result} = list_table @instance_name

    assert length(list_tables_result.table_names) > 0

    _update_table_result =
      update_table @instance_name, table_name,
        reserved_throughput_write: 10,
        time_to_live: 200_000,
        stream_spec: [is_enabled: false]

    {:ok, describe_table_result} =
      describe_table @instance_name, table_name

    assert describe_table_result.table_meta.table_name == table_name

    del_table_reslt = delete_table @instance_name, table_name
    assert del_table_reslt == :ok

  end
end
