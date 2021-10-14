defmodule ExAliyunOts.MixinTest.Table do
  use ExUnit.Case
  use ExAliyunOts, instance: EDCEXTestInstance
  require Logger

  test "table" do
    cur_timestamp = System.os_time(:second)
    table_name = "test_mixin_table_#{cur_timestamp}"

    create_table_result =
      create_table(table_name, [{"key1", :string}],
        reserved_throughput_write: 1,
        reserved_throughput_read: 1,
        time_to_live: 100_000,
        max_versions: 3,
        deviation_cell_version_in_sec: 6_400,
        stream_spec: [is_enabled: true, expiration_time: 2]
      )

    assert create_table_result == :ok

    {:ok, list_tables_result} = list_table()

    assert length(list_tables_result.table_names) > 0
    assert table_name in list_tables_result.table_names == true

    _update_table_result =
      update_table(table_name,
        reserved_throughput_write: 10,
        time_to_live: 200_000,
        stream_spec: [is_enabled: false]
      )

    {:ok, describe_table_result} = describe_table(table_name)

    assert describe_table_result.table_meta.table_name == table_name

    del_table_reslt = delete_table(table_name)
    assert del_table_reslt == :ok
  end
end
