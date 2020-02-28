defmodule ExAliyunOts.MixinTest.CRUD do

  use ExAliyunOts,
    instance: EDCEXTestInstance

  use ExUnit.Case

  require Logger

  test "CRUD" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name1 = "test_mixin_table_tmp1_#{cur_timestamp}"
    table_name2 = "test_mixin_table_tmp2_#{cur_timestamp}"

    create_table1_result = 
      create_table table_name1, [{"key1", PKType.integer}, {"key2", PKType.string}]

    create_table2_result =
      create_table table_name2, [{"key1", PKType.string}]

    assert create_table1_result == :ok
    assert create_table2_result == :ok

    var_name = "1"
    {:ok, response} =
      get_row table_name1, [{"key1", cur_timestamp}, {"key2", "#{cur_timestamp}"}],
        columns_to_get: ["name", "level"],
        filter: filter(("name[ignore_if_missing: true, latest_version_only: true]" == var_name and "age" > 1) or ("class" == "1"))
    assert response.row == nil 

    {:ok, put_row_response} =
      put_row table_name1, [{"key1", 0}, {"key2", "0"}],
        [{"attr1", 2}, {"attr2", "attrname_1"}],
        condition: condition(:expect_not_exist),
        return_type: :pk

    assert put_row_response.row != nil

    {:ok, put_row_response} =
      put_row table_name1, [{"key1", 0}, {"key2", "0"}],
        [{"attr1", 2}, {"attr2", "attrname_1"}],
        condition: condition(:expect_exist),
        return_type: :pk

    assert put_row_response.row != nil

    for id <- 1..5 do
      {:ok, put_row_response} =
        put_row table_name1, [{"key1", id}, {"key2", "#{id}"}],
          [{"attr1", id * 2}, {"attr2", "attrname_#{id}"}],
          condition: condition(:expect_not_exist),
          return_type: :pk
      assert put_row_response.row == {[{"key1", id}, {"key2", "#{id}"}], nil}
    end

    {:ok, put_row_response} =
      put_row table_name2, [{"key1", "tab2_id1"}],
        [{"name", "name1"}, {"age", 20}],
        condition: condition(:expect_not_exist),
        return_type: :pk
    assert put_row_response.row == {[{"key1", "tab2_id1"}], nil}

    {:ok, response} =
      get_row table_name1, [{"key1", 2}, {"key2", "2"}],
        columns_to_get: ["attr2"]
    {pk_keys, attrs} = response.row
    assert pk_keys == [{"key1", 2}, {"key2", "2"}]
    {"attr2", "attrname_2", key2_attr2_ts} = Enum.at(attrs, 0)

    value = "attrname_2"
    {:ok, _response} =
      update_row table_name1, [{"key1", 2}, {"key2", "2"}],
        delete: [{"attr2", nil, key2_attr2_ts}],
        delete_all: ["attr1"],
        put: [{"attr3", "put_attr3"}],
        return_type: :pk,
        condition: condition(:expect_exist, "attr2" == value)
    {:ok, response} =
      get_row table_name1, [{"key1", 2}, {"key2", "2"}]
    {_pk_keys, attrs} = response.row
    {"attr3", "put_attr3", _ts} = Enum.at(attrs, 0)

    {:ok, _delete_row_response} =
      delete_row table_name1, [{"key1", 3}, {"key2", "3"}],
      condition: condition(:expect_exist, "attr2" == "attrname_3")

    {:ok, batch_get_row_response} =
      batch_get [
        get(table_name1, [[{"key1", 1}, {"key2", "1"}]]),
        get(table_name2, [{"key1", "tab2_id1"}],
          columns_to_get: ["name", "age"],
          filter: filter "age" >= 10),
      ]

    table1 = Enum.at(batch_get_row_response.tables, 0)
    row1_table1 = Enum.at(table1.rows, 0)
    {[{"key1", 1}, {"key2", "1"}], [{"attr1", 2, _}, {"attr2", "attrname_1", _}]} = row1_table1.row

    table2 = Enum.at(batch_get_row_response.tables, 1)
    row2_table1 = Enum.at(table2.rows, 0)
    {[{"key1", "tab2_id1"}], [{"age", 20, _}, {"name", "name1", _}]} = row2_table1.row

    {:ok, _batch_write_row_response} =
      batch_write [{table_name1, [
        write_delete([{"key1", 5}, {"key2", "5"}],
          return_type: :pk,
          condition: condition(:expect_exist, "attr1" == 5)),
        write_put([{"key1", 6}, {"key2", "6"}],
          [{"new_put_val1", "val1"}, {"new_put_val2", "val2"}],
          condition: condition(:expect_not_exist),
          return_type: :pk)
      ]}, {table_name2, [
        write_update([{"key1", "new_tab3_id2"}],
          put: [{"new_put1", "u1"}, {"new_put2", 2.5}],
          condition: condition(:expect_not_exist)),
        write_put([{"key1", "new_tab3_id3"}],
          [{"new_put1", "put1"}, {"new_put2", 10}],
          condition: condition(:expect_not_exist))
      ]}]

    {:ok, get_range_response} =
      get_range table_name1,
        [{"key1", 1}, {"key2", PKType.inf_min}],
        [{"key1", 4}, {"key2", PKType.inf_max}],
        limit: 2,
        direction: :forward
    get_range_rows = get_range_response.rows
    assert length(get_range_rows) == 2
    {[{"key1", 1}, {"key2", _}], attrs_key_1} = Enum.at(get_range_rows, 0)
    {[{"key1", 2}, {"key2", _}], attrs_key_2} = Enum.at(get_range_rows, 1)

    next_primary_key = get_range_response.next_start_primary_key
    assert next_primary_key != nil

    {:ok, get_range_response2} =
      get_range table_name1,
        next_primary_key,
        [{"key1", 4}, {"key2", PKType.inf_max}],
        direction: :forward
    get_range_rows2 = get_range_response2.rows
    assert length(get_range_rows2) == 1
    {[{"key1", 4}, {"key2", _}], attrs_key_4} = Enum.at(get_range_rows2, 0)

    {:ok, _iterate_all_range_response} =
      iterate_all_range table_name1,
        [{"key1", 1}, {"key2", PKType.inf_min}],
        [{"key1", 4}, {"key2", PKType.inf_max}],
        direction: :forward

    {_key, _value, start_timestamp} = List.first(attrs_key_1)
    {_key, _value, end_timestamp} = List.first(attrs_key_2)

    {:ok, get_range_response} =
      get_range table_name1,
        [{"key1", 1}, {"key2", PKType.inf_min}],
        [{"key1", 4}, {"key2", PKType.inf_max}],
        time_range: {start_timestamp, end_timestamp},
        direction: :forward
    get_range_rows = get_range_response.rows
    assert length(get_range_rows) == 3
    {[{"key1", 1}, {"key2", _}], q2_attrs_key_1} = Enum.at(get_range_rows, 0)
    {[{"key1", 2}, {"key2", _}], nil} = Enum.at(get_range_rows, 1)
    {[{"key1", 4}, {"key2", _}], q2_attrs_key_4} = Enum.at(get_range_rows, 2)

    assert attrs_key_1 == q2_attrs_key_1
    assert attrs_key_4 == q2_attrs_key_4

    {:ok, get_range_response} =
      get_range table_name1,
        [{"key1", 1}, {"key2", PKType.inf_min}],
        [{"key1", 4}, {"key2", PKType.inf_max}],
        time_range: start_timestamp,
        direction: :forward
    get_range_rows = get_range_response.rows
    assert length(get_range_rows) == 3
    {[{"key1", 1}, {"key2", _}], q3_attrs_key_1} = Enum.at(get_range_rows, 0)
    {[{"key1", 2}, {"key2", _}], nil} = Enum.at(get_range_rows, 1)
    {[{"key1", 4}, {"key2", _}], nil} = Enum.at(get_range_rows, 2)

    assert attrs_key_1 == q3_attrs_key_1

    del_table1_reslt = delete_table table_name1
    assert del_table1_reslt == :ok
    del_table2_reslt = delete_table table_name2
    assert del_table2_reslt == :ok
  end
end
