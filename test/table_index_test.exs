defmodule ExAliyunOtsTest.TableIndexTest do
  use ExUnit.Case
  use ExAliyunOts, instance: EDCEXTestInstance
  require Logger

  test "create table index" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_#{cur_timestamp}"

    defined_columns = [
      {"integer_col", :integer},
      {"double_col", :double},
      {"boolean_col", :boolean},
      {"string_col", :string},
      {"binary_col", :binary}
    ]

    index1_name = table_name <> "index_1"
    index2_name = table_name <> "index_2"
    index3_name = table_name <> "index_3"

    index1 =
      {
        index1_name,
        ["integer_col"],
        [
          "double_col",
          "boolean_col",
          "string_col",
          "binary_col"
        ]
      }

    index2 =
      {
        index2_name,
        ["string_col"],
        [
          "integer_col",
          "double_col",
          "boolean_col",
          "binary_col"
        ]
      }

    index3 =
      {
        index3_name,
        ["key1", "string_col"],
        [
          "integer_col",
          "double_col",
          "boolean_col",
          "binary_col"
        ]
      }

    index_metas = [index1, index2, index3]

    create_table_result =
      create_table(table_name, [{"key1", :string}],
        defined_columns: defined_columns,
        index_metas: index_metas
      )

    assert create_table_result == :ok

    {:ok, describe_table_result} = describe_table(table_name)
    assert describe_table_result.table_meta.table_name == table_name
    assert length(describe_table_result.table_meta.defined_column) == 5
    [index_meta1_from_describe_table, index_meta2_from_describe_table, index_meta3_from_describe_table] = describe_table_result.index_metas

    assert index_meta1_from_describe_table.name == index1_name
    assert index_meta2_from_describe_table.name == index2_name
    assert index_meta3_from_describe_table.name == index3_name

    assert delete_table(table_name) == :ok
  end

  test "create table and then create index api" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_index_#{cur_timestamp}"

    defined_columns = [
      {"integer_col", :integer},
      {"double_col", :double},
      {"boolean_col", :boolean},
      {"string_col", :string},
      {"binary_col", :binary}
    ]

    index1_name = table_name <> "index_1"
    index2_name = table_name <> "index_2"
    index3_name = table_name <> "index_3"

    index1_pks = ["integer_col"]
    index2_pks = ["string_col"]
    index3_pks = ["key1", "string_col"]

    index1 =
      {
        index1_name,
        index1_pks,
        [
          "double_col",
          "boolean_col",
          "string_col",
          "binary_col"
        ]
      }

    index2 =
      {
        index2_name,
        index2_pks,
        [
          "integer_col",
          "double_col",
          "boolean_col",
          "binary_col"
        ]
      }

    index3 =
      {
        index3_name,
        index3_pks,
        [
          "integer_col",
          "double_col",
          "boolean_col",
          "binary_col"
        ]
      }

    assert create_table(table_name, [{"key1", :string}], defined_columns: defined_columns) == :ok
    assert create_index(table_name, elem(index1, 0), elem(index1, 1), elem(index1, 2)) == :ok
    assert create_index(table_name, elem(index2, 0), elem(index2, 1), elem(index2, 2)) == :ok
    assert create_index(table_name, elem(index3, 0), elem(index3, 1), elem(index3, 2)) == :ok

    {:ok, describe_table_result} = describe_table(table_name)
    assert length(describe_table_result.table_meta.defined_column) == 5
    assert length(describe_table_result.index_metas) == 3
    [index_meta1_from_describe_table, index_meta2_from_describe_table, index_meta3_from_describe_table] = describe_table_result.index_metas

    assert index_meta1_from_describe_table.primary_key == index1_pks
    assert index_meta2_from_describe_table.primary_key == index2_pks
    assert index_meta3_from_describe_table.primary_key == index3_pks

    assert delete_index(table_name, index1_name) == :ok
    assert delete_index(table_name, index2_name) == :ok
    assert delete_index(table_name, index3_name) == :ok
    {:ok, describe_table_result} = describe_table(table_name)
    assert describe_table_result.index_metas == []

    assert delete_table(table_name) == :ok
  end
end
