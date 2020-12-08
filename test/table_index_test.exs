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

    index1 =
      index_meta(table_name <> "index_1", ["integer_col"], [
        "double_col",
        "boolean_col",
        "string_col",
        "binary_col"
      ])

    index2 =
      index_meta(table_name <> "index_2", ["string_col"], [
        "integer_col",
        "double_col",
        "boolean_col",
        "binary_col"
      ])

    index3 =
      index_meta(table_name <> "index_3", ["key1", "string_col"], [
        "integer_col",
        "double_col",
        "boolean_col",
        "binary_col"
      ])

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
    assert length(describe_table_result.index_metas) == 3
    assert describe_table_result.index_metas == index_metas
    assert delete_table(table_name) == :ok
  end

  test "create table index api" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_index_#{cur_timestamp}"

    defined_columns = [
      {"integer_col", :integer},
      {"double_col", :double},
      {"boolean_col", :boolean},
      {"string_col", :string},
      {"binary_col", :binary}
    ]

    index1 =
      index_meta(table_name <> "index_1", ["integer_col"], [
        "double_col",
        "boolean_col",
        "string_col",
        "binary_col"
      ])

    index2 =
      index_meta(table_name <> "index_2", ["string_col"], [
        "integer_col",
        "double_col",
        "boolean_col",
        "binary_col"
      ])

    index3 =
      index_meta(table_name <> "index_3", ["key1", "string_col"], [
        "integer_col",
        "double_col",
        "boolean_col",
        "binary_col"
      ])

    index_metas = [index1, index2, index3]

    assert create_table(table_name, [{"key1", :string}], defined_columns: defined_columns) == :ok
    assert create_index(table_name, index1) == :ok
    assert create_index(table_name, index2) == :ok
    assert create_index(table_name, index3) == :ok

    {:ok, describe_table_result} = describe_table(table_name)
    assert length(describe_table_result.table_meta.defined_column) == 5
    assert length(describe_table_result.index_metas) == 3
    assert describe_table_result.index_metas == index_metas

    assert delete_index(table_name, index1.name) == :ok
    assert delete_index(table_name, index2.name) == :ok
    assert delete_index(table_name, index3.name) == :ok
    {:ok, describe_table_result} = describe_table(table_name)
    assert describe_table_result.index_metas == []

    assert delete_table(table_name) == :ok
  end
end
