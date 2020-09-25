defmodule ExAliyunOts.DSL do
  @moduledoc false
  require ExAliyunOts.Constants, as: Constants
  alias ExAliyunOts.{Var, Filter}
  alias ExAliyunOts.TableStore.{Condition, IndexMeta}

  @type row_existence :: :expect_exist | :expect_not_exist | :ignore

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/35193.html) | [English](https://www.alibabacloud.com/help/doc-detail/35193.html)

  ## Example

      import MyApp.TableStore

      get_row table_name1, [{"key", "key1"}],
        columns_to_get: ["name", "level"],
        filter: filter(
          ({"name", ignore_if_missing: true, latest_version_only: true} == var_name and "age" > 1) or
            ("class" == "1")
        )

      batch_get [
        get(
          table_name2,
          [{"key", "key1"}],
          filter: filter "age" >= 10
        )
      ]

  ## Options

    * `ignore_if_missing`, used when attribute column not existed.
      * if a attribute column is not existed, when set `ignore_if_missing: true` in filter expression, there will ignore this row data in the returned result;
      * if a attribute column is existed, the returned result won't be affected no matter true or false was set.
    * `latest_version_only`, used when attribute column has multiple versions.
      * if set `latest_version_only: true`, there will only check the value of the latest version is matched or not, by default it's set as `latest_version_only: true`;
      * if set `latest_version_only: false`, there will check the value of all versions are matched or not.

  """
  @doc row: :row
  defmacro filter(filter_expr) do
    Filter.build_filter(filter_expr)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/35194.html) | [English](https://www.alibabacloud.com/help/doc-detail/35194.html)

  ## Example

      import MyApp.TableStore

      update_row "table", [{"pk", "pk1"}],
        delete_all: ["attr1", "attr2"],
        return_type: :pk,
        condition: condition(:expect_exist)

  The available `existence` options: `:expect_exist` | `:expect_not_exist` | `:ignore`, here are some use cases for your reference:

  Use `condition(:expect_exist)`, expect the primary keys to row is existed.
    * for `put_row/5`, if the primary keys have auto increment column type, meanwhile the target primary keys row is existed,
    only use `condition(:expect_exist)` can successfully overwrite the row.
    * for `update_row/4`, if the primary keys have auto increment column type, meanwhile the target primary keys row is existed,
    only use `condition(:expect_exist)` can successfully update the row.
    * for `delete_row/4`, no matter what primary keys type are, use `condition(:expect_exist)` can successfully delete the row.

  Use `condition(:expect_not_exist)`, expect the primary_keys to row is not existed.
    * for `put_row/5`, if the primary keys have auto increment type,
      - while the target primary keys row is existed, only use `condition(:expect_exist)` can successfully put the row;
      - while the target primary keys row is not existed, only use `condition(:ignore)` can successfully put the row.

  Use `condition(:ignore)`, ignore the row existence check
    * for `put_row/5`, if the primary keys have auto increment column type, meanwhile the target primary keys row is not existed,
    only use `condition(:ignore)` can successfully put the row.
    * for `update_row/4`, if the primary keys have auto increment column type, meanwhile the target primary keys row is not existed,
    only use `condition(:ignore)` can successfully update the row.
    * for `delete_row/4`, no matter what primary keys type are, use `condition(:ignore)` can successfully delete the row if existed.

  The `batch_write/3` operation is a collection of put_row / update_row / delete_row operations.
  """
  @doc row: :row
  @spec condition(row_existence) :: map()
  defmacro condition(row_existence) do
    quote do
      %Condition{row_existence: unquote(map_row_existence(row_existence))}
    end
  end

  @doc """
  Similar to `condition/1` and support use filter expression (please see `filter/1`) as well, please refer them for details.

  ## Example

      import MyApp.TableStore

      delete_row "table",
        [{"key", "key1"}, {"key2", "key2"}],
        condition: condition(:expect_exist, "attr_column" == "value2")

  """
  @doc row: :row
  defmacro condition(row_existence, filter_expr) do
    row_existence = map_row_existence(row_existence)
    column_condition = Filter.build_filter(filter_expr)

    quote do
      %Condition{
        row_existence: unquote(row_existence),
        column_condition: unquote(column_condition)
      }
    end
  end

  defp map_row_existence(row_existence) do
    Constants.row_existence(row_existence)
  rescue
    _ ->
      raise ExAliyunOts.RuntimeError,
            "Invalid existence: #{inspect(row_existence)} in condition, please use one of :ignore | :expect_exist | :expect_not_exist option."
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/44573.html) | [English](https://www.alibabacloud.com/help/doc-detail/44573.html)

  ## Example

      import MyApp.TableStore

      get_row table_name,
        [{"key", "1"}],
        start_column: "room",
        filter: pagination(offset: 0, limit: 3)

  Use `pagination/1` for `:filter` options when get row.
  """
  @doc row: :row
  @spec pagination(options :: Keyword.t()) :: map()
  def pagination(options) do
    offset = Keyword.get(options, :offset)
    limit = Keyword.get(options, :limit)

    %Var.Filter{
      filter_type: :FT_COLUMN_PAGINATION,
      filter: %Var.ColumnPaginationFilter{offset: offset, limit: limit}
    }
  end

  @doc false
  @spec index_meta(
          index_name :: String.t(),
          primary_keys :: [String.t()],
          defined_columns :: [String.t()]
        ) :: IndexMeta.t()
  def index_meta(index_name, primary_keys, defined_columns) do
    %IndexMeta{
      name: index_name,
      primary_key: primary_keys,
      defined_column: defined_columns,
      index_update_mode: :IUM_ASYNC_INDEX,
      index_type: :IT_GLOBAL_INDEX
    }
  end
end
