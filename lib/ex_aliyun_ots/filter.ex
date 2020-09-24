defmodule ExAliyunOts.Filter do
  @moduledoc false

  @operator_mapping %{
    and: :LO_AND,
    not: :LO_NOT,
    or: :LO_OR
  }

  @comparator_mapping %{
    ==: :CT_EQUAL,
    !=: :CT_NOT_EQUAL,
    >: :CT_GREATER_THAN,
    >=: :CT_GREATER_EQUAL,
    <: :CT_LESS_THAN,
    <=: :CT_LESS_EQUAL
  }

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
    build_filter(filter_expr)
  end

  @doc false
  def build_filter({combinator, _, _} = ast) when combinator in [:and, :not, :or] do
    composite_filter(ast)
  end

  def build_filter({combinator, _, _} = ast) when combinator in [:==, :>, :>=, :!=, :<, :<=] do
    single_filter(ast)
  end

  def build_filter({binding_name, _, nil} = ast) when is_atom(binding_name) do
    ast
  end

  def build_filter(ast) do
    raise ExAliyunOts.RuntimeError, "Invalid filter expression: #{Macro.to_string(ast)}"
  end

  defp composite_filter({combinator, _, expressions}) do
    sub_filters = Enum.map(expressions, &build_filter/1)

    quote do
      %ExAliyunOts.Var.Filter{
        filter_type: :FT_COMPOSITE_COLUMN_VALUE,
        filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
          combinator: unquote(@operator_mapping[combinator]),
          sub_filters: unquote(sub_filters)
        }
      }
    end
  end

  defp single_filter({comparator, _, [column_name, column_value]}) do
    quote location: :keep do
      comparator = unquote(@comparator_mapping[comparator])

      case unquote(column_name) do
        {column_name, column_options} ->
          %ExAliyunOts.Var.Filter{
            filter_type: :FT_SINGLE_COLUMN_VALUE,
            filter: %ExAliyunOts.Var.SingleColumnValueFilter{
              comparator: comparator,
              column_name: column_name,
              column_value: unquote(column_value),
              ignore_if_missing: Keyword.get(column_options, :ignore_if_missing, false),
              latest_version_only: Keyword.get(column_options, :latest_version_only, true)
            }
          }

        column_name ->
          %ExAliyunOts.Var.Filter{
            filter_type: :FT_SINGLE_COLUMN_VALUE,
            filter: %ExAliyunOts.Var.SingleColumnValueFilter{
              comparator: comparator,
              column_name: column_name,
              column_value: unquote(column_value),
              ignore_if_missing: false,
              latest_version_only: true
            }
          }
      end
    end
  end
end
