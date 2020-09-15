defmodule ExAliyunOts.Filter do
  @moduledoc false
  require ExAliyunOts.Const.ComparatorType, as: ComparatorType
  require ExAliyunOts.Const.LogicOperator, as: LogicOperator

  @operator_mapping %{
    and: LogicOperator.and(),
    not: LogicOperator.not(),
    or: LogicOperator.or()
  }

  @comparator_mapping %{
    ==: ComparatorType.eq(),
    >: ComparatorType.gt(),
    >=: ComparatorType.ge(),
    !=: ComparatorType.not_eq(),
    <: ComparatorType.lt(),
    <=: ComparatorType.le()
  }

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/35193.html) | [English](https://www.alibabacloud.com/help/doc-detail/35193.html)

  ## Example

      import MyApp.TableStore

      get_row table_name1, [{"key", "key1"}],
        columns_to_get: ["name", "level"],
        filter: filter(({"name", ignore_if_missing: true, latest_version_only: true} == var_name and "age" > 1) or ("class" == "1"))

      batch_get [
        get(
          table_name2,
          [{"key", "key1"}],
          filter: filter "age" >= 10
        )
      ]

  ## Options

    * `ignore_if_missing`, used when attribute column not existed.
      * if a attribute column is not existed, when set `[ignore_if_missing: true]` in filter expression, there will ignore this row data in the returned result;
      * if a attribute column is existed, the returned result won't be affected no matter true or false was set.
    * `latest_version_only`, used when attribute column has multiple versions.
      * if set `[latest_version_only: true]`, there will only check the value of the latest version is matched or not, by default it's set as `[latest_version_only: true]`;
      * if set `[latest_version_only: false]`, there will check the value of all versions are matched or not.

  """
  @doc row: :row
  defmacro filter(filter_expr) do
    build_filter(filter_expr)
  end

  defp build_filter({combinator, _, _} = ast) when combinator in [:and, :not, :or] do
    composite_filter(ast)
  end

  defp build_filter({combinator, _, _} = ast) when combinator in [:==, :>, :>=, :!=, :<, :<=] do
    single_filter(ast)
  end

  defp build_filter({binding_name, _, nil} = ast) when is_atom(binding_name) do
    ast
  end

  defp build_filter(ast) do
    raise ExAliyunOts.RuntimeError, "Invalid filter expression: #{Macro.to_string(ast)}"
  end

  defp composite_filter({combinator, _, expressions}) do
    sub_filters = Enum.map(expressions, &build_filter/1)

    quote do
      require ExAliyunOts.Const.FilterType

      %ExAliyunOts.Var.Filter{
        filter_type: ExAliyunOts.Const.FilterType.composite_column(),
        filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
          combinator: unquote(@operator_mapping[combinator]),
          sub_filters: unquote(sub_filters)
        }
      }
    end
  end

  defp single_filter({comparator, _, [column_name, column_value]}) do
    {column_name, options} =
      case column_name do
        {column_name, options} -> {column_name, options}
        column_name -> {column_name, []}
      end

    quote do
      require ExAliyunOts.Const.FilterType

      %ExAliyunOts.Var.Filter{
        filter_type: ExAliyunOts.Const.FilterType.single_column(),
        filter: %ExAliyunOts.Var.SingleColumnValueFilter{
          comparator: unquote(@comparator_mapping[comparator]),
          column_name: unquote(column_name),
          column_value: unquote(column_value),
          ignore_if_missing: Keyword.get(unquote(options), :ignore_if_missing, false),
          latest_version_only: Keyword.get(unquote(options), :latest_version_only, true)
        }
      }
    end
  end
end
