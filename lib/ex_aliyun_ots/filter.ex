defmodule ExAliyunOts.Filter do
  @moduledoc false
  require ExAliyunOts.Const.FilterType, as: FilterType
  require ExAliyunOts.Const.LogicOperator, as: LogicOperator
  require ExAliyunOts.Const.ComparatorType, as: ComparatorType
  alias ExAliyunOts.PlainBuffer

  alias ExAliyunOts.TableStoreFilter.{
    Filter,
    SingleColumnValueFilter,
    CompositeColumnValueFilter,
    ColumnPaginationFilter
  }

  @comparator_mapping %{
    ==: ComparatorType.equal(),
    !=: ComparatorType.not_equal(),
    >: ComparatorType.greater_than(),
    >=: ComparatorType.greater_equal(),
    <: ComparatorType.less_than(),
    <=: ComparatorType.less_equal()
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
    combinator = Map.fetch!(LogicOperator.mapping(), combinator)

    quote do
      %Filter{
        type: unquote(FilterType.composite_column()),
        filter: %CompositeColumnValueFilter{
          combinator: unquote(combinator),
          sub_filters: unquote(sub_filters)
        }
      }
    end
  end

  defp single_filter({comparator, _, [column_name, column_value]}) do
    filter_type = FilterType.single_column()
    comparator = @comparator_mapping[comparator]

    quote location: :keep do
      case unquote(column_name) do
        {column_name, column_options} ->
          %Filter{
            type: unquote(filter_type),
            filter: %SingleColumnValueFilter{
              comparator: unquote(comparator),
              column_name: column_name,
              column_value: unquote(column_value),
              filter_if_missing: not Keyword.get(column_options, :ignore_if_missing, false),
              latest_version_only: Keyword.get(column_options, :latest_version_only, true)
            }
          }

        column_name ->
          %Filter{
            type: unquote(filter_type),
            filter: %SingleColumnValueFilter{
              comparator: unquote(comparator),
              column_name: column_name,
              column_value: unquote(column_value),
              filter_if_missing: true,
              latest_version_only: true
            }
          }
      end
    end
  end

  def serialize_filter(nil), do: nil

  def serialize_filter(filter) do
    filter
    |> do_serialize_filter()
    |> Filter.encode!()
    |> IO.iodata_to_binary()
  end

  defp do_serialize_filter(%Filter{type: FilterType.single_column(), filter: filter} = wrapper) do
    column_value = PlainBuffer.serialize_column_value(filter.column_value)
    filter = SingleColumnValueFilter.encode!(%{filter | column_value: column_value}) |> IO.iodata_to_binary()
    %{wrapper | filter: filter}
  end

  defp do_serialize_filter(%Filter{type: FilterType.composite_column(), filter: filter} = wrapper) do
    sub_filters = Enum.map(filter.sub_filters, &do_serialize_filter/1)
    filter = CompositeColumnValueFilter.encode!(%{filter | sub_filters: sub_filters}) |> IO.iodata_to_binary()
    %{wrapper | filter: filter}
  end

  defp do_serialize_filter(
         %Filter{type: FilterType.column_pagination(), filter: filter} = wrapper
       ) do
    %{wrapper | filter: ColumnPaginationFilter.encode!(filter) |> IO.iodata_to_binary()}
  end
end
