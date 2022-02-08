defmodule ExAliyunOts.Filter do
  @moduledoc false
  require ExAliyunOts.Const.FilterType, as: FilterType
  require ExAliyunOts.Const.LogicOperator, as: LogicOperator
  require ExAliyunOts.Const.ComparatorType, as: ComparatorType
  require ExAliyunOts.Const.VariantType, as: VariantType
  alias ExAliyunOts.PlainBuffer

  alias ExAliyunOts.TableStoreFilter.{
    Filter,
    SingleColumnValueFilter,
    CompositeColumnValueFilter,
    ColumnPaginationFilter,
    ValueTransferRule
  }

  @comparator_mapping %{
    ==: ComparatorType.equal(),
    !=: ComparatorType.not_equal(),
    >: ComparatorType.greater_than(),
    >=: ComparatorType.greater_equal(),
    <: ComparatorType.less_than(),
    <=: ComparatorType.less_equal()
  }

  @doc ~S"""
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

      put_row(table_name1, [{"key", "key1"}], [{"type", "t:5"}])

      # Use `~r/\d+/` regex expression to fetch the matched part (in this case it is "5") from
      # the attribute column field, and then cast it into an integer for the "==" comparator.
      #
      get_row table_name1, [{"key", "key1"}],
        filter: filter(
          {"type", value_trans_rule: {~r/\d+/, :integer}} == 5
        )

  ## Options

    * `ignore_if_missing`, used when attribute column not existed.
      * if a attribute column is not existed, when set `ignore_if_missing: true` in filter expression, there will ignore this row data in the returned result;
      * if a attribute column is existed, the returned result won't be affected no matter true or false was set.
    * `latest_version_only`, used when attribute column has multiple versions.
      * if set `latest_version_only: true`, there will only check the value of the latest version is matched or not, by default it's set as `latest_version_only: true`;
      * if set `latest_version_only: false`, there will check the value of all versions are matched or not.
    * `value_trans_rule`, optional, a two-element tuple contains a `Regex` expression and one of [:integer, :double, :string] atom as a cast type, the regex expression
      matched part will be cast into the corresponding type and then use it into the current condition comparator.
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
        type: FilterType.composite_column(),
        filter: %CompositeColumnValueFilter{
          combinator: unquote(combinator),
          sub_filters: unquote(sub_filters)
        }
      }
    end
  end

  defp single_filter({comparator, _, [column_name, column_value]}) do
    comparator = @comparator_mapping[comparator]

    quote location: :keep, bind_quoted: [column_name: column_name, column_value: column_value, comparator: comparator] do
      case column_name do
        {column_name, column_options} ->
          %Filter{
            type: FilterType.single_column(),
            filter: %SingleColumnValueFilter{
              comparator: comparator,
              column_name: column_name,
              column_value: column_value,
              filter_if_missing: not (column_options[:ignore_if_missing] || false),
              latest_version_only: column_options[:latest_version_only] || true,
              value_trans_rule: ExAliyunOts.Filter.value_transfer_rule(column_options[:value_trans_rule])
            }
          }

        column_name ->
          %Filter{
            type: FilterType.single_column(),
            filter: %SingleColumnValueFilter{
              comparator: comparator,
              column_name: column_name,
              column_value: column_value,
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

  @doc false
  def value_transfer_rule({%Regex{} = regex, cast_type}) do
    with {:ok, cast_type} <- map_cast_type!(cast_type) do
      %ValueTransferRule{
        regex: Regex.source(regex),
        cast_type: cast_type
      }
    end
  end
  def value_transfer_rule(nil), do: nil
  def value_transfer_rule(invalid_rule) do
    raise ExAliyunOts.RuntimeError, "Invalid value_trans_rule: #{inspect invalid_rule}, expect it is a two-element tuple as {Regex.t(), :double | :string | :integer}"
  end

  defp map_cast_type!(:double), do: {:ok, VariantType.double()}
  defp map_cast_type!(:string), do: {:ok, VariantType.string()}
  defp map_cast_type!(:integer), do: {:ok, VariantType.integer()}
  defp map_cast_type!(unexpected) do
    raise ExAliyunOts.RuntimeError, "Invalid cast type: #{inspect unexpected} to value_trans_rule, please use :double | :string | :integer"
  end

end
