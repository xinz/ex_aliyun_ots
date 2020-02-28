defmodule ExAliyunOts do

  alias ExAliyunOts.{Var, Client}

  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence, FilterType, ComparatorType, LogicOperator, Direction}

  require PKType
  require OperationType
  require ReturnType
  require RowExistence
  require FilterType
  require ComparatorType
  require LogicOperator
  require Direction

  @regex_filter_options ~r/^(.+?)(\[.+?\])$/

  defmacro __using__(opts \\ []) do
    opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))

    quote do

      @instance Keyword.get(unquote(opts), :instance)

      use ExAliyunOts.Constants

      import ExAliyunOts, only: [
        filter: 1,
        condition: 1,
        condition: 2,
        pagination: 1
      ]

      @before_compile ExAliyunOts.Compiler
    end
  end

  defmacro filter(filter_expr) do
    quote do
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      ExAliyunOts.expressions_to_filter(ast_expr, context_binding)
    end
  end

  def condition(existence) do
    map_condition(existence)
  end

  def pagination(options) do
    offset = Keyword.get(options, :offset)
    limit = Keyword.get(options, :limit)
    %Var.Filter{
      filter_type: FilterType.column_pagination,
      filter: %Var.ColumnPaginationFilter{offset: offset, limit: limit}
    }
  end

  defmacro condition(existence, filter_expr) do
    quote do
      condition = ExAliyunOts.map_condition(unquote(existence))
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      column_condition = ExAliyunOts.expressions_to_filter(ast_expr, context_binding)
      %{condition | column_condition: column_condition}
    end
  end

  def create_table(instance, table, pk_keys, options \\ []) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_create_table, options)
    Client.create_table(instance, prepared_var)
  end

  def delete_table(instance, table) do
    Client.delete_table(instance, table)
  end

  def list_table(instance) do
    Client.list_table(instance)
  end

  def update_table(instance, table, options \\ []) do
    var_update_table = %Var.UpdateTable{
      table_name: table
    }
    prepared_var = map_options(var_update_table, options)
    Client.update_table(instance, prepared_var)
  end

  def describe_table(instance, table) do
    Client.describe_table(instance, table)
  end

  def batch_get(instance, requests) do
    Client.batch_get_row(instance, requests)
  end

  def batch_write(instance, requests, options \\ [])
  def batch_write(instance, requests, options) when is_list(requests) do
    batch_write_requests =
      Enum.map(requests, fn({table, write_rows}) ->
        %Var.BatchWriteRequest{
          table_name: table,
          rows: write_rows
        }
      end)
    Client.batch_write_row(instance, batch_write_requests, options)
  end
  def batch_write(instance, {table, write_rows}, options) do
    batch_write_request = %Var.BatchWriteRequest{
      table_name: table,
      rows: write_rows
    }
    Client.batch_write_row(instance, batch_write_request, options)
  end

  def get_row(instance, table, pk_keys, options \\ []) do
    var_get_row = %Var.GetRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_get_row, options)
    Client.get_row(instance, prepared_var)
  end

  def put_row(instance, table, pk_keys, attrs, options \\ []) do
    var_put_row = %Var.PutRow{
      table_name: table,
      primary_keys: pk_keys,
      attribute_columns: attrs,
    }
    prepared_var = map_options(var_put_row, options)
    Client.put_row(instance, prepared_var)
  end

  def update_row(instance, table, pk_keys, options \\ []) do
    prepared_var =
      %Var.UpdateRow{
        table_name: table,
        primary_keys: pk_keys,
      }
      |> map_options(options)
      |> Map.put(:updates, map_updates(options))

    Client.update_row(instance, prepared_var)
  end

  def delete_row(instance, table, pk_keys, options \\ []) do
    var_delete_row = %Var.DeleteRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_delete_row, options)
    Client.delete_row(instance, prepared_var)
  end

  def get(table, pk_keys, options \\ []) do
    var_get_row = %Var.GetRow{
      table_name: table,
      primary_keys: pk_keys,
    }
    map_options(var_get_row, options)
  end

  def write_put(pk_keys, attrs, options \\ []) do
    var_batch_put_row = %Var.RowInBatchWriteRequest{
      type: OperationType.put,
      primary_keys: pk_keys,
      updates: attrs,
    }
    map_options(var_batch_put_row, options)
  end

  def write_update(pk_keys, options \\ []) do
    var_batch_update_row = %Var.RowInBatchWriteRequest{
      type: OperationType.update,
      primary_keys: pk_keys,
      updates: map_updates(options)
    }
    map_options(var_batch_update_row, options)
  end

  def write_delete(pk_keys, options \\ []) do
    var_batch_delete_row = %Var.RowInBatchWriteRequest{
      type: OperationType.delete,
      primary_keys: pk_keys,
    }
    map_options(var_batch_delete_row, options)
  end

  def get_range(instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ [])
  def get_range(instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) when is_list(inclusive_start_primary_keys) do
    var_get_range = %Var.GetRange{
      table_name: table,
      inclusive_start_primary_keys: inclusive_start_primary_keys,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_get_range, options)
    Client.get_range(instance, prepared_var, nil)
  end
  def get_range(instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) when is_binary(inclusive_start_primary_keys) do
    var_get_range = %Var.GetRange{
      table_name: table,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_get_range, options)
    Client.get_range(instance, prepared_var, inclusive_start_primary_keys)
  end

  def iterate_all_range(instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ []) do
    var_iterate_all_range = %Var.GetRange{
      table_name: table,
      inclusive_start_primary_keys: inclusive_start_primary_keys,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_iterate_all_range, options)
    Client.iterate_get_all_range(instance, prepared_var)
  end

  def search(instance, table, index_name, options) do
    var_search_request = %Var.Search.SearchRequest{
      table_name: table,
      index_name: index_name
    }
    prepared_var = ExAliyunOts.Search.map_search_options(var_search_request, options)
    Client.search(instance, prepared_var)
  end

  def list_search_index(instance, table) do
    Client.list_search_index(instance, table)
  end

  def delete_search_index(instance, table, index_name) do
    var_delete_request = %Var.Search.DeleteSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    Client.delete_search_index(instance, var_delete_request)
  end

  def describe_search_index(instance, table, index_name) do
    var_describe_request = %Var.Search.DescribeSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    Client.describe_search_index(instance, var_describe_request)
  end

  def start_local_transaction(instance, table, partition_key) do
    var_start_local_transaction = %Var.Transaction.StartLocalTransactionRequest{
      table_name: table,
      partition_key: partition_key
    }
    Client.start_local_transaction(instance, var_start_local_transaction)
  end

  def commit_transaction(instance, transaction_id) do
    Client.commit_transaction(instance, transaction_id)
  end

  def abort_transaction(instance, transaction_id) do
    Client.abort_transaction(instance, transaction_id)
  end

  @doc false
  def map_condition(:ignore) do
    %Var.Condition{
      row_existence: RowExistence.ignore
    }
  end
  def map_condition(:expect_exist) do
    %Var.Condition{
      row_existence: RowExistence.expect_exist
    }
  end
  def map_condition(:expect_not_exist) do
    %Var.Condition{
      row_existence: RowExistence.expect_not_exist
    }
  end
  def map_condition(existence) do
    raise ExAliyunOts.RuntimeError, "Invalid existence: #{inspect existence} in condition, please use one of :ignore | :expect_exist | :expect_not_exist option."
  end

  @doc false
  def expressions_to_filter({:and, _, expressions}, binding) do
    %Var.Filter{
      filter_type: FilterType.composite_column,
      filter: %Var.CompositeColumnValueFilter{
        combinator: LogicOperator.and,
        sub_filters: Enum.map(expressions, fn(expr) -> 
          expressions_to_filter(expr, binding)
        end)
      }
    }
  end
  def expressions_to_filter({:not, _, expressions}, binding) do
    %Var.Filter{
      filter_type: FilterType.composite_column,
      filter: %Var.CompositeColumnValueFilter{
        combinator: LogicOperator.not,
        sub_filters: Enum.map(expressions, fn(expr) -> 
          expressions_to_filter(expr, binding)
        end)
      }
    }
  end
  def expressions_to_filter({:or, _, expressions}, binding) do
    %Var.Filter{
      filter_type: FilterType.composite_column,
      filter: %Var.CompositeColumnValueFilter{
        combinator: LogicOperator.or,
        sub_filters: Enum.map(expressions, fn(expr) ->
          expressions_to_filter(expr, binding)
        end)
      }
    }
  end
  def expressions_to_filter({:==, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.eq, column_name, column_value, binding)
  end
  def expressions_to_filter({:>, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.gt, column_name, column_value, binding)
  end
  def expressions_to_filter({:>=, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.ge, column_name, column_value, binding)
  end
  def expressions_to_filter({:!=, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.not_eq, column_name, column_value, binding)
  end
  def expressions_to_filter({:<, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.lt, column_name, column_value, binding)
  end
  def expressions_to_filter({:<=, _, [column_name, column_value]}, binding) do
    prepare_single_column_value_filter(ComparatorType.le, column_name, column_value, binding)
  end

  defp prepare_single_column_value_filter(comparator, column_name, column_value, binding) do
    {column_name, options} = check_signal_col_val_filter_options(column_name)
    filter = %Var.SingleColumnValueFilter{
      comparator: comparator,
      column_name: column_name,
      column_value: map_filter_column_value(column_value, binding),
    }
    filter_with_options = map_options(filter, options)
    %Var.Filter{
      filter_type: FilterType.single_column,
      filter: filter_with_options
    }
  end

  defp check_signal_col_val_filter_options(column_content) do
    case Regex.run(@regex_filter_options, column_content) do
      nil ->
        {column_content, nil}
      [_, column_name, options_str] ->
        {options, _} = Code.eval_string(options_str)
        {column_name, options}
      _ ->
        raise ExAliyunOts.RuntimeError, "filter expression: #{inspect column_content}"
    end
  end

  defp map_filter_column_value({column_value_bound_var, _, _} = ast, binding) do
    prepared = Keyword.get(binding, column_value_bound_var)
    if prepared == nil do
      raise ExAliyunOts.RuntimeError, "Invalid expression `#{Macro.to_string(ast)}` in context, please use a variable refer the value in filter expression."
    else
      prepared
    end
  end
  defp map_filter_column_value(column_value, _binding) do
    column_value
  end

  defp map_options(var, nil), do: var
  defp map_options(var, options) do
    options
    |> Keyword.keys()
    |> Enum.reduce(var, fn(key, acc) ->
      value = Keyword.get(options, key)
      if value != nil and Map.has_key?(var, key) do
        case key do
          :return_type ->
            Map.put(acc, key, map_return_type(value))
          :direction ->
            Map.put(acc, key, map_direction(value))
          :stream_spec ->
            Map.put(acc, key, map_stream_spec(value))
          :time_range ->
            Map.put(acc, key, map_time_range(value))
          _ ->
            Map.put(acc, key, value)
        end
      else
        acc
      end
    end)
  end

  defp map_return_type(:none), do: ReturnType.none
  defp map_return_type(:pk), do: ReturnType.pk
  defp map_return_type(:after_modify), do: ReturnType.after_modify
  defp map_return_type(ReturnType.none), do: ReturnType.none
  defp map_return_type(ReturnType.pk), do: ReturnType.pk
  defp map_return_type(ReturnType.after_modify), do: ReturnType.after_modify
  defp map_return_type(invalid_return_type) do
    raise ExAliyunOts.RuntimeError, "invalid return_type: #{inspect invalid_return_type}"
  end

  defp map_direction(:backward), do: Direction.backward
  defp map_direction(:forward), do: Direction.forward
  defp map_direction(Direction.backward), do: Direction.backward
  defp map_direction(Direction.forward), do: Direction.forward
  defp map_direction(invalid_direction) do
    raise ExAliyunOts.RuntimeError, "invalid direction: #{inspect invalid_direction}"
  end

  defp map_stream_spec(values) do
    is_enabled = Keyword.get(values, :is_enabled)
    expiration_time = Keyword.get(values, :expiration_time)
    %Var.StreamSpec{is_enabled: is_enabled, expiration_time: expiration_time}
  end

  defp map_time_range(specific_time) when is_integer(specific_time) do
    %Var.TimeRange{specific_time: specific_time}
  end
  defp map_time_range({start_time, end_time}) when is_integer(start_time) and is_integer(end_time) do
    %Var.TimeRange{start_time: start_time, end_time: end_time}
  end

  defp map_updates(options) do
    Enum.reduce([:delete, :delete_all, :put, :increment], %{}, fn(update_operation, acc) ->
      {matched_update, _rest_opts} = Keyword.pop(options, update_operation)
      if matched_update != nil do
        Map.put(acc, map_operation_type(update_operation), matched_update)
      else
        acc
      end
    end)
  end

  defp map_operation_type(:put), do: OperationType.put
  defp map_operation_type(:delete), do: OperationType.delete
  defp map_operation_type(:delete_all), do: OperationType.delete_all
  defp map_operation_type(:increment), do: OperationType.increment
  defp map_operation_type(OperationType.put), do: OperationType.put
  defp map_operation_type(OperationType.delete), do: OperationType.delete
  defp map_operation_type(OperationType.delete_all), do: OperationType.delete_all
  defp map_operation_type(OperationType.increment), do: OperationType.increment
  defp map_operation_type(invalid_operation_type) do
    raise ExAliyunOts.RuntimeError, "invalid operation_type: #{inspect invalid_operation_type}"
  end

end
