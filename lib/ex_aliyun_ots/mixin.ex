defmodule ExAliyunOts.Mixin do

  alias ExAliyunOts.Var
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Client

  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence, FilterType, ComparatorType, LogicOperator, Direction, Search.QueryType, Search.ColumnReturnType, Search.SortType}

  require PKType
  require OperationType
  require ReturnType
  require RowExistence
  require FilterType
  require ComparatorType
  require LogicOperator
  require Direction
  require QueryType
  require ColumnReturnType
  require SortType

  @regex_filter_options ~r/^(.+?)(\[.+?\])$/

  defmacro __using__(_opts) do

    quote do

      import unquote(__MODULE__)

      def create_table(instance_key, table, pk_keys, options \\ Keyword.new()) do
        execute_create_table(instance_key, table, pk_keys, options)
      end

      def delete_table(instance_key, table, options \\ Keyword.new()) do
        execute_delete_table(instance_key, table, options)
      end

      def list_table(instance_key, options \\ Keyword.new()) do
        execute_list_table(instance_key, options)
      end

      def update_table(instance_key, table, options \\ Keyword.new()) do
        execute_update_table(instance_key, table, options)
      end

      def describe_table(instance_key, table, options \\ Keyword.new()) do
        execute_describe_table(instance_key, table, options)
      end

      def batch_get(instance_key, get_requests, options \\ Keyword.new()) do
        execute_batch_get(instance_key, get_requests, options)
      end

      def batch_write(instance_key, write_requests, options \\ Keyword.new()) do
        execute_batch_write(instance_key, write_requests, options)
      end

      def get_row(instance_key, table, pk_keys, options \\ Keyword.new()) do
        execute_get_row(instance_key, table, pk_keys, options)
      end

      def get(table, pk_keys, options \\ Keyword.new()) do
        execute_get(table, pk_keys, options)
      end

      def put_row(instance_key, table, pk_keys, attrs, options \\ Keyword.new()) do
        execute_put_row(instance_key, table, pk_keys, attrs, options)
      end

      def update_row(instance_key, table, pk_keys, options \\ Keyword.new()) do
        execute_update_row(instance_key, table, pk_keys, options)
      end

      def delete_row(instance_key, table, pk_keys, options \\ Keyword.new()) do
        execute_delete_row(instance_key, table, pk_keys, options)
      end

      def write_put(pk_keys, attrs, options \\ Keyword.new()) do
        execute_write_put(pk_keys, attrs, options)
      end

      def write_update(pk_keys, options \\ Keyword.new()) do
        execute_write_update(pk_keys, options)
      end

      def write_delete(pk_keys, options \\ Keyword.new()) do
        execute_write_delete(pk_keys, options)
      end

      def get_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ Keyword.new()) do
        execute_get_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options)
      end

      def iterate_all_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ Keyword.new()) do
        execute_iterate_all_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options)
      end

      def pagination(offset: offset, limit: limit) do
        execute_pagination(offset, limit)
      end

      def search(instance_key, table, index_name, options \\ Keyword.new()) do
        execute_search(instance_key, table, index_name, options)
      end

      def list_search_index(instance_key, table, options \\ Keyword.new()) do
        execute_list_search_index(instance_key, table, options)
      end

      def delete_search_index(instance_key, table, index_name, options \\ Keyword.new()) do
        execute_delete_search_index(instance_key, table, index_name, options)
      end

      def describe_search_index(instance_key, table, index_name, options \\ Keyword.new()) do
        execute_describe_search_index(instance_key, table, index_name, options)
      end

    end
  end

  def execute_create_table(instance_key, table, pk_keys, options) do
    var_create_table = %Var.CreateTable{
        table_name: table,
        primary_keys: pk_keys
    }
    prepared_var = map_options(var_create_table, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.create_table(instance_key, prepared_var, request_time)
    else
      Client.create_table(instance_key, prepared_var)
    end
  end

  def execute_delete_table(instance_key, table, options) do
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.delete_table(instance_key, table, request_time)
    else
      Client.delete_table(instance_key, table)
    end
  end

  def execute_list_table(instance_key, options) do
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.list_table(instance_key, request_time)
    else
      Client.list_table(instance_key)
    end
  end

  def execute_update_table(instance_key, table, options) do
    var_update_table = %Var.UpdateTable{
      table_name: table
    }
    prepared_var = map_options(var_update_table, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.update_table(instance_key, prepared_var, request_time)
    else
      Client.update_table(instance_key, prepared_var)
    end
  end

  def execute_describe_table(instance_key, table, options) do
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.describe_table(instance_key, table, request_time)
    else
      Client.describe_table(instance_key, table)
    end
  end

  def execute_batch_get(instance_key, get_requests, options) do
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.batch_get_row(instance_key, get_requests, request_time)
    else
      Client.batch_get_row(instance_key, get_requests)
    end
  end

  def execute_batch_write(instance_key, write_requests, options) do
    batch_write_requests = Enum.map(write_requests, fn({table, write_rows}) ->
      %Var.BatchWriteRequest{
        table_name: table,
        rows: write_rows
      }
    end)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.batch_write_row(instance_key, batch_write_requests, request_time)
    else
      Client.batch_write_row(instance_key, batch_write_requests)
    end
  end

  def execute_get_row(instance_key, table, pk_keys, options) do
    var_get_row = %Var.GetRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_get_row, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.get_row(instance_key, prepared_var, request_time)
    else
      Client.get_row(instance_key, prepared_var)
    end
  end

  def execute_get(table, pk_keys, options) do
    var_get_row = %Var.GetRow{
      table_name: table,
      primary_keys: pk_keys,
    }
    map_options(var_get_row, options)
  end

  def execute_put_row(instance_key, table, pk_keys, attrs, options) do
    var_put_row = %Var.PutRow{
      table_name: table,
      primary_keys: pk_keys,
      attribute_columns: attrs,
    }
    prepared_var = map_options(var_put_row, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.put_row(instance_key, prepared_var, request_time)
    else
      Client.put_row(instance_key, prepared_var)
    end
  end

  def execute_update_row(instance_key, table, pk_keys, options) do
    var_update_row = %Var.UpdateRow{
      table_name: table,
      primary_keys: pk_keys,
    }
    prepared_var =
      var_update_row
      |> map_options(options)
      |> Map.put(:updates, map_updates(options))
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.update_row(instance_key, prepared_var, request_time)
    else
      Client.update_row(instance_key, prepared_var)
    end
  end

  def execute_delete_row(instance_key, table, pk_keys, options) do
    var_delete_row = %Var.DeleteRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_delete_row, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.delete_row(instance_key, prepared_var, request_time)
    else
      Client.delete_row(instance_key, prepared_var)
    end
  end

  def execute_write_put(pk_keys, attrs, options) do
    var_batch_put_row = %Var.RowInBatchWriteRequest{
      type: OperationType.put,
      primary_keys: pk_keys,
      updates: attrs,
    }
    map_options(var_batch_put_row, options)
  end

  def execute_write_update(pk_keys, options) do
    var_batch_update_row = %Var.RowInBatchWriteRequest{
      type: OperationType.update,
      primary_keys: pk_keys,
      updates: map_updates(options)
    }
    map_options(var_batch_update_row, options)
  end

  def execute_write_delete(pk_keys, options) do
    var_batch_delete_row = %Var.RowInBatchWriteRequest{
      type: OperationType.delete,
      primary_keys: pk_keys,
    }
    map_options(var_batch_delete_row, options)
  end

  def execute_get_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) when is_list(inclusive_start_primary_keys) do
    var_get_range = %Var.GetRange{
      table_name: table,
      inclusive_start_primary_keys: inclusive_start_primary_keys,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_get_range, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.get_range(instance_key, prepared_var, nil, request_time)
    else
      Client.get_range(instance_key, prepared_var)
    end
  end
  def execute_get_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) when is_binary(inclusive_start_primary_keys) do
    var_get_range = %Var.GetRange{
      table_name: table,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_get_range, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.get_range(instance_key, prepared_var, inclusive_start_primary_keys, request_time)
    else
      Client.get_range(instance_key, prepared_var, inclusive_start_primary_keys)
    end
  end

  def execute_iterate_all_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) do
    var_iterate_all_range = %Var.GetRange{
      table_name: table,
      inclusive_start_primary_keys: inclusive_start_primary_keys,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_iterate_all_range, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.iterate_get_all_range(instance_key, prepared_var, request_time)
    else
      Client.iterate_get_all_range(instance_key, prepared_var)
    end
  end

  def execute_pagination(offset, limit) do
    %Var.Filter{
      filter_type: FilterType.column_pagination,
      filter: %Var.ColumnPaginationFilter{offset: offset, limit: limit}
    }
  end

  def execute_search(instance_key, table, index_name, options) do
    var_search_request = %Var.Search.SearchRequest{
      table_name: table,
      index_name: index_name
    }
    prepared_var = map_search_options(var_search_request, options)
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.search(instance_key, prepared_var, request_time)
    else
      Client.search(instance_key, prepared_var)
    end
  end

  def execute_list_search_index(instance_key, table, options) do
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.list_search_index(instance_key, table, request_time)
    else
      Client.list_search_index(instance_key, table)
    end
  end

  def execute_delete_search_index(instance_key, table, index_name, options) do
    var_delete_request = %Var.Search.DeleteSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.delete_search_index(instance_key, var_delete_request, request_time)
    else
      Client.delete_search_index(instance_key, var_delete_request)
    end
  end

  def execute_describe_search_index(instance_key, table, index_name, options) do
    var_describe_request = %Var.Search.DescribeSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    request_time = Keyword.get(options, :request_time)
    if request_time != nil do
      Client.describe_search_index(instance_key, var_describe_request, request_time)
    else
      Client.describe_search_index(instance_key, var_describe_request)
    end
  end

  defp map_options(var, nil) do
    var
  end
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

  defp map_search_options(var, nil) do
    var
  end
  defp map_search_options(var, options) do
    options
    |> Keyword.keys()
    |> Enum.reduce(var, fn(key, acc) ->
      value = Keyword.get(options, key)
      if value != nil and Map.has_key?(var, key) do
        case key do
          :search_query ->
            Map.put(acc, key, map_search_query(value))
          :columns_to_get ->
            Map.put(acc, key, map_columns_to_get(value))
          :sort ->
            Map.put(acc, key, map_search_sort(value))
          :must ->
            # for BoolQuery
            queries = Enum.map(value, fn(query) -> map_query_details(query) end)
            Map.put(acc, key, queries)
          :must_not ->
            # for BoolQuery
            queries = Enum.map(value, fn(query) -> map_query_details(query) end)
            Map.put(acc, key, queries)
          :filter ->
            # for BoolQuery
            queries = Enum.map(value, fn(query) -> map_query_details(query) end)
            Map.put(acc, key, queries)
          :should ->
            # for BoolQuery
            queries = Enum.map(value, fn(query) -> map_query_details(query) end)
            Map.put(acc, key, queries)
          :query ->
            # for NestedQuery
            Map.put(acc, key, map_query_details(value))
          _ ->
            Map.put(acc, key, value)
        end
      else
        acc
      end
    end)
  end

  defp map_time_range(specific_time) when is_integer(specific_time) do
    %Var.TimeRange{specific_time: specific_time}
  end
  defp map_time_range({start_time, end_time}) when is_integer(start_time) and is_integer(end_time) do
    %Var.TimeRange{start_time: start_time, end_time: end_time}
  end

  defp map_stream_spec(values) do
    is_enabled = Keyword.get(values, :is_enabled, nil)
    expiration_time = Keyword.get(values, :expiration_time, nil)
    %Var.StreamSpec{is_enabled: is_enabled, expiration_time: expiration_time}
  end

  defp map_return_type(:'RT_NONE') do
    ReturnType.none
  end
  defp map_return_type(:none) do
    ReturnType.none
  end
  defp map_return_type(:'RT_PK') do
    ReturnType.pk
  end
  defp map_return_type(:pk) do
    ReturnType.pk
  end
  defp map_return_type(:'RT_AFTER_MODIFY') do
    ReturnType.after_modify
  end
  defp map_return_type(:after_modify) do
    ReturnType.after_modify
  end
  defp map_return_type(invalid_return_type) do
    raise ExAliyunOts.Error, "invalid return_type: #{inspect invalid_return_type}"
  end

  defp map_operation_type(:'DELETE') do
    OperationType.delete
  end
  defp map_operation_type(:delete) do
    OperationType.delete
  end
  defp map_operation_type(:'DELETE_ALL') do
    OperationType.delete_all
  end
  defp map_operation_type(:delete_all) do
    OperationType.delete_all
  end
  defp map_operation_type(:'PUT') do
    OperationType.put
  end
  defp map_operation_type(:put) do
    OperationType.put
  end
  defp map_operation_type(:'INCREMENT') do
    OperationType.increment
  end
  defp map_operation_type(:increment) do
    OperationType.increment
  end
  defp map_operation_type(invalid_operation_type) do
    raise ExAliyunOts.Error, "invalid operation_type: #{inspect invalid_operation_type}"
  end

  defp map_direction(:'FORWARD') do
    Direction.forward
  end
  defp map_direction(:forward) do
    Direction.forward
  end
  defp map_direction(:'BACKWARD') do
    Direction.backward
  end
  defp map_direction(:backward) do
    Direction.backward
  end
  defp map_direction(invalid_direction) do
    raise ExAliyunOts.Error, "invalid direction: #{inspect invalid_direction}"
  end

  defp map_search_query(search_query) when is_list(search_query) do
    if not Keyword.keyword?(search_query), do: raise ExAliyunOts.Error, "input query: #{inspect search_query} required to be keyword"

    {query, rest_search_query_options} = Keyword.pop(search_query, :query, Keyword.new())

    search_query = map_search_options(%Search.SearchQuery{}, rest_search_query_options)

    var_query = map_query_details(query)

    Map.put(search_query, :query, var_query)
  end

  defp map_query_details(query) when is_list(query) do
    case Keyword.get(query, :type) do
      QueryType.match ->
        map_search_options(%Search.MatchQuery{}, query)
      QueryType.match_all ->
        map_search_options(%Search.MatchAllQuery{}, query)
      QueryType.match_phrase ->
        map_search_options(%Search.MatchPhraseQuery{}, query)
      QueryType.term ->
        map_search_options(%Search.TermQuery{}, query)
      QueryType.terms ->
        map_search_options(%Search.TermsQuery{}, query)
      QueryType.prefix ->
        map_search_options(%Search.PrefixQuery{}, query)
      QueryType.wildcard ->
        map_search_options(%Search.WildcardQuery{}, query)
      QueryType.range ->
        map_search_options(%Search.RangeQuery{}, query)
      QueryType.bool ->
        map_search_options(%Search.BoolQuery{}, query)
      QueryType.nested ->
        map_search_options(%Search.NestedQuery{}, query)
      _ ->
        raise ExAliyunOts.Error, "Not supported query when map query details: #{inspect query}"
    end
  end
  defp map_query_details(query) do
    raise ExAliyunOts.Error, "Input invalid query to map query details: #{inspect query}"
  end

  defp map_columns_to_get(value) when is_list(value) do
    %Search.ColumnsToGet{
      return_type: ColumnReturnType.specified,
      column_names: value
    }
  end
  defp map_columns_to_get({return_type, column_names}) when is_list(column_names) do
    if return_type not in [
         ColumnReturnType.all(),
         ColumnReturnType.none(),
         ColumnReturnType.specified()
       ],
       do: raise(ExAliyunOts.Error, "invalid return_type: #{inspect return_type} in columns_to_get")

    %Search.ColumnsToGet{
      return_type: return_type,
      column_names: column_names
    }
  end
  defp map_columns_to_get(ColumnReturnType.all) do
    %Search.ColumnsToGet{
      return_type: ColumnReturnType.all
    }
  end
  defp map_columns_to_get(ColumnReturnType.none) do
    %Search.ColumnsToGet{
      return_type: ColumnReturnType.none
    }
  end
  defp map_columns_to_get(value) do
    raise ExAliyunOts.Error, "invalid columns_to_get for search: #{inspect value}"
  end

  defp map_search_sort(nil) do
    nil
  end
  defp map_search_sort(sorters) when is_list(sorters) do
    Enum.map(sorters, fn(sorter) ->
      {sorter_type, rest_sorter_options} = Keyword.pop(sorter, :type)
      case sorter_type do
        SortType.field ->
          map_search_options(%Search.FieldSort{}, rest_sorter_options)
        _ ->
          raise ExAliyunOts.Error, "invalid sorter: #{inspect sorter}"
      end
    end)
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

  defmacro filter(filter_expr) do
    quote do
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      expressions_to_filter(ast_expr, context_binding)
    end
  end

  defmacro condition(existence) do
    condition = map_condition(existence)
    Macro.escape(condition)
  end
  defmacro condition(existence, filter_expr) do
    quote do
      condition = map_condition(unquote(existence))
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      column_condition = expressions_to_filter(ast_expr, context_binding)
      %{condition | column_condition: column_condition}
    end
  end

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
    raise ExAliyunOts.Error, "invalid existence: #{inspect existence} in condition"
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
        raise ExAliyunOts.Error, "filter expression: #{inspect column_content}"
    end
  end

  defp map_filter_column_value(column_value, binding) when is_tuple(column_value) do
    {column_value_bound_var, _, _} = column_value
    prepared = Keyword.get(binding, column_value_bound_var)
    if prepared == nil do
      raise ExAliyunOts.Error, "not found variables `#{column_value_bound_var}` in binding context, please check variables references in filter."
    else
      prepared
    end
  end
  defp map_filter_column_value(column_value, _binding) do
    column_value
  end

end
