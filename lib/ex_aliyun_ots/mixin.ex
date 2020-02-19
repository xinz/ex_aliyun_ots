defmodule ExAliyunOts.Mixin do

  alias ExAliyunOts.Var
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Client

  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence, FilterType, ComparatorType, LogicOperator, Direction, Search.QueryType, Search.ColumnReturnType, Search.SortType, Search.AggregationType, Search.GroupByType, Search.SortOrder}

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
  require AggregationType
  require GroupByType
  require SortOrder

  @regex_filter_options ~r/^(.+?)(\[.+?\])$/

  defmacro __using__(_opts) do

    quote do

      import unquote(__MODULE__)

      def create_table(instance_key, table, pk_keys, options \\ Keyword.new()) do
        execute_create_table(instance_key, table, pk_keys, options)
      end

      def delete_table(instance_key, table) do
        execute_delete_table(instance_key, table)
      end

      def list_table(instance_key) do
        execute_list_table(instance_key)
      end

      def update_table(instance_key, table, options \\ Keyword.new()) do
        execute_update_table(instance_key, table, options)
      end

      def describe_table(instance_key, table) do
        execute_describe_table(instance_key, table)
      end

      def batch_get(instance_key, get_requests) do
        execute_batch_get(instance_key, get_requests)
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

      def list_search_index(instance_key, table) do
        execute_list_search_index(instance_key, table)
      end

      def delete_search_index(instance_key, table, index_name) do
        execute_delete_search_index(instance_key, table, index_name)
      end

      def describe_search_index(instance_key, table, index_name) do
        execute_describe_search_index(instance_key, table, index_name)
      end

      def start_local_transaction(instance_key, table, partition_key) do
        execute_start_local_transaction(instance_key, table, partition_key)
      end

      def commit_transaction(instance_key, transaction_id) do
        execute_commit_transaction(instance_key, transaction_id)
      end

      def abort_transaction(instance_key, transaction_id) do
        execute_abort_transaction(instance_key, transaction_id)
      end

    end
  end

  def execute_create_table(instance_key, table, pk_keys, options) do
    var_create_table = %Var.CreateTable{
        table_name: table,
        primary_keys: pk_keys
    }
    prepared_var = map_options(var_create_table, options)
    Client.create_table(instance_key, prepared_var)
  end

  def execute_delete_table(instance_key, table) do
    Client.delete_table(instance_key, table)
  end

  def execute_list_table(instance_key) do
    Client.list_table(instance_key)
  end

  def execute_update_table(instance_key, table, options) do
    var_update_table = %Var.UpdateTable{
      table_name: table
    }
    prepared_var = map_options(var_update_table, options)
    Client.update_table(instance_key, prepared_var)
  end

  def execute_describe_table(instance_key, table) do
    Client.describe_table(instance_key, table)
  end

  def execute_batch_get(instance_key, get_requests) do
    Client.batch_get_row(instance_key, get_requests)
  end

  def execute_batch_write(instance_key, write_requests, options) when is_list(write_requests) do
    batch_write_requests = Enum.map(write_requests, fn({table, write_rows}) ->
      %Var.BatchWriteRequest{
        table_name: table,
        rows: write_rows
      }
    end)
    Client.batch_write_row(instance_key, batch_write_requests, options)
  end
  def execute_batch_write(instance_key, {table, write_rows}, options) do
    batch_write_request = %Var.BatchWriteRequest{
      table_name: table,
      rows: write_rows
    }
    Client.batch_write_row(instance_key, batch_write_request, options)
  end

  def execute_get_row(instance_key, table, pk_keys, options) do
    var_get_row = %Var.GetRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_get_row, options)
    Client.get_row(instance_key, prepared_var)
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
    Client.put_row(instance_key, prepared_var)
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
    
    Client.update_row(instance_key, prepared_var)
  end

  def execute_delete_row(instance_key, table, pk_keys, options) do
    var_delete_row = %Var.DeleteRow{
      table_name: table,
      primary_keys: pk_keys
    }
    prepared_var = map_options(var_delete_row, options)
    Client.delete_row(instance_key, prepared_var)
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
    Client.get_range(instance_key, prepared_var, nil)
  end
  def execute_get_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) when is_binary(inclusive_start_primary_keys) do
    var_get_range = %Var.GetRange{
      table_name: table,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_get_range, options)
    Client.get_range(instance_key, prepared_var, inclusive_start_primary_keys)
  end

  def execute_iterate_all_range(instance_key, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options) do
    var_iterate_all_range = %Var.GetRange{
      table_name: table,
      inclusive_start_primary_keys: inclusive_start_primary_keys,
      exclusive_end_primary_keys: exclusive_end_primary_keys
    }
    prepared_var = map_options(var_iterate_all_range, options)
    Client.iterate_get_all_range(instance_key, prepared_var)
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
    Client.search(instance_key, prepared_var)
  end

  def execute_list_search_index(instance_key, table) do
    Client.list_search_index(instance_key, table)
  end

  def execute_delete_search_index(instance_key, table, index_name) do
    var_delete_request = %Var.Search.DeleteSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    Client.delete_search_index(instance_key, var_delete_request)
  end

  def execute_describe_search_index(instance_key, table, index_name) do
    var_describe_request = %Var.Search.DescribeSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    Client.describe_search_index(instance_key, var_describe_request)
  end

  def execute_start_local_transaction(instance_key, table, partition_key) do
    var_start_local_transaction = %Var.Transaction.StartLocalTransactionRequest{
      table_name: table,
      partition_key: partition_key
    }
    Client.start_local_transaction(instance_key, var_start_local_transaction)
  end

  def execute_commit_transaction(instance_key, transaction_id) do
    Client.commit_transaction(instance_key, transaction_id)
  end

  def execute_abort_transaction(instance_key, transaction_id) do
    Client.abort_transaction(instance_key, transaction_id)
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
        do_map_search_options(key, value, acc)
      else
        acc
      end
    end)
  end

  defp do_map_search_options(:search_query = key, value, var) do
    Map.put(var, key, map_search_query(value))
  end
  defp do_map_search_options(:columns_to_get = key, value, var) do
    Map.put(var, key, map_columns_to_get(value))
  end
  defp do_map_search_options(:sort = key, value, var) do
    Map.put(var, key, map_search_sort(value))
  end
  defp do_map_search_options(:must = key, value, var) when is_list(value) do
    # for BoolQuery within `must` items list
    queries = Enum.map(value, fn(query) -> map_query_details(query) end)
    Map.put(var, key, queries)
  end
  defp do_map_search_options(:must = key, value, var) when is_map(value) do
    # for BoolQuery within a single `must` item
    Map.put(var, key, [value])
  end
  defp do_map_search_options(:must_not = key, value, var) when is_list(value) do
    # for BoolQuery within `must_not` items list
    queries = Enum.map(value, fn(query) -> map_query_details(query) end)
    Map.put(var, key, queries)
  end
  defp do_map_search_options(:must_not = key, value, var) when is_map(value) do
    # for BoolQuery within a single `must_not` item
    Map.put(var, key, [value])
  end
  defp do_map_search_options(:filter = key, value, var) when is_list(value) do
    # for BoolQuery within `filters` items list
    queries = Enum.map(value, fn(query) -> map_query_details(query) end)
    Map.put(var, key, queries)
  end
  defp do_map_search_options(:filters = key, value, var) when is_map(value) do
    # for BoolQuery within a single `filters` item
    Map.put(var, key, [value])
  end
  defp do_map_search_options(:should = key, value, var) when is_list(value) do
    # for BoolQuery within `should` items list
    queries = Enum.map(value, fn(query) -> map_query_details(query) end)
    Map.put(var, key, queries)
  end
  defp do_map_search_options(:should = key, value, var) when is_map(value) do
    # for BoolQuery within a single `should` item
    Map.put(var, key, [value])
  end
  defp do_map_search_options(:query = key, value, var) do
    # for NestedQuery
    Map.put(var, key, map_query_details(value))
  end
  defp do_map_search_options(key, value, var) do
    Map.put(var, key, value)
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
    raise ExAliyunOts.RuntimeError, "invalid return_type: #{inspect invalid_return_type}"
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
    raise ExAliyunOts.RuntimeError, "invalid operation_type: #{inspect invalid_operation_type}"
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
    raise ExAliyunOts.RuntimeError, "invalid direction: #{inspect invalid_direction}"
  end

  defp map_search_query(search_query) when is_list(search_query) do
    if not Keyword.keyword?(search_query), do: raise ExAliyunOts.RuntimeError, "input query: #{inspect search_query} required to be keyword"

    {query, rest_search_query_options} = Keyword.pop(search_query, :query, Keyword.new())

    search_query = map_search_options(%Search.SearchQuery{}, rest_search_query_options)

    var_query = map_query_details(query)

    Map.put(search_query, :query, var_query)
  end

  defp map_query_details([query]) when is_map(query) do
    query
  end
  defp map_query_details(query) when is_list(query) do
    query_type = Keyword.get(query, :type)
    map_query_details(query_type, query)
  end
  defp map_query_details(query) when is_map(query) do
    query
  end
  defp map_query_details(query) do
    raise ExAliyunOts.RuntimeError, "Input invalid query to map query details: #{inspect query}"
  end

  defp map_query_details(QueryType.match, query) do
    map_search_options(%Search.MatchQuery{}, query)
  end
  defp map_query_details(QueryType.match_all, query) do
    map_search_options(%Search.MatchAllQuery{}, query)
  end
  defp map_query_details(QueryType.match_phrase, query) do
    map_search_options(%Search.MatchPhraseQuery{}, query)
  end
  defp map_query_details(QueryType.term, query) do
    map_search_options(%Search.TermQuery{}, query)
  end
  defp map_query_details(QueryType.terms, query) do
    map_search_options(%Search.TermsQuery{}, query)
  end
  defp map_query_details(QueryType.prefix, query) do
    map_search_options(%Search.PrefixQuery{}, query)
  end
  defp map_query_details(QueryType.wildcard, query) do
    map_search_options(%Search.WildcardQuery{}, query)
  end
  defp map_query_details(QueryType.range, query) do
    map_search_options(%Search.RangeQuery{}, query)
  end
  defp map_query_details(QueryType.bool, query) do
    map_search_options(%Search.BoolQuery{}, query)
  end
  defp map_query_details(QueryType.nested, query) do
    map_search_options(%Search.NestedQuery{}, query)
  end
  defp map_query_details(QueryType.exists, query) do
    map_search_options(%Search.ExistsQuery{}, query)
  end
  defp map_query_details(_query_type, query) do
    raise ExAliyunOts.RuntimeError, "Not supported query when map query details: #{inspect query}"
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
       do: raise(ExAliyunOts.RuntimeError, "invalid return_type: #{inspect return_type} in columns_to_get")

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
    raise ExAliyunOts.RuntimeError, "invalid columns_to_get for search: #{inspect value}"
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
          raise ExAliyunOts.RuntimeError, "invalid sorter: #{inspect sorter}"
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

  def agg_min(agg_name, field_name, opts \\ []) do
    %Search.Aggregation{
      type: AggregationType.min,
      name: agg_name,
      field_name: field_name,
      missing: Keyword.get(opts, :missing)
    }
  end

  def agg_max(agg_name, field_name, opts \\ []) do
    %Search.Aggregation{
      type: AggregationType.max,
      name: agg_name,
      field_name: field_name,
      missing: Keyword.get(opts, :missing)
    }
  end

  def agg_avg(agg_name, field_name, opts \\ []) do
    %Search.Aggregation{
      type: AggregationType.avg,
      name: agg_name,
      field_name: field_name,
      missing: Keyword.get(opts, :missing)
    }
  end

  def agg_distinct_count(agg_name, field_name, opts \\ []) do
    %Search.Aggregation{
      type: AggregationType.distinct_count,
      name: agg_name,
      field_name: field_name,
      missing: Keyword.get(opts, :missing)
    }
  end

  def agg_sum(agg_name, field_name, opts \\ []) do
    %Search.Aggregation{
      type: AggregationType.sum,
      name: agg_name,
      field_name: field_name,
      missing: Keyword.get(opts, :missing)
    }
  end

  def agg_count(agg_name, field_name) do
    %Search.Aggregation{
      type: AggregationType.count,
      name: agg_name,
      field_name: field_name,
    }
  end

  def group_by_field(group_name, field_name, opts \\ []) do
    %Search.GroupByField{
      name: group_name,
      field_name: field_name,
      size: Keyword.get(opts, :size),
      sub_aggs: Keyword.get(opts, :sub_aggs),
      sub_group_bys: Keyword.get(opts, :sub_group_bys),
      sort: Keyword.get(opts, :sort)
    }
  end

  def group_by_range(group_name, field_name, opts \\ []) do
    %Search.GroupByRange{
      name: group_name,
      field_name: field_name,
      sub_aggs: Keyword.get(opts, :sub_aggs),
      sub_group_bys: Keyword.get(opts, :sub_group_bys),
      ranges: Keyword.get(opts, :ranges)
    }
  end

  def group_by_filter(group_name, filters, opts \\ []) when is_list(filters) do
    %Search.GroupByFilter{
      name: group_name,
      filters: filters,
      sub_aggs: Keyword.get(opts, :sub_aggs),
      sub_group_bys: Keyword.get(opts, :sub_group_bys)
    }
  end

  def group_key_sort(order)
      when order == SortOrder.desc
      when order == :desc do
    %Search.GroupKeySort{order: SortOrder.desc}
  end
  def group_key_sort(order)
      when order == SortOrder.asc
      when order == :asc do
    %Search.GroupKeySort{order: SortOrder.asc}
  end
  def group_key_sort(invalid) do
    raise ExAliyunOts.RuntimeError, "invalid sort order: #{inspect invalid}, please use `:desc` or `:asc`"
  end

  def row_count_sort(order)
      when order == SortOrder.desc
      when order == :desc do
    %Search.RowCountSort{order: SortOrder.desc}
  end
  def row_count_sort(order)
      when order == SortOrder.asc
      when order == :asc do
    %Search.RowCountSort{order: SortOrder.asc}
  end
  def row_count_sort(invalid) do
    raise ExAliyunOts.RuntimeError, "invalid sort order: #{inspect invalid}, please use `:desc` or `:asc`"
  end

  def sub_agg_sort(sub_agg_name, _)
      when is_bitstring(sub_agg_name) == false
      when sub_agg_name == "" do
    raise ExAliyunOts.RuntimeError, "require sub_agg_name as a string, but input \"#{inspect sub_agg_name}\""
  end
  def sub_agg_sort(sub_agg_name, order)
      when order == SortOrder.desc
      when order == :desc do
    %Search.SubAggSort{sub_agg_name: sub_agg_name, order: SortOrder.desc}
  end
  def sub_agg_sort(sub_agg_name, order)
      when is_bitstring(sub_agg_name) and order == SortOrder.asc
      when is_bitstring(sub_agg_name) and order == :asc do
    %Search.SubAggSort{sub_agg_name: sub_agg_name}
  end
  def sub_agg_sort(_sub_agg_name, invalid) do
    raise ExAliyunOts.RuntimeError, "invalid sort order: #{inspect invalid}, please use `:desc` or `:asc`"
  end

  def match_query(field_name, text, opts \\ []) do
    %Search.MatchQuery{
      field_name: field_name,
      text: text,
      minimum_should_match: Keyword.get(opts, :minimum_should_match, 1),
    }
  end

  def match_all_query() do
    %Search.MatchAllQuery{}
  end

  def match_phrase_query(field_name, text) do
    %Search.MatchPhraseQuery{field_name: field_name, text: text}
  end

  def term_query(field_name, term) do
    %Search.TermQuery{field_name: field_name, term: term}
  end

  def terms_query(field_name, terms) when is_list(terms) do
    %Search.TermsQuery{field_name: field_name, terms: terms}
  end

  def prefix_query(field_name, prefix) do
    %Search.PrefixQuery{field_name: field_name, prefix: prefix}
  end

  def range_query(field_name, opts \\ []) do
    %Search.RangeQuery{
      field_name: field_name,
      from: Keyword.get(opts, :from),
      to: Keyword.get(opts, :to),
      include_lower: Keyword.get(opts, :include_lower, true),
      include_upper: Keyword.get(opts, :include_upper, true)
    }
  end

  def wildcard_query(field_name, value) do
    %Search.WildcardQuery{field_name: field_name, value: value}
  end

  def bool_query(opts) do
    map_search_options(%Search.BoolQuery{}, opts)
  end

  def nested_query(path, query, opts \\ []) do
    opts = Keyword.merge(opts, [path: path, query: query])
    map_search_options(%Search.NestedQuery{}, opts)
  end

  def exists_query(field_name) do
    %Search.ExistsQuery{field_name: field_name}
  end

  defmacro filter(filter_expr) do
    quote do
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      ExAliyunOts.Mixin.expressions_to_filter(ast_expr, context_binding)
    end
  end

  defmacro condition(existence) do
    condition = map_condition(existence)
    Macro.escape(condition)
  end
  defmacro condition(existence, filter_expr) do
    quote do
      condition = ExAliyunOts.Mixin.map_condition(unquote(existence))
      ast_expr = unquote(Macro.escape(filter_expr))
      context_binding = binding()
      column_condition = ExAliyunOts.Mixin.expressions_to_filter(ast_expr, context_binding)
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
    raise ExAliyunOts.RuntimeError, "invalid existence: #{inspect existence} in condition"
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

  defp map_filter_column_value(column_value, binding) when is_tuple(column_value) do
    {column_value_bound_var, _, _} = column_value
    prepared = Keyword.get(binding, column_value_bound_var)
    if prepared == nil do
      raise ExAliyunOts.RuntimeError, "not found variables `#{column_value_bound_var}` in binding context, please check variables references in filter."
    else
      prepared
    end
  end
  defp map_filter_column_value(column_value, _binding) do
    column_value
  end

end
