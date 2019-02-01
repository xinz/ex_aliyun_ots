defmodule ExAliyunOts.Client.Search do
  # SearchIndex functions
  require Logger

  alias ExAliyunOts.TableStoreSearch.{
    CreateSearchIndexRequest,
    IndexSchema,
    FieldSchema,
    FieldSort,
    Sorter,
    Sort,
    CreateSearchIndexRequest,
    CreateSearchIndexResponse,
    ColumnsToGet,
    IndexSetting,
    SearchQuery,
    SearchRequest,
    SearchResponse,
    Query,
    MatchQuery,
    MatchAllQuery,
    MatchPhraseQuery,
    TermQuery,
    TermsQuery,
    PrefixQuery,
    WildcardQuery,
    RangeQuery,
    BoolQuery,
    NestedQuery
  }

  alias ExAliyunOts.Http
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.Search.{FieldType, SortOrder, QueryType, ScoreMode}

  require FieldType
  require SortOrder
  require QueryType
  require ScoreMode

  @variant_type_integer 0x0
  @variant_type_double 0x1
  @variant_type_boolean 0x2
  @variant_type_string 0x3

  def request_to_create_search_index(%Search.CreateSearchIndexRequest{
        table_name: table_name,
        index_name: index_name,
        index_schema: index_schema
      }) do
    proto_field_schemas =
      Enum.map(index_schema.field_schemas, fn field_schema ->
        iterate_all_field_schemas(field_schema)
      end)

    proto_index_setting = prepare_index_setting(index_schema.index_setting)
    proto_sort = prepare_sort(index_schema.index_sorts)

    proto_index_schema =
      IndexSchema.new(
        index_sort: proto_sort,
        field_schemas: proto_field_schemas,
        index_setting: proto_index_setting
      )

    request =
      CreateSearchIndexRequest.new(
        schema: proto_index_schema,
        table_name: table_name,
        index_name: index_name
      )

    CreateSearchIndexRequest.encode(request)
  end

  def remote_create_search_index(instance, request_body) do
    result =
      instance
      |> Http.client("/CreateSearchIndex", request_body, &CreateSearchIndexResponse.decode/1)
      |> Http.post()

    Logger.info(fn -> "create_search_index result: #{inspect(result)}" end)
    result
  end

  def request_to_search(%Search.SearchRequest{
        table_name: table_name,
        index_name: index_name,
        columns_to_get: %Search.ColumnsToGet{
          return_type: return_type,
          column_names: column_names
        },
        search_query: search_query,
      }) do
    proto_search_query =
      SearchQuery.new(
          offset: search_query.offset,
          limit: search_query.limit,
          query: prepare_query(search_query.query),
          sort: prepare_sort(search_query.sort),
          get_total_count: search_query.get_total_count,
          token: search_query.token
        )
    proto_columns_to_get =
      ColumnsToGet.new(
        return_type: return_type,
        column_names: column_names
      )
    request =
      SearchRequest.new(
        table_name: table_name,
        index_name: index_name,
        columns_to_get: proto_columns_to_get,
        search_query: SearchQuery.encode(proto_search_query)
      )
    SearchRequest.encode(request)
  end

  def remote_search(instance, request_body) do
    result =
      instance
      |> Http.client("/Search", request_body, &SearchResponse.decode/1)
      |> Http.post()
    result
  end

  defp term_to_bytes(term) when is_bitstring(term) do
    <<@variant_type_string, byte_size(term)::little-integer-size(32), term::binary>>
  end
  defp term_to_bytes(term) when is_integer(term) do
    <<@variant_type_integer, term::little-integer-size(64)>>
  end
  defp term_to_bytes(term) when is_float(term) do
    <<@variant_type_double, term::float-little>>
  end
  defp term_to_bytes(true) do
    <<@variant_type_boolean, 1>>
  end
  defp term_to_bytes(false) do
    <<@variant_type_boolean, 0>>
  end
  defp term_to_bytes(term) do
    raise ExAliyunOts.Error, "invalid type of term: #{inspect term}, please use string/integer/float/boolean."
  end

  defp iterate_all_field_schemas(var_field_schema) do
    field_type = var_field_schema.field_type
    sub_field_schemas = var_field_schema.field_schemas
    size_sub_field_schemas = length(sub_field_schemas)

    if field_type == FieldType.nested() and (size_sub_field_schemas == 0 and size_sub_field_schemas > 25) do
      raise ExAliyunOts.Error, "Invalid nested type field schema with : #{size_sub_field_schemas} sub field schemas, the valid range size of sub field schemas is [1, 25]"
    end

    proto_field_schema =
      FieldSchema.new(
        field_name: var_field_schema.field_name,
        field_type: var_field_schema.field_type,
        index: var_field_schema.index,
        doc_values: var_field_schema.enable_sort_and_agg,
        store: var_field_schema.store
      )

    cond do
      field_type == FieldType.nested() ->
        prepared_sub_field_schemas =
          Enum.map(sub_field_schemas, fn sub_field_schema ->
            if sub_field_schema.field_type == FieldType.nested() do
              raise ExAliyunOts.Error, "Mapping depth in the nested attribute column only supports one level, cannot nest the nested type of field schema as the sub field schemas"
            else
              iterate_all_field_schemas(sub_field_schema)
            end
          end)
        # nested field schema not supports `:index` | `:store` | `:doc_values definition`
        proto_field_schema
        |> Map.put(:field_schemas, prepared_sub_field_schemas)
        |> Map.put(:index, nil)
        |> Map.put(:store, nil)
        |> Map.put(:doc_values, nil)

      field_type == FieldType.text() ->
        Map.put(proto_field_schema, :doc_values, false)
      true ->
        Map.put(proto_field_schema, :is_array, var_field_schema.is_array)
    end
  end

  defp prepare_sort([]) do
    nil
  end
  defp prepare_sort(nil) do
    nil
  end
  defp prepare_sort(sorters) when is_list(sorters) do
    prepared_sorters =
      Enum.map(sorters, fn sorter ->
        case sorter do
          %Search.FieldSort{field_name: field_name, order: order} ->
            if order not in [SortOrder.asc(), SortOrder.desc()] do
              raise ExAliyunOts.Error, "Invalid sort order: #{inspect(order)}"
            end

            Sorter.new(field_sort: FieldSort.new(field_name: field_name, order: order))

          _not_implemented_yet ->
            Logger.error("** #{inspect(sorter)} sorter is not implemented yet.")
            nil
        end
      end)
      |> Enum.filter(fn sorter -> sorter != nil end)
    Sort.new(sorter: prepared_sorters)
  end

  defp prepare_index_setting(setting) do
    IndexSetting.new(
      number_of_shards: setting.number_of_shards,
      routing_fields: setting.routing_fields,
      routing_partition_size: setting.routing_partition_size
    )
  end

  defp prepare_query(%Search.MatchQuery{
         field_name: field_name,
         text: text,
         minimum_should_match: minimum_should_match
       }) do
    proto_query = MatchQuery.new(field_name: field_name, text: text, minimum_should_match: minimum_should_match)
    Query.new(
      type: QueryType.match,
      query: MatchQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.MatchAllQuery{}) do
    proto_query = MatchAllQuery.new()
    Query.new(
      type: QueryType.match_all,
      query: MatchAllQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.MatchPhraseQuery{
         field_name: field_name,
         text: text
       }) do
    proto_query = MatchPhraseQuery.new(field_name: field_name, text: text)
    Query.new(
      type: QueryType.match_phrase,
      query: MatchPhraseQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.TermQuery{
           field_name: field_name,
           term: term
         }) do
    term_bytes = term_to_bytes(term)
    proto_query = TermQuery.new(field_name: field_name, term: term_bytes)
    Query.new(
      type: QueryType.term,
      query: TermQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.TermsQuery{
           field_name: field_name,
           terms: terms
         }) do
    terms_bytes = Enum.map(terms, fn(term) -> term_to_bytes(term) end)
    proto_query = TermsQuery.new(field_name: field_name, terms: terms_bytes)
    Query.new(
      type: QueryType.terms,
      query: TermsQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.PrefixQuery{
           field_name: field_name,
           prefix: prefix
         }) do
    proto_query = PrefixQuery.new(field_name: field_name, prefix: prefix)
    Query.new(
      type: QueryType.prefix,
      query: PrefixQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.WildcardQuery{
           field_name: field_name,
           value: value
         }) do
    proto_query = WildcardQuery.new(field_name: field_name, value: value)
    Query.new(
      type: QueryType.wildcard,
      query: WildcardQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.RangeQuery{
           field_name: field_name,
           from: from,
           to: to,
           include_lower: include_lower,
           include_upper: include_upper
         }) do
    # `from` value is lower value, and `to` value is upper value.
    # if both of them are not nil, we should set "`from` <= `to`" as expected.
    cond do
      from == nil and to == nil ->
        raise ExAliyunOts.Error, "No `from` or `to` specified for range query"
      from != nil and to != nil and from > to ->
        raise ExAliyunOts.Error, "Require `from` value should be less than or equal to `to` value"
      true ->
        :ok
    end

    bytes_from = if from == nil, do: nil, else: term_to_bytes(from)
    bytes_to = if to == nil, do: nil, else: term_to_bytes(to)
    proto_query = RangeQuery.new(field_name: field_name, range_from: bytes_from, range_to: bytes_to, include_lower: include_lower, include_upper: include_upper)
    Query.new(
      type: QueryType.range,
      query: RangeQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.BoolQuery{
           must: must,
           must_not: must_not,
           filter: filter,
           should: should,
           minimum_should_match: minimum_should_match
         }) do
    must_queries = Enum.map(must, fn(query) -> prepare_query(query) end)
    must_not_queries = Enum.map(must_not, fn(query) -> prepare_query(query) end)
    filter_queries = Enum.map(filter, fn(query) -> prepare_query(query) end)

    should_queries = Enum.map(should, fn(query) -> prepare_query(query) end)
    should_queries_size = length(should_queries)

    minimum_should_match =
      if should_queries_size > 0 do
        cond do
          minimum_should_match == nil ->
            1
          not is_integer(minimum_should_match) ->
            raise ExAliyunOts.Error, "Invalid minimum_should_match: #{inspect minimum_should_match}, should be integer"
          minimum_should_match > should_queries_size ->
            raise ExAliyunOts.Error, "Invalid minimum_should_match: #{inspect minimum_should_match}, should be less than or equal to the size of should queries (size: #{inspect minimum_should_match})"
          true ->
            minimum_should_match
        end
      else
        # if `should_queries` is empty list, should set `minimum_should_match` as nil
        nil
      end
    proto_query = BoolQuery.new(must_queries: must_queries, must_not_queries: must_not_queries, filter_queries: filter_queries, should_queries: should_queries, minimum_should_match: minimum_should_match)
    Query.new(
      type: QueryType.bool,
      query: BoolQuery.encode(proto_query)
    )
  end
  defp prepare_query(%Search.NestedQuery{
           path: path,
           query: query,
           score_mode: score_mode
         }) do
    if score_mode not in valid_score_modes(), do: raise ExAliyunOts.Error, "Invalid score_mode: #{inspect score_mode}"
    proto_inner_query = prepare_query(query)
    proto_query = NestedQuery.new(path: path, query: proto_inner_query, score_mode: score_mode)
    Query.new(
      type: QueryType.nested,
      query: NestedQuery.encode(proto_query)
    )
  end
  defp prepare_query(query) do
    raise ExAliyunOts.Error, "Not supported query: #{inspect query}"
  end

  defp valid_score_modes() do
    [ScoreMode.none, ScoreMode.avg, ScoreMode.max, ScoreMode.total, ScoreMode.min]
  end
end
