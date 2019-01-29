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
  }

  alias ExAliyunOts.Http
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.Search.{FieldType, SortOrder, QueryType}

  require FieldType
  require SortOrder
  require QueryType

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
    proto_query = prepare_query(search_query.query)

    proto_search_query =
      SearchQuery.new(
          offset: search_query.offset,
          limit: search_query.limit,
          query: proto_query,
          sort: search_query.sort,
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
    nested_field_schemas = var_field_schema.field_schemas

    if field_type == FieldType.nested() and nested_field_schemas == [] do
      raise ExAliyunOts.Error,
            "Invalid nested type field_schema with an empty nested list: #{
              inspect(var_field_schema)
            }"
    end

    Logger.info("var_field_schema: #{inspect(var_field_schema)}")

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
        prepared_nested =
          Enum.map(nested_field_schemas, fn nested_field_schema ->
            iterate_all_field_schemas(nested_field_schema)
          end)
        Map.put(proto_field_schema, :field_schemas, prepared_nested)
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

  defp prepare_sort(index_sorts) when is_list(index_sorts) do
    prepared_sorters =
      Enum.map(index_sorts, fn index_sort ->
        case index_sort do
          %Search.FieldSort{field_name: field_name, order: order} ->
            if order not in [SortOrder.asc(), SortOrder.desc()] do
              raise ExAliyunOts.Error, "Invalid sort order: #{inspect(order)}"
            end

            Sorter.new(field_sort: FieldSort.new(field_name: field_name, order: order))

          _not_implemented_yet ->
            Logger.info("** index_sort as #{inspect(index_sort)} is not implemented yet.")
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
         minimun_should_match: minimun_should_match
       }) do
    proto_query = MatchQuery.new(field_name: field_name, text: text, minimun_should_match: minimun_should_match) 
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
  defp prepare_query(query) do
    raise ExAliyunOts.Error, "Not supported query: #{inspect query}"
  end

end
