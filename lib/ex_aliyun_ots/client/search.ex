defmodule ExAliyunOts.Client.Search do
  @moduledoc false

  alias ExAliyunOts.TableStoreSearch.{
    CreateSearchIndexRequest,
    IndexSchema,
    FieldSchema,
    SingleWordAnalyzerParameter,
    SplitAnalyzerParameter,
    FuzzyAnalyzerParameter,
    PrimaryKeySort,
    FieldSort,
    GeoDistanceSort,
    ScoreSort,
    Sorter,
    Sort,
    Collapse,
    CreateSearchIndexRequest,
    CreateSearchIndexResponse,
    DeleteSearchIndexRequest,
    DeleteSearchIndexResponse,
    ListSearchIndexRequest,
    ListSearchIndexResponse,
    DescribeSearchIndexRequest,
    DescribeSearchIndexResponse,
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
    NestedQuery,
    GeoDistanceQuery,
    GeoBoundingBoxQuery,
    GeoPolygonQuery,
    ExistsQuery,
    Aggregation,
    Aggregations,
    AvgAggregation,
    MaxAggregation,
    MinAggregation,
    SumAggregation,
    CountAggregation,
    DistinctCountAggregation,
    AggregationsResult,
    AvgAggregationResult,
    DistinctCountAggregationResult,
    MaxAggregationResult,
    MinAggregationResult,
    SumAggregationResult,
    CountAggregationResult,
    GroupBys,
    GroupBy,
    GroupByField,
    GroupByRange,
    GroupByFilter,
    GroupByGeoDistance,
    FieldRange,
    GroupByHistogram,
    GroupBysResult,
    GroupByFilterResult,
    GroupByFilterResultItem,
    GroupByGeoDistanceResult,
    GroupByGeoDistanceResultItem,
    GroupByRangeResult,
    GroupByRangeResultItem,
    GroupByFieldResult,
    GroupByFieldResultItem,
    GroupByHistogramResult,
    GroupByHistogramItem,
    GroupBySort,
    GroupBySorter,
    GroupKeySort,
    RowCountSort,
    SubAggSort,
    Range,
    GeoPoint,
    NestedFilter,
    ParallelScanRequest,
    ParallelScanResponse,
    ScanQuery
  }

  alias ExAliyunOts.TableStore.{
    ComputeSplitsRequest,
    SearchIndexSplitsOptions,
    ComputeSplitsResponse
  }

  alias ExAliyunOts.{Http, Utils}
  alias ExAliyunOts.Var.Search

  alias ExAliyunOts.Const.Search.{
    FieldType,
    SortOrder,
    QueryType,
    ScoreMode,
    AggregationType,
    GroupByType,
    SortMode
  }

  import ExAliyunOts.Logger, only: [error: 1]
  require FieldType
  require SortOrder
  require QueryType
  require ScoreMode
  require AggregationType
  require GroupByType
  require SortMode

  @variant_type_integer 0x0
  @variant_type_double 0x1
  @variant_type_boolean 0x2
  @variant_type_string 0x3

  defp request_to_create_search_index(%Search.CreateSearchIndexRequest{
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
      %IndexSchema{
        index_sort: proto_sort,
        field_schemas: proto_field_schemas,
        index_setting: proto_index_setting
      }

    request =
      %CreateSearchIndexRequest{
        schema: proto_index_schema,
        table_name: table_name,
        index_name: index_name
      }

    request |> CreateSearchIndexRequest.encode!() |> IO.iodata_to_binary()
  end

  def remote_create_search_index(instance, var_create_search_index) do
    request_body = request_to_create_search_index(var_create_search_index)

    instance
    |> Http.client("/CreateSearchIndex", request_body, &CreateSearchIndexResponse.decode!/1)
    |> Http.post()
  end

  defp request_to_search(%Search.SearchRequest{
         table_name: table_name,
         index_name: index_name,
         columns_to_get: %Search.ColumnsToGet{
           return_type: return_type,
           column_names: column_names
         },
         search_query: search_query
       }) do
    search_query =
      %SearchQuery{
        offset: search_query.offset,
        limit: search_query.limit,
        query: prepare_query(search_query.query),
        sort: prepare_sort(search_query.sort),
        collapse: prepare_collapse(search_query.collapse),
        aggs: prepare_aggs(search_query.aggs),
        group_bys: prepare_group_bys(search_query.group_bys),
        getTotalCount: search_query.get_total_count,
        token: search_query.token
      }
      |> SearchQuery.encode!()
      |> IO.iodata_to_binary()

    proto_columns_to_get =
      %ColumnsToGet{
        return_type: return_type,
        column_names: column_names
      }

    request =
      %SearchRequest{
        table_name: table_name,
        index_name: index_name,
        columns_to_get: proto_columns_to_get,
        search_query: search_query
      }

    request |> SearchRequest.encode!() |> IO.iodata_to_binary()
  end

  def remote_search(instance, var_search_request) do
    request_body = request_to_search(var_search_request)

    result =
      instance
      |> Http.client("/Search", request_body, &decode_search_response/1)
      |> Http.post()

    result
  end

  defp request_to_delete_search_index(%Search.DeleteSearchIndexRequest{
         table_name: table_name,
         index_name: index_name
       }) do
    DeleteSearchIndexRequest
    |> struct([table_name: table_name, index_name: index_name])
    |> DeleteSearchIndexRequest.encode!()
    |> IO.iodata_to_binary()
  end

  def remote_delete_search_index(instance, var_delete_search_index) do
    request_body = request_to_delete_search_index(var_delete_search_index)

    instance
    |> Http.client("/DeleteSearchIndex", request_body, &DeleteSearchIndexResponse.decode!/1)
    |> Http.post()
  end

  defp request_to_list_search_index(table_name) do
    %ListSearchIndexRequest{table_name: table_name} |> ListSearchIndexRequest.encode!() |> IO.iodata_to_binary()
  end

  def remote_list_search_index(instance, table_name) do
    request_body = request_to_list_search_index(table_name)

    instance
    |> Http.client("/ListSearchIndex", request_body, &ListSearchIndexResponse.decode!/1)
    |> Http.post()
  end

  defp request_to_describe_search_index(%Search.DescribeSearchIndexRequest{
         table_name: table_name,
         index_name: index_name
       }) do
    DescribeSearchIndexRequest
    |> struct([table_name: table_name, index_name: index_name])
    |> DescribeSearchIndexRequest.encode!()
    |> IO.iodata_to_binary()
  end

  def remote_describe_search_index(instance, var_describe_search_index) do
    request_body = request_to_describe_search_index(var_describe_search_index)

    instance
    |> Http.client("/DescribeSearchIndex", request_body, &decode_describe_search_index_response/1)
    |> Http.post()
  end

  def request_to_compute_splits(table_name, index_name) do
    %ComputeSplitsRequest{
      table_name: table_name,
      search_index_splits_options: %SearchIndexSplitsOptions{
        index_name: index_name
      }
    }
    |> ComputeSplitsRequest.encode!()
    |> IO.iodata_to_binary()
  end

  def remote_compute_splits(instance, request_body) do
    instance
    |> Http.client("/ComputeSplits", request_body, &ComputeSplitsResponse.decode!/1)
    |> Http.post()
  end

  def request_to_parallel_scan(%Search.ParallelScanRequest{
        table_name: table_name,
        index_name: index_name,
        columns_to_get: %Search.ColumnsToGet{
          return_type: return_type,
          column_names: column_names
        },
        scan_query: scan_query,
        session_id: session_id
      }) do

    scan_query =
      %ScanQuery{
        query: prepare_query(scan_query.query),
        limit: scan_query.limit,
        alive_time: scan_query.alive_time,
        token: scan_query.token,
        current_parallel_id: scan_query.current_parallel_id,
        max_parallel: scan_query.max_parallel
      }
      |> ScanQuery.encode!()
      |> IO.iodata_to_binary()

    proto_columns_to_get =
      %ColumnsToGet{
        return_type: return_type,
        column_names: column_names
      }

    request =
      %ParallelScanRequest{
        table_name: table_name,
        index_name: index_name,
        columns_to_get: proto_columns_to_get,
        scan_query: scan_query,
        session_id: session_id
      }

    request |> ParallelScanRequest.encode!() |> IO.iodata_to_binary()
  end

  def remote_parallel_scan(instance, request_body) do
    instance
    |> Http.client("/ParallelScan", request_body, &ParallelScanResponse.decode!/1)
    |> Http.post()
  end

  def term_to_bytes(term) when is_bitstring(term) do
    <<@variant_type_string, byte_size(term)::little-integer-size(32), term::binary>>
  end

  def term_to_bytes(term) when is_integer(term) do
    <<@variant_type_integer, term::little-integer-size(64)>>
  end

  def term_to_bytes(term) when is_float(term) do
    <<@variant_type_double, term::float-little>>
  end

  def term_to_bytes(true) do
    <<@variant_type_boolean, 1>>
  end

  def term_to_bytes(false) do
    <<@variant_type_boolean, 0>>
  end

  def term_to_bytes(term) do
    raise ExAliyunOts.RuntimeError,
          "invalid type of term: #{inspect(term)}, please use string/integer/float/boolean."
  end

  def bytes_to_term(<<@variant_type_string, _size::little-integer-size(32), term::binary>>), do: term
  def bytes_to_term(<<@variant_type_integer, term::little-integer-size(64)>>), do: term
  def bytes_to_term(<<@variant_type_double, term::float-little>>), do: term
  def bytes_to_term(<<@variant_type_boolean, 1>>), do: true
  def bytes_to_term(<<@variant_type_boolean, 0>>), do: false
  def bytes_to_term(bytes) do
    raise ExAliyunOts.RuntimeError,
          "invalid type of bytes: #{inspect(bytes)}, please use string/integer/float/boolean."
  end

  defp agg_missing_to_bytes(nil) do
    nil
  end

  defp agg_missing_to_bytes(missing) when is_integer(missing) do
    term_to_bytes(missing)
  end

  defp agg_missing_to_bytes(missing) when is_float(missing) do
    term_to_bytes(missing)
  end

  defp agg_missing_to_bytes(missing) do
    raise ExAliyunOts.RuntimeError,
          "invalid missing value of aggregation: #{inspect(missing)}, please use integer or float for it."
  end

  defp iterate_all_field_schemas(var_field_schema) do
    field_type = var_field_schema.field_type
    sub_field_schemas = var_field_schema.field_schemas
    size_sub_field_schemas = length(sub_field_schemas)

    if field_type == FieldType.nested() and
         (size_sub_field_schemas == 0 and size_sub_field_schemas > 25) do
      raise ExAliyunOts.RuntimeError,
            "Invalid nested type field schema with : #{size_sub_field_schemas} sub field schemas, the valid range size of sub field schemas is [1, 25]"
    end

    %FieldSchema{
      field_name: var_field_schema.field_name,
      field_type: var_field_schema.field_type,
      index: var_field_schema.index,
      sort_and_agg: var_field_schema.enable_sort_and_agg,
      store: var_field_schema.store,
      is_array: var_field_schema.is_array,
      is_virtual_field: var_field_schema.is_virtual_field,
      source_field_names: prepare_source_field_names(var_field_schema.source_field_name)
    }
    |> build_extras_by_field_type(var_field_schema)
  end

  # NOTICE:
  # The type of source_field_names in protobuf schema is string[]
  # But the actual relationship from virtual field to source field is one-to-one
  # (confirmed with AlibabaCloud support team)
  # So we convert type of source_field_name[s]? manually here
  defp prepare_source_field_names(nil), do: []
  defp prepare_source_field_names(source_field_name), do: [source_field_name]

  defp build_extras_by_field_type(proto_field_schema, %{field_type: FieldType.nested()} = var_field_schema) do
    sub_field_schemas = var_field_schema.field_schemas
    prepared_sub_field_schemas =
      Enum.map(sub_field_schemas, fn sub_field_schema ->
        if sub_field_schema.field_type == FieldType.nested() do
          raise ExAliyunOts.RuntimeError,
                "Mapping depth in the nested attribute column only supports one level, cannot nest the nested type of field schema as the sub field schemas"
        else
          iterate_all_field_schemas(sub_field_schema)
        end
      end)
    # nested field schema not supports `:index` | `:store` | `:sort_and_agg` definition
    proto_field_schema
    |> Map.put(:field_schemas, prepared_sub_field_schemas)
    |> Map.put(:index, nil)
    |> Map.put(:store, nil)
    |> Map.put(:sort_and_agg, nil)
  end

  defp build_extras_by_field_type(proto_field_schema, %{field_type: FieldType.text()} = var_field_schema) do
    proto_field_schema
    |> Map.put(:sort_and_agg, nil)
    |> Map.put(:analyzer, var_field_schema.analyzer)
    |> Map.put(:analyzer_parameter, prepare_analyzer_parameter(var_field_schema.analyzer, var_field_schema.analyzer_parameter))
  end

  defp build_extras_by_field_type(proto_field_schema, _var_field_schema), do: proto_field_schema

  defp prepare_analyzer_parameter("single_word", parameter) when is_map(parameter) or is_list(parameter), do:
    do_prepare_analyzer_parameter(SingleWordAnalyzerParameter, parameter)

  defp prepare_analyzer_parameter("split", parameter) when is_map(parameter) or is_list(parameter), do:
    do_prepare_analyzer_parameter(SplitAnalyzerParameter, parameter)

  defp prepare_analyzer_parameter("fuzzy", parameter) when is_map(parameter) or is_list(parameter), do:
    do_prepare_analyzer_parameter(FuzzyAnalyzerParameter, parameter)

  defp prepare_analyzer_parameter(analyzer, parameter) when is_atom(analyzer), do:
    prepare_analyzer_parameter(to_string(analyzer), parameter)

  defp prepare_analyzer_parameter(_analyzer, _parameter), do: nil

  defp do_prepare_analyzer_parameter(struct_module, parameter)  do
    analyzer_parameter = struct(struct_module, parameter)
    struct_module
    |> apply(:encode!, [analyzer_parameter])
    |> IO.iodata_to_binary()
  end

  defp prepare_sort([]), do: nil
  defp prepare_sort(nil), do: nil
  defp prepare_sort(sorters) when is_list(sorters) do
    prepared_sorters =
      sorters
      |> Enum.map(&prepare_sorter/1)
      |> Enum.filter(fn sorter -> sorter != nil end)

    %Sort{sorter: prepared_sorters}
  end

  defp prepare_sorter(%Search.PrimaryKeySort{order: order}) do
    assert_valid_sort_order(order)
    %Sorter{pk_sort: %PrimaryKeySort{order: order}}
  end

  defp prepare_sorter(%Search.FieldSort{
         field_name: field_name,
         order: order,
         mode: mode,
         nested_filter: nested_filter
       }) do
    assert_valid_sort_order(order)
    assert_valid_sort_mode(mode)

    nested_filter = prepare_sorter_nested_filter(nested_filter)

    %Sorter{
      field_sort:
        %FieldSort{
          field_name: field_name,
          order: order,
          mode: mode,
          nested_filter: nested_filter
        }
    }
  end

  defp prepare_sorter(%Search.GeoDistanceSort{
         field_name: field_name,
         order: order,
         points: points,
         distance_type: distance_type,
         mode: mode,
         nested_filter: nested_filter
       }) do
    assert_valid_sort_order(order)
    assert_valid_sort_mode(mode)
    assert_valid_geo_points(points)

    nested_filter = prepare_sorter_nested_filter(nested_filter)

    %Sorter{
      geo_distance_sort:
        %GeoDistanceSort{
          field_name: field_name,
          order: order,
          points: points,
          distance_type: distance_type,
          mode: mode,
          nested_filter: nested_filter
        }
    }
  end

  defp prepare_sorter(%Search.ScoreSort{order: order}) do
    assert_valid_sort_order(order)
    %Sorter{score_sort: %ScoreSort{order: order}}
  end

  defp prepare_sorter(sorter) do
    error(fn ->
      [
        "** ",
        inspect(sorter),
        " sorter is not implemented yet."
      ]
    end)

    nil
  end

  defp prepare_sorter_nested_filter(nil), do: nil

  defp prepare_sorter_nested_filter(%Search.NestedFilter{path: path, filter: filter}) do
    query = prepare_query(filter)
    %NestedFilter{path: path, filter: query}
  end

  defp assert_valid_sort_order(SortOrder.asc()), do: :ok
  defp assert_valid_sort_order(SortOrder.desc()), do: :ok

  defp assert_valid_sort_order(invalid) do
    raise ExAliyunOts.RuntimeError,
          "Invalid sort order: #{inspect(invalid)}, please use SortOrder.desc or SortOrder.asc."
  end

  defp assert_valid_sort_mode(SortMode.min()), do: :ok
  defp assert_valid_sort_mode(SortMode.max()), do: :ok
  defp assert_valid_sort_mode(SortMode.avg()), do: :ok
  defp assert_valid_sort_mode(nil), do: :ok

  defp assert_valid_sort_mode(invalid) do
    raise ExAliyunOts.RuntimeError,
          "Invalid sort mode: #{inspect(invalid)}, please use SortMode.min | SortMode.max | SortMode.avg for it."
  end

  defp assert_valid_score_mode(ScoreMode.none()), do: :ok
  defp assert_valid_score_mode(ScoreMode.avg()), do: :ok
  defp assert_valid_score_mode(ScoreMode.max()), do: :ok
  defp assert_valid_score_mode(ScoreMode.total()), do: :ok
  defp assert_valid_score_mode(ScoreMode.min()), do: :ok

  defp assert_valid_score_mode(invalid) do
    raise ExAliyunOts.RuntimeError,
          "Invalid score_mode: #{inspect(invalid)} for NestedQuery, please use ScoreMode.none | ScoreMode.avg | ScoreMode.max | ScoreMode.total | ScoreMode.min for it."
  end

  defp assert_valid_geo_points(points) do
    invalid =
      Enum.find(points, fn point ->
        Utils.valid_geo_point?(point) == false
      end)

    if invalid != nil do
      raise ExAliyunOts.RuntimeError,
            "Invalid geo point: #{inspect(invalid)}, please set it as `$latitude,$longitude` format."
    else
      :ok
    end
  end

  defp assert_valid_geo_point(lat, lon) when is_number(lat) and is_number(lon) do
    :ok
  end

  defp assert_valid_geo_point(lat, lon) do
    raise ExAliyunOts.RuntimeError,
          "Invalid latitude: `#{inspect(lat)}` or longitude: `#{inspect(lon)}` for a geo point, please set them as number."
  end

  defp prepare_collapse("") do
    nil
  end

  defp prepare_collapse(field_name) when is_bitstring(field_name) do
    %Collapse{field_name: field_name}
  end

  defp prepare_collapse(_field_name) do
    nil
  end

  defp prepare_aggs(nil), do: nil
  defp prepare_aggs([]), do: nil

  defp prepare_aggs(aggs) when is_list(aggs) do
    map_aggs(aggs, [])
  end

  defp map_aggs(nil, []), do: nil
  defp map_aggs([], []), do: nil

  defp map_aggs([], result) do
    %Aggregations{aggs: Enum.reverse(result)}
  end

  defp map_aggs([agg | rest], result) do
    agg = map_agg(agg)
    map_aggs(rest, [agg | result])
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.min() do
    MinAggregation
    |> struct([field_name: agg.field_name, missing: agg_missing_to_bytes(agg.missing)])
    |> MinAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.max() do
    MaxAggregation
    |> struct([field_name: agg.field_name, missing: agg_missing_to_bytes(agg.missing)])
    |> MaxAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.avg() do
    AvgAggregation
    |> struct([field_name: agg.field_name, missing: agg_missing_to_bytes(agg.missing)])
    |> AvgAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.distinct_count() do
    DistinctCountAggregation
    |> struct([field_name: agg.field_name, missing: agg_missing_to_bytes(agg.missing)])
    |> DistinctCountAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.sum() do
    SumAggregation
    |> struct([field_name: agg.field_name, missing: agg_missing_to_bytes(agg.missing)])
    |> SumAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp map_agg(%{type: type} = agg) when type == AggregationType.count() do
    CountAggregation
    |> struct([field_name: agg.field_name])
    |> CountAggregation.encode!()
    |> IO.iodata_to_binary()
    |> to_aggregation(agg.name, type)
  end

  defp to_aggregation(body, name, type) do
    %Aggregation{body: body, name: name, type: type}
  end

  defp prepare_group_bys(nil), do: nil
  defp prepare_group_bys([]), do: nil

  defp prepare_group_bys(group_bys) when is_list(group_bys) do
    map_group_bys(group_bys, [])
  end

  defp map_group_bys(nil, []), do: nil
  defp map_group_bys([], []), do: nil

  defp map_group_bys([], result) do
    %GroupBys{group_bys: Enum.reverse(result)}
  end

  defp map_group_bys([group_by | rest], result) do
    group_by = map_group_by(group_by)
    map_group_bys(rest, [group_by | result])
  end

  defp map_group_by(%Search.GroupByField{
         name: name,
         field_name: field_name,
         size: size,
         sub_group_bys: sub_group_bys,
         sub_aggs: sub_aggs,
         sort: sort
       }) do
    sub_group_bys = map_group_bys(sub_group_bys, [])
    sub_aggs = map_aggs(sub_aggs, [])
    sort = map_group_by_sort(sort, [])

    GroupByField
    |> struct([
      field_name: field_name,
      size: size,
      sub_group_bys: sub_group_bys,
      sub_aggs: sub_aggs,
      sort: sort
    ])
    |> GroupByField.encode!()
    |> IO.iodata_to_binary()
    |> to_group_by(name, GroupByType.field())
  end

  defp map_group_by(%Search.GroupByRange{
         name: name,
         field_name: field_name,
         ranges: ranges,
         sub_group_bys: sub_group_bys,
         sub_aggs: sub_aggs
       })
       when is_list(ranges) do
    sub_group_bys = map_group_bys(sub_group_bys, [])
    sub_aggs = map_aggs(sub_aggs, [])
    ranges = map_group_by_ranges(ranges, [])

    GroupByRange
    |> struct([field_name: field_name, sub_group_bys: sub_group_bys, sub_aggs: sub_aggs, ranges: ranges])
    |> GroupByRange.encode!()
    |> IO.iodata_to_binary()
    |> to_group_by(name, GroupByType.range())
  end

  defp map_group_by(%Search.GroupByFilter{
         name: name,
         filters: filters,
         sub_group_bys: sub_group_bys,
         sub_aggs: sub_aggs
       })
       when is_list(filters) do
    sub_group_bys = map_group_bys(sub_group_bys, [])
    sub_aggs = map_aggs(sub_aggs, [])
    filters = map_group_by_filters(filters, [])

    GroupByFilter
    |> struct([filters: filters, sub_group_bys: sub_group_bys, sub_aggs: sub_aggs])
    |> GroupByFilter.encode!()
    |> IO.iodata_to_binary()
    |> to_group_by(name, GroupByType.filter())
  end

  defp map_group_by(%Search.GroupByGeoDistance{
         name: name,
         field_name: field_name,
         ranges: ranges,
         sub_group_bys: sub_group_bys,
         sub_aggs: sub_aggs,
         lat: lat,
         lon: lon
       }) do
    assert_valid_geo_point(lat, lon)

    sub_group_bys = map_group_bys(sub_group_bys, [])
    sub_aggs = map_aggs(sub_aggs, [])
    ranges = map_group_by_ranges(ranges, [])
    origin = %GeoPoint{lat: lat, lon: lon}

    GroupByGeoDistance
    |> struct([
      field_name: field_name,
      sub_group_bys: sub_group_bys,
      sub_aggs: sub_aggs,
      ranges: ranges,
      origin: origin
    ])
    |> GroupByGeoDistance.encode!()
    |> IO.iodata_to_binary()
    |> to_group_by(name, GroupByType.geo_distance())
  end

  defp map_group_by(%Search.GroupByHistogram{
      name: name,
      field_name: field_name,
      interval: interval,
      field_range: {min, max},
      min_doc_count: min_doc_count,
      missing: missing
  }) do
    field_range = map_group_by_field_range(min, max)
    missing = if is_nil(missing), do: nil, else: term_to_bytes(missing)

    GroupByHistogram
    |> struct([
      field_name: field_name,
      interval: term_to_bytes(interval),
      field_range: field_range,
      min_doc_count: min_doc_count,
      missing: missing
    ])
    |> GroupByHistogram.encode!()
    |> IO.iodata_to_binary()
    |> to_group_by(name, GroupByType.histogram())
  end

  defp to_group_by(body, name, type) do
    %GroupBy{body: body, name: name, type: type}
  end

  defp map_group_by_sort(nil, []), do: nil
  defp map_group_by_sort([], []), do: nil

  defp map_group_by_sort([], result) do
    %GroupBySort{sorters: Enum.reverse(result)}
  end

  defp map_group_by_sort([sorter | rest], result) do
    sorter = map_group_by_sorter(sorter)
    map_group_by_sort(rest, [sorter | result])
  end

  defp map_group_by_sorter(%Search.GroupKeySort{order: order}) do
    %GroupBySorter{group_key_sort: %GroupKeySort{order: order}}
  end

  defp map_group_by_sorter(%Search.RowCountSort{order: order}) do
    %GroupBySorter{row_count_sort: %RowCountSort{order: order}}
  end

  defp map_group_by_sorter(%Search.SubAggSort{sub_agg_name: sub_agg_name, order: order}) do
    %GroupBySorter{sub_agg_sort: %SubAggSort{sub_agg_name: sub_agg_name, order: order}}
  end

  defp map_group_by_ranges(nil, []), do: nil
  defp map_group_by_ranges([], []), do: nil

  defp map_group_by_ranges([], result) do
    Enum.reverse(result)
  end

  defp map_group_by_ranges([{from, to} | rest], result) when is_number(from) and is_number(to) do
    range = %Range{from: from, to: to}
    map_group_by_ranges(rest, [range | result])
  end

  defp map_group_by_ranges([{from, to} | _rest], _result) do
    raise ExAliyunOts.RuntimeError,
      "Invalid from: `#{inspect(from)}` or to: `#{inspect(to)}` for a range, please set them as number."
  end

  defp map_group_by_field_range(min, max) when is_number(min) and is_number(max), do: %FieldRange{min: term_to_bytes(min), max: term_to_bytes(max)}
  defp map_group_by_field_range(min, max) do
    raise ExAliyunOts.RuntimeError,
          "Invalid from: `#{inspect(min)}` to: `#{inspect(max)}` for a field range, please set them as number."
  end

  defp map_group_by_filters(nil, []), do: nil
  defp map_group_by_filters([], []), do: nil

  defp map_group_by_filters([], result) do
    Enum.reverse(result)
  end

  defp map_group_by_filters([query | rest], result) do
    query = prepare_query(query)
    map_group_by_filters(rest, [query | result])
  end

  defp prepare_index_setting(setting) do
    %IndexSetting{
      number_of_shards: setting.number_of_shards,
      routing_fields: setting.routing_fields,
      routing_partition_size: setting.routing_partition_size
    }
  end

  defp prepare_query(%Search.MatchQuery{
         field_name: field_name,
         text: text,
         minimum_should_match: minimum_should_match
       }) do
    query =
      %MatchQuery{
        field_name: field_name,
        text: text,
        minimum_should_match: minimum_should_match
      }
      |> MatchQuery.encode!()
      |> IO.iodata_to_binary()

    %Query{
      type: QueryType.match(),
      query: query
    }
  end

  defp prepare_query(%Search.MatchAllQuery{}) do
    query = %MatchAllQuery{} |> MatchAllQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.match_all(),
      query: query
    }
  end

  defp prepare_query(%Search.MatchPhraseQuery{
         field_name: field_name,
         text: text
       }) do
    query = %MatchPhraseQuery{field_name: field_name, text: text} |> MatchPhraseQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.match_phrase(),
      query: query
    }
  end

  defp prepare_query(%Search.TermQuery{
         field_name: field_name,
         term: term
       }) do
    query = %TermQuery{field_name: field_name, term: term_to_bytes(term)} |> TermQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.term(),
      query: query
    }
  end

  defp prepare_query(%Search.TermsQuery{
         field_name: field_name,
         terms: terms
       }) do
    terms_bytes = Enum.map(terms, fn term -> term_to_bytes(term) end)
    query = %TermsQuery{field_name: field_name, terms: terms_bytes} |> TermsQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.terms(),
      query: query
    }
  end

  defp prepare_query(%Search.PrefixQuery{
         field_name: field_name,
         prefix: prefix
       }) do
    query = %PrefixQuery{field_name: field_name, prefix: prefix} |> PrefixQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.prefix(),
      query: query
    }
  end

  defp prepare_query(%Search.WildcardQuery{
         field_name: field_name,
         value: value
       }) do
    query = %WildcardQuery{field_name: field_name, value: value} |> WildcardQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.wildcard(),
      query: query
    }
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
        raise ExAliyunOts.RuntimeError, "No `from` or `to` specified for range query"

      from != nil and to != nil and from > to ->
        raise ExAliyunOts.RuntimeError,
              "Require `from` value should be less than or equal to `to` value"

      true ->
        :ok
    end

    bytes_from = if from == nil, do: nil, else: term_to_bytes(from)
    bytes_to = if to == nil, do: nil, else: term_to_bytes(to)

    query =
      %RangeQuery{
        field_name: field_name,
        range_from: bytes_from,
        range_to: bytes_to,
        include_lower: include_lower,
        include_upper: include_upper
      }
      |> RangeQuery.encode!()
      |> IO.iodata_to_binary()

    %Query{
      type: QueryType.range(),
      query: query
    }
  end

  defp prepare_query(%Search.BoolQuery{
         must: must,
         must_not: must_not,
         filter: filter,
         should: should,
         minimum_should_match: minimum_should_match
       }) do
    must_queries = Enum.map(must, fn query -> prepare_query(query) end)
    must_not_queries = Enum.map(must_not, fn query -> prepare_query(query) end)
    filter_queries = Enum.map(filter, fn query -> prepare_query(query) end)

    should_queries = Enum.map(should, fn query -> prepare_query(query) end)
    should_queries_size = length(should_queries)

    minimum_should_match =
      if should_queries_size > 0 do
        cond do
          minimum_should_match == nil ->
            1

          not is_integer(minimum_should_match) ->
            raise ExAliyunOts.RuntimeError,
                  "Invalid minimum_should_match: #{inspect(minimum_should_match)}, should be integer"

          minimum_should_match > should_queries_size ->
            raise ExAliyunOts.RuntimeError,
                  "Invalid minimum_should_match: #{inspect(minimum_should_match)}, should be less than or equal to the size of should queries (size: #{
                    inspect(minimum_should_match)
                  })"

          true ->
            minimum_should_match
        end
      else
        # if `should_queries` is empty list, should set `minimum_should_match` as nil
        nil
      end

    query =
      %BoolQuery{
        must_queries: must_queries,
        must_not_queries: must_not_queries,
        filter_queries: filter_queries,
        should_queries: should_queries,
        minimum_should_match: minimum_should_match
      }
      |> BoolQuery.encode!()
      |> IO.iodata_to_binary()

    %Query{
      type: QueryType.bool(),
      query: query
    }
  end

  defp prepare_query(%Search.NestedQuery{
         path: path,
         query: query,
         score_mode: score_mode
       }) do
    assert_valid_score_mode(score_mode)

    query = %NestedQuery{path: path, query: prepare_query(query), score_mode: score_mode} |> NestedQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.nested(),
      query: query
    }
  end

  defp prepare_query(%Search.GeoDistanceQuery{
         field_name: field_name,
         center_point: center_point,
         distance: distance
       })
       when is_number(distance) and distance >= 0 do
    if Utils.valid_geo_point?(center_point) do
      query =
        %GeoDistanceQuery{
          field_name: field_name,
          center_point: center_point,
          distance: distance
        }
        |> GeoDistanceQuery.encode!()
        |> IO.iodata_to_binary()

      %Query{
        type: QueryType.geo_distance(),
        query: query
      }
    else
      raise ExAliyunOts.RuntimeError,
            "Invalid center_point: #{inspect(center_point)}, please set it as `$latitude,$longitude` format."
    end
  end

  defp prepare_query(%Search.GeoBoundingBoxQuery{
         field_name: field_name,
         top_left: top_left,
         bottom_right: bottom_right
       }) do
    assert_valid_geo_points([top_left, bottom_right])

    query =
      %GeoBoundingBoxQuery{
        field_name: field_name,
        top_left: top_left,
        bottom_right: bottom_right
      }
      |> GeoBoundingBoxQuery.encode!()
      |> IO.iodata_to_binary()

    %Query{
      type: QueryType.geo_bounding_box(),
      query: query
    }
  end

  defp prepare_query(%Search.GeoPolygonQuery{
         field_name: field_name,
         points: points
       }) do
    assert_valid_geo_points(points)

    query =
      %GeoPolygonQuery{
        field_name: field_name,
        points: points
      }
      |> GeoPolygonQuery.encode!()
      |> IO.iodata_to_binary()

    %Query{
      type: QueryType.geo_polygon(),
      query: query
    }
  end

  defp prepare_query(%Search.ExistsQuery{
         field_name: field_name
       }) do
    query = %ExistsQuery{field_name: field_name} |> ExistsQuery.encode!() |> IO.iodata_to_binary()

    %Query{
      type: QueryType.exists(),
      query: query
    }
  end

  defp prepare_query(query) do
    raise ExAliyunOts.RuntimeError, "Not supported query: #{inspect(query)}"
  end

  defp decode_describe_search_index_response(response_body) do
    response_body
    |> DescribeSearchIndexResponse.decode!()
    |> Map.update(:schema, %{}, fn schema ->
        Map.update(schema, :field_schemas, [], fn field_schemas ->
          Enum.map(field_schemas, fn field_schema ->
            Map.update(field_schema, :analyzer_parameter, nil, fn analyzer_parameter ->
              decode_analyzer_parameter(field_schema.analyzer, analyzer_parameter)
            end)
          end)
        end)
    end)
  end

  defp decode_analyzer_parameter("single_word", analyzer_parameter) when is_binary(analyzer_parameter), do: SingleWordAnalyzerParameter.decode!(analyzer_parameter)

  defp decode_analyzer_parameter("split", analyzer_parameter) when is_binary(analyzer_parameter), do: SplitAnalyzerParameter.decode!(analyzer_parameter)

  defp decode_analyzer_parameter("fuzzy", analyzer_parameter) when is_binary(analyzer_parameter), do: FuzzyAnalyzerParameter.decode!(analyzer_parameter)

  defp decode_analyzer_parameter(_analyzer, analyzer_parameter), do: analyzer_parameter

  defp decode_search_response(response_body) do
    response_body
    |> SearchResponse.decode!()
    |> Map.update(:aggs, nil, &decode_aggs/1)
    |> Map.update(:group_bys, nil, &decode_group_bys/1)
  end

  defp decode_aggs(nil), do: nil

  defp decode_aggs(aggs) do
    result = AggregationsResult.decode!(aggs)
    Enum.reduce(result.agg_results, %{}, &decode_agg/2)
  end

  defp decode_agg(%{type: AggregationType.avg(), name: name, agg_result: agg_result}, agg_results) do
    decoded = AvgAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :avg, name, decoded.value)
  end

  defp decode_agg(%{type: AggregationType.max(), name: name, agg_result: agg_result}, agg_results) do
    decoded = MaxAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :max, name, decoded.value)
  end

  defp decode_agg(%{type: AggregationType.min(), name: name, agg_result: agg_result}, agg_results) do
    decoded = MinAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :min, name, decoded.value)
  end

  defp decode_agg(%{type: AggregationType.sum(), name: name, agg_result: agg_result}, agg_results) do
    decoded = SumAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :sum, name, decoded.value)
  end

  defp decode_agg(
         %{type: AggregationType.count(), name: name, agg_result: agg_result},
         agg_results
       ) do
    decoded = CountAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :count, name, decoded.value)
  end

  defp decode_agg(
         %{type: AggregationType.distinct_count(), name: name, agg_result: agg_result},
         agg_results
       ) do
    decoded = DistinctCountAggregationResult.decode!(agg_result)
    sort_map_results_by_type(agg_results, :distinct_count, name, decoded.value)
  end

  defp decode_group_bys(nil), do: nil

  defp decode_group_bys(group_bys) do
    result = GroupBysResult.decode!(group_bys)
    Enum.reduce(result.group_by_results, %{}, &decode_group_by/2)
  end

  defp decode_group_by(
         %{type: GroupByType.field(), name: name, group_by_result: group_by_result},
         map_results
       ) do
    result = GroupByFieldResult.decode!(group_by_result)
    items = decode_sub_details(result.group_by_field_result_items, [])
    sort_map_results_by_type(map_results, :by_field, name, items)
  end

  defp decode_group_by(
         %{type: GroupByType.range(), name: name, group_by_result: group_by_result},
         map_results
       ) do
    result = GroupByRangeResult.decode!(group_by_result)
    items = decode_sub_details(result.group_by_range_result_items, [])
    sort_map_results_by_type(map_results, :by_range, name, items)
  end

  defp decode_group_by(
         %{type: GroupByType.filter(), name: name, group_by_result: group_by_result},
         map_results
       ) do
    result = GroupByFilterResult.decode!(group_by_result)
    items = decode_sub_details(result.group_by_filter_result_items, [])
    sort_map_results_by_type(map_results, :by_filter, name, items)
  end

  defp decode_group_by(
         %{type: GroupByType.geo_distance(), name: name, group_by_result: group_by_result},
         map_results
       ) do
    result = GroupByGeoDistanceResult.decode!(group_by_result)
    items = decode_sub_details(result.group_by_geo_distance_result_items, [])
    sort_map_results_by_type(map_results, :by_geo_distance, name, items)
  end

  defp decode_group_by(
         %{type: GroupByType.histogram(), name: name, group_by_result: group_by_result},
         map_results
       ) do
    result = GroupByHistogramResult.decode!(group_by_result)
    items = decode_sub_details(result.group_by_histogra_items, [])
    sort_map_results_by_type(map_results, :by_histogram, name, items)
  end

  defp decode_sub_details([], prepared) do
    Enum.reverse(prepared)
  end

  defp decode_sub_details(
         [
           %GroupByFieldResultItem{
             sub_aggs_result: sub_aggs_result,
             sub_group_bys_result: sub_group_bys_result
           } = item
           | rest
         ],
         prepared
       ) do
    sub_aggs = decode_sub_aggs(sub_aggs_result)
    sub_group_bys = decode_sub_group_bys(sub_group_bys_result)

    prepared_item = %{
      key: item.key,
      row_count: item.row_count,
      sub_aggs: sub_aggs,
      sub_group_bys: sub_group_bys
    }

    decode_sub_details(rest, [prepared_item | prepared])
  end

  defp decode_sub_details(
         [
           %GroupByRangeResultItem{
             sub_aggs_result: sub_aggs_result,
             sub_group_bys_result: sub_group_bys_result
           } = item
           | rest
         ],
         prepared
       ) do
    sub_aggs = decode_sub_aggs(sub_aggs_result)
    sub_group_bys = decode_sub_group_bys(sub_group_bys_result)

    prepared_item = %{
      from: item.from,
      to: item.to,
      row_count: item.row_count,
      sub_aggs: sub_aggs,
      sub_group_bys: sub_group_bys
    }

    decode_sub_details(rest, [prepared_item | prepared])
  end

  defp decode_sub_details(
         [
           %GroupByFilterResultItem{
             sub_aggs_result: sub_aggs_result,
             sub_group_bys_result: sub_group_bys_result
           } = item
           | rest
         ],
         prepared
       ) do
    sub_aggs = decode_sub_aggs(sub_aggs_result)
    sub_group_bys = decode_sub_group_bys(sub_group_bys_result)

    prepared_item = %{
      row_count: item.row_count,
      sub_aggs: sub_aggs,
      sub_group_bys: sub_group_bys
    }

    decode_sub_details(rest, [prepared_item | prepared])
  end

  defp decode_sub_details(
         [
           %GroupByGeoDistanceResultItem{
             sub_aggs_result: sub_aggs_result,
             sub_group_bys_result: sub_group_bys_result
           } = item
           | rest
         ],
         prepared
       ) do
    sub_aggs = decode_sub_aggs(sub_aggs_result)
    sub_group_bys = decode_sub_group_bys(sub_group_bys_result)

    prepared_item = %{
      from: item.from,
      to: item.to,
      row_count: item.row_count,
      sub_aggs: sub_aggs,
      sub_group_bys: sub_group_bys
    }

    decode_sub_details(rest, [prepared_item | prepared])
  end

  defp decode_sub_details(
         [%GroupByHistogramItem{} = item | rest],
         prepared
       ) do
    prepared_item = %{
      key: bytes_to_term(item.key),
      value: item.value
    }
    decode_sub_details(rest, [prepared_item | prepared])
  end

  defp decode_sub_aggs(nil), do: nil

  defp decode_sub_aggs(sub_aggs_result) do
    Enum.reduce(sub_aggs_result.agg_results, %{}, &decode_agg/2)
  end

  defp decode_sub_group_bys(nil), do: nil

  defp decode_sub_group_bys(sub_group_bys_result) do
    Enum.reduce(sub_group_bys_result.group_by_results, %{}, &decode_group_by/2)
  end

  defp sort_map_results_by_type(results, type, name, new_result) when is_map(results) do
    current = Map.get(results, type)

    if current != nil do
      Map.put(results, type, Map.put(current, name, new_result))
    else
      Map.put(results, type, %{name => new_result})
    end
  end
end
