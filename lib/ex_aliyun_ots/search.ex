defmodule ExAliyunOts.Search do

  alias ExAliyunOts.Var.Search

  alias ExAliyunOts.Const.{Search.QueryType, Search.ColumnReturnType, Search.SortType, Search.AggregationType, Search.SortOrder, Search.SortMode, Search.GeoDistanceType}

  require QueryType
  require ColumnReturnType
  require SortType
  require AggregationType
  require SortOrder
  require SortMode
  require GeoDistanceType

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

  def geo_distance_query(field_name, distance, center_point) do
    %Search.GeoDistanceQuery{
      field_name: field_name,
      distance: distance,
      center_point: center_point
    }
  end

  def geo_bounding_box_query(field_name, top_left, bottom_right) do
    %Search.GeoBoundingBoxQuery{
      field_name: field_name,
      top_left: top_left,
      bottom_right: bottom_right
    }
  end

  def geo_polygon_query(field_name, points) do
    %Search.GeoPolygonQuery{
      field_name: field_name,
      points: points
    }
  end

  def exists_query(field_name) do
    %Search.ExistsQuery{field_name: field_name}
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

  def group_by_geo_distance(group_name, field_name, opts \\ []) do
    %Search.GroupByGeoDistance{
      name: group_name,
      field_name: field_name,
      lat: Keyword.get(opts, :lat),
      lon: Keyword.get(opts, :lon),
      sub_aggs: Keyword.get(opts, :sub_aggs),
      sub_group_bys: Keyword.get(opts, :sub_group_bys),
      ranges: Keyword.get(opts, :ranges)
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

  def pk_sort(order) do
    %Search.PrimaryKeySort{order: map_query_sort_order(order)}
  end

  def score_sort(order) do
    %Search.ScoreSort{order: map_query_sort_order(order)}
  end

  def field_sort(field_name, opts \\ []) do
    %Search.FieldSort{
      field_name: field_name,
      order: map_query_sort_order(Keyword.get(opts, :order)),
      mode: map_query_sort_mode(Keyword.get(opts, :mode)),
      nested_filter: Keyword.get(opts, :nested_filter)
    }
  end

  def geo_distance_sort(field_name, points, opts) when is_list(points) do
    %Search.GeoDistanceSort{
      field_name: field_name,
      order: map_query_sort_order(Keyword.get(opts, :order)),
      mode: map_query_sort_mode(Keyword.get(opts, :mode)),
      distance_type: map_query_sort_geo_distance_type(Keyword.get(opts, :distance_type)),
      points: points
    }
  end

  def nested_filter(path, filter) when is_map(filter) do
    %Search.NestedFilter{
      path: path,
      filter: filter
    }
  end

  @doc false
  def map_search_options(var, nil) do
    var
  end
  def map_search_options(var, opts) do
    Enum.reduce(opts, var, fn({key, value}, acc) ->
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
    Map.put(var, key, map_query_sort(value))
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
  defp map_query_details(QueryType.geo_distance, query) do
    map_search_options(%Search.GeoDistanceQuery{}, query)
  end
  defp map_query_details(QueryType.geo_bounding_box, query) do
    map_search_options(%Search.GeoBoundingBoxQuery{}, query)
  end
  defp map_query_details(QueryType.geo_polygon, query) do
    map_search_options(%Search.GeoPolygonQuery{}, query)
  end
  defp map_query_details(QueryType.exists, query) do
    map_search_options(%Search.ExistsQuery{}, query)
  end
  defp map_query_details(_query_type, query) do
    raise ExAliyunOts.RuntimeError, "Not supported query when map query details: #{inspect query}"
  end

  defp map_query_sort(nil), do: nil
  defp map_query_sort(sorters) when is_list(sorters) do
    Enum.map(sorters, &map_search_query_sorter/1)
  end

  defp map_search_query_sorter(sorter) when is_list(sorter) do
    {sorter_type, rest_sorter_options} = Keyword.pop(sorter, :type)
    case sorter_type do
      SortType.field ->
        map_search_query_sort_options(%Search.FieldSort{}, rest_sorter_options)
      SortType.geo_distance ->
        map_search_query_sort_options(%Search.GeoDistanceSort{}, rest_sorter_options)
      SortType.pk ->
        map_search_query_sort_options(%Search.PrimaryKeySort{}, rest_sorter_options)
      SortType.score ->
        map_search_query_sort_options(%Search.ScoreSort{}, rest_sorter_options)
      _ ->
        raise ExAliyunOts.RuntimeError, "invalid sorter: #{inspect sorter}"
    end
  end
  defp map_search_query_sorter(%Search.GeoDistanceSort{} = sorter) do
    sorter
  end
  defp map_search_query_sorter(%Search.FieldSort{} = sorter) do
    sorter
  end

  defp map_search_query_sort_options(var, nil) do
    var
  end
  defp map_search_query_sort_options(var, opts) do
    Enum.reduce(opts, var, fn({key, value}, acc) ->
      if value != nil and Map.has_key?(var, key) do
        do_map_search_query_sort_options(key, value, acc)
      else
        acc
      end
    end)
  end

  defp do_map_search_query_sort_options(:order = key, value, var) do
    Map.put(var, key, map_query_sort_order(value))
  end
  defp do_map_search_query_sort_options(:type = key, value, var) do
    Map.put(var, key, map_query_sort_type(value))
  end
  defp do_map_search_query_sort_options(:mode = key, value, var) do
    Map.put(var, key, map_query_sort_mode(value))
  end
  defp do_map_search_query_sort_options(:distance_type = key, value, var) do
    Map.put(var, key, map_query_sort_geo_distance_type(value))
  end
  defp do_map_search_query_sort_options(key, value, var) do
    Map.put(var, key, value)
  end

  defp map_query_sort_order(nil), do: SortOrder.asc
  defp map_query_sort_order(:asc), do: SortOrder.asc
  defp map_query_sort_order(:desc), do: SortOrder.desc
  defp map_query_sort_order(SortOrder.asc), do: SortOrder.asc
  defp map_query_sort_order(SortOrder.desc), do: SortOrder.desc

  defp map_query_sort_type(nil), do: nil
  defp map_query_sort_type(:field), do: SortType.field
  defp map_query_sort_type(:geo_distance), do: SortType.geo_distance
  defp map_query_sort_type(:pk), do: SortType.pk
  defp map_query_sort_type(:score), do: SortType.score

  defp map_query_sort_mode(nil), do: nil
  defp map_query_sort_mode(:min), do: SortMode.min
  defp map_query_sort_mode(:max), do: SortMode.max
  defp map_query_sort_mode(:avg), do: SortMode.avg
  defp map_query_sort_mode(SortMode.min), do: SortMode.min
  defp map_query_sort_mode(SortMode.max), do: SortMode.max
  defp map_query_sort_mode(SortMode.avg), do: SortMode.avg

  defp map_query_sort_geo_distance_type(nil), do: nil
  defp map_query_sort_geo_distance_type(:arc), do: GeoDistanceType.arc
  defp map_query_sort_geo_distance_type(:plane), do: GeoDistanceType.plane
  defp map_query_sort_geo_distance_type(GeoDistanceType.arc), do: GeoDistanceType.arc
  defp map_query_sort_geo_distance_type(GeoDistanceType.plane), do: GeoDistanceType.plane

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

end
