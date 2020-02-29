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

  @doc """
  Use MatchQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: match_query("age", 28)
    ]
  ```
  """
  @doc query: :query
  def match_query(field_name, text, options \\ []) do
    %Search.MatchQuery{
      field_name: field_name,
      text: text,
      minimum_should_match: Keyword.get(options, :minimum_should_match, 1),
    }
  end

  @doc """
  Use MatchAllQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: match_all_query()
    ]
  ```
  """
  @doc query: :query
  def match_all_query() do
    %Search.MatchAllQuery{}
  end

  @doc """
  Use MatchPhraseQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: match_phrase_query("content", "tablestore")
    ]
  ```
  """
  @doc query: :query
  def match_phrase_query(field_name, text) do
    %Search.MatchPhraseQuery{field_name: field_name, text: text}
  end

  @doc """
  Use TermQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: term_query("age", 28)
    ]
  ```
  """
  @doc query: :query
  def term_query(field_name, term) do
    %Search.TermQuery{field_name: field_name, term: term}
  end

  @doc """
  Use TermsQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: terms_query("age", [28, 29, 30])
    ]
  ```
  """
  @doc query: :query
  def terms_query(field_name, terms) when is_list(terms) do
    %Search.TermsQuery{field_name: field_name, terms: terms}
  end

  @doc """
  Use PrefixQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: prefix_query("name", "n")
    ]
  ```
  """
  @doc query: :query
  def prefix_query(field_name, prefix) do
    %Search.PrefixQuery{field_name: field_name, prefix: prefix}
  end

  @doc """
  Use RangeQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: range_query(
        "score",
        from: 60,
        to: 80,
        include_upper: false,
        include_lower: false
      )
    ]
  ```

  Options:

    * `:from`, ...
    * `:to`, ...
    * `:include_lower`, by default it's `true`.
    * `:include_upper`, by default it's `true`.

  """
  @doc query: :query
  def range_query(field_name, options \\ []) do
    %Search.RangeQuery{
      field_name: field_name,
      from: Keyword.get(options, :from),
      to: Keyword.get(options, :to),
      include_lower: Keyword.get(options, :include_lower, true),
      include_upper: Keyword.get(options, :include_upper, true)
    }
  end

  @doc """
  Use WildcardQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: wildcard_query("name", "n*")
    ]
  ```
  """
  @doc query: :query
  def wildcard_query(field_name, value) do
    %Search.WildcardQuery{field_name: field_name, value: value}
  end

  @doc """
  Use BoolQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: bool_query(
        must: range_query("age", from: 20, to: 32),
        must_not: term_query("age", 28)
      )
    ]
  ```

  The following options can be a single `Query` or a list of `Query` to combine the "And | Or | At least"
  serach condication.

  Options:

    * `:must`
    * `:must_not`
    * `:should`

  """
  @doc query: :query
  def bool_query(options) do
    map_search_options(%Search.BoolQuery{}, options)
  end

  @doc """
  Use NestedQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: nested_query(
        "content",
        [
          term_query("content.header", "header1")
        ]
      )
    ]
  ```

  Options(TODO):

    * `:score_mode`, available options have `:none` | `:avg` | `:max` | `:total` | `:min`, by default
    it's `:none`
  """
  @doc query: :query
  def nested_query(path, query, options \\ []) do
    options = Keyword.merge(options, [path: path, query: query])
    map_search_options(%Search.NestedQuery{}, options)
  end

  @doc """
  Use GeoDistanceQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: geo_distance_query("location", 500_000, "5,5")
    ]
  ```

  Please notice that all geographic coordinates are in "$latitude,$longitude" format.
  """
  @doc query: :query
  def geo_distance_query(field_name, distance, center_point) do
    %Search.GeoDistanceQuery{
      field_name: field_name,
      distance: distance,
      center_point: center_point
    }
  end

  @doc """
  Use GeoBoundingBoxQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: geo_bounding_box_query("location", "10,-10", "-10,10")
    ]
  ```

  Please notice that all geographic coordinates are in "$latitude,$longitude" format.
  """
  @doc query: :query
  @spec geo_bounding_box_query(field_name :: String.t(), top_left :: String.t(), bottom_right :: String.t())
    :: %Search.GeoBoundingBoxQuery{}
  def geo_bounding_box_query(field_name, top_left, bottom_right) do
    %Search.GeoBoundingBoxQuery{
      field_name: field_name,
      top_left: top_left,
      bottom_right: bottom_right
    }
  end

  @doc """
  Use GeoPolygonQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: geo_polygon_query("location", ["11,11", "0,0", "1,5"])
    ]
  ```

  Please notice that all geographic coordinates are in "$latitude,$longitude" format.
  """
  @doc query: :query
  @spec geo_polygon_query(field_name :: String.t(), geo_points :: list())
    :: %Search.GeoPolygonQuery{}
  def geo_polygon_query(field_name, geo_points) do
    %Search.GeoPolygonQuery{
      field_name: field_name,
      points: geo_points
    }
  end

  @doc """
  Use ExistsQuery as the nested `:query` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: exists_query("values")
    ]
  ```
  """
  @doc query: :query
  @spec exists_query(field_name :: String.t())
    :: %Search.ExistsQuery{}
  def exists_query(field_name) do
    %Search.ExistsQuery{field_name: field_name}
  end

  @doc """
  Calculate the minimum value of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_min("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  Options

    * `:missing`, when the field is not existed in a row of data, if `:missing` is not set, the row will be ignored
    in statistics; if `:missing` is set, the row will use `:missing` value to participate in the statistics of minimum
    value, by default it's `nil` (not-set).
  """
  @doc aggs: :aggs
  @spec agg_min(aggregation_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.Aggregation{}
  def agg_min(aggregation_name, field_name, options \\ []) do
    %Search.Aggregation{
      type: AggregationType.min,
      name: aggregation_name,
      field_name: field_name,
      missing: Keyword.get(options, :missing)
    }
  end

  @doc """
  Calculate the maximum value of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_max("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  Options

    * `:missing`, when the field is not existed in a row of data, if `:missing` is not set, the row will be ignored
    in statistics; if `:missing` is set, the row will use `:missing` value to participate in the statistics of maximum
    value, by default it's `nil` (not-set).
  """
  @doc aggs: :aggs
  @spec agg_max(aggregation_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.Aggregation{}
  def agg_max(aggregation_name, field_name, options \\ []) do
    %Search.Aggregation{
      type: AggregationType.max,
      name: aggregation_name,
      field_name: field_name,
      missing: Keyword.get(options, :missing)
    }
  end

  @doc """
  Calculate the average value of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_avg("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  Options

    * `:missing`, when the field is not existed in a row of data, if `:missing` is not set, the row will be ignored
    in statistics; if `:missing` is set, the row will use `:missing` value to participate in the statistics of average
    value, by default it's `nil` (not-set).
  """
  @doc aggs: :aggs
  @spec agg_avg(aggregation_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.Aggregation{}
  def agg_avg(aggregation_name, field_name, options \\ []) do
    %Search.Aggregation{
      type: AggregationType.avg,
      name: aggregation_name,
      field_name: field_name,
      missing: Keyword.get(options, :missing)
    }
  end

  @doc """
  Calculate the distinct count of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_distinct_count("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  Options

    * `:missing`, when the field is not existed in a row of data, if `:missing` is not set, the row will be ignored
    in statistics; if `:missing` is set, the row will use `:missing` value to participate in the statistics of distinct 
    count, by default it's `nil` (not-set).
  """
  @doc aggs: :aggs
  @spec agg_max(aggregation_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.Aggregation{}
  def agg_distinct_count(aggregation_name, field_name, options \\ []) do
    %Search.Aggregation{
      type: AggregationType.distinct_count,
      name: aggregation_name,
      field_name: field_name,
      missing: Keyword.get(options, :missing)
    }
  end

  @doc """
  Calculate the summation of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_sum("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  Options

    * `:missing`, when the field is not existed in a row of data, if `:missing` is not set, the row will be ignored
    in statistics; if `:missing` is set, the row will use `:missing` value to participate in the statistics of summation
    value, by default it's `nil` (not-set).
  """
  @doc aggs: :aggs
  @spec agg_sum(aggregation_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.Aggregation{}
  def agg_sum(aggregation_name, field_name, options \\ []) do
    %Search.Aggregation{
      type: AggregationType.sum,
      name: aggregation_name,
      field_name: field_name,
      missing: Keyword.get(options, :missing)
    }
  end

  @doc """
  Calculate the count of the assigned field by aggregation in the nested `:aggs` option of `:search_query`
  option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      aggs: [
        agg_sum("agg_name", "score")
      ]
    ]
  ```

  The `aggregation_name` can be any business description string, when get the calculated results, we need to use
  it to fetch them.

  If the field is not existed in a row of data, then this row does not participate in the statistics of count.
  """
  @doc aggs: :aggs
  @spec agg_count(aggregation_name :: String.t(), field_name :: String.t())
    :: %Search.Aggregation{}
  def agg_count(aggregation_name, field_name) do
    %Search.Aggregation{
      type: AggregationType.count,
      name: aggregation_name,
      field_name: field_name,
    }
  end

  @doc """
  The `:group_bys` results are grouped according to the value of a field, the same value will be put into a group, finally, 
  the value of each group and the number corresponding to the value will be returned.

  We can set it in the nested `:group_bys` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search_query: [
    query: ...,
    group_bys: [
      group_by_field("group_name", "type",
        size: 3,
        sub_group_bys: [
          group_by_field("sub_gn1", "is_actived")
        ],
        sort: [
          row_count_sort(:asc),
          group_key_sort(:desc)
        ]
      ),
      group_by_field("group_name2", "is_actived")
    ]
  ]

  The `group_name` can be any business description string, when get the grouped results, we need to use
  it to fetch them.

  Options(TODO)

    * `:sort`, optional,
    * `:size`, optional,
    * `:sub_group_bys`, optional,
    * `:sub_aggs`, optional,
  ```
  """
  @doc group_bys: :group_bys
  @spec group_by_field(group_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.GroupByField{}
  def group_by_field(group_name, field_name, options \\ []) do
    %Search.GroupByField{
      name: group_name,
      field_name: field_name,
      size: Keyword.get(options, :size),
      sub_aggs: Keyword.get(options, :sub_aggs),
      sub_group_bys: Keyword.get(options, :sub_group_bys),
      sort: Keyword.get(options, :sort)
    }
  end

  @doc """
  The `:group_bys` results are grouped according to the range of a field, if the field value is within a range,
  it will be put into a group, finally, the number corresponding to the value will be returned.

  We can set it in the nested `:group_bys` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      group_bys: [
        group_by_range("group_name", "price",
          ranges: [
            {0, 18},
            {18, 50}
          ],
          sub_group_bys: [
            group_by_field("sorted_by_type", "type",
              sort: [
                group_key_sort(:asc)
              ]
            )
          ],
          sub_aggs: [
            agg_distinct_count("distinct_price", "price")
          ]
        )
      ]
    ]

  The `group_name` can be any business description string, when get the grouped results, we need to use
  it to fetch them.

  Please notice that each range item(as a tuple, according to {`from`, `to`}) of `ranges`, its start is greater
  than or equal to `from`, and its ending is less than `to`, the range interval value can be integer or float.

  Options

    * `:sub_group_bys`, optional,
    * `:sub_aggs`, optional
  ```
  """
  @doc group_bys: :group_bys
  @spec group_by_range(group_name :: String.t(), field_name :: String.t(), ranges :: list(),
    options :: Keyword.t()) :: %Search.GroupByRange{}
  def group_by_range(group_name, field_name, ranges, options \\ []) do
    %Search.GroupByRange{
      name: group_name,
      field_name: field_name,
      ranges: ranges,
      sub_aggs: Keyword.get(options, :sub_aggs),
      sub_group_bys: Keyword.get(options, :sub_group_bys),
    }
  end

  @doc """
  On the query results, group by filters (they're `Query` usecase), and then get the number of matched filters,
  the order of the returned results is the same as that of the added filter(s).

  We can set it in the nested `:group_bys` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      group_bys: [
        group_by_filter(
          "group_name",
          [
            term_query("is_actived", true),
            range_query("price", from: 50)
          ]
        )
      ]
    ]
  ```

  Options

    * `:sub_aggs`
    * `:sub_group_bys`
  """
  @doc group_bys: :group_bys
  @spec group_by_filter(group_name :: String.t(), filters :: list(), options :: Keyword.t())
    :: %Search.GroupByFilter{}
  def group_by_filter(group_name, filters, options \\ []) when is_list(filters) do
    %Search.GroupByFilter{
      name: group_name,
      filters: filters,
      sub_aggs: Keyword.get(options, :sub_aggs),
      sub_group_bys: Keyword.get(options, :sub_group_bys)
    }
  end

  @doc """
  The query results are grouped according to the range from a certain center geo point, if the distance difference
  is within a certain range, it will be put into a group, and finally the number of corresponding items in each
  range will be returned.

  We can set it in the nested `:group_bys` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      group_bys: [
        group_by_geo_distance("test", "location",
          lat: 0,
          lon: 0,
          ranges: [
            {0, 100_000},
            {100_000, 500_000},
            {500_000, 1000_000},
          ],
          sub_aggs: [
            agg_sum("test_sum", "value")
          ]
        )
      ]
    ]
  ```

  Options

    * `:lat`, required
    * `:lon`, required
    * `:ranges`, required
    * `:sub_aggs`, optional
    * `:sub_group_bys`, optional
  """
  @doc group_bys: :group_bys
  @spec group_by_geo_distance(group_name :: String.t(), field_name :: String.t(), options :: Keyword.t())
    :: %Search.GroupByGeoDistance{}
  def group_by_geo_distance(group_name, field_name, options \\ []) do
    %Search.GroupByGeoDistance{
      name: group_name,
      field_name: field_name,
      lat: Keyword.fetch!(options, :lat),
      lon: Keyword.fetch!(options, :lon),
      sub_aggs: Keyword.get(options, :sub_aggs),
      sub_group_bys: Keyword.get(options, :sub_group_bys),
      ranges: Keyword.fetch!(options, :ranges)
    }
  end

  @doc """
  Use in `group_by_field/3` scenario, in ascending/descending order of field literal.

  In the following example, the returned results will be sorted in descending order of the `"type"` field:

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      group_bys: [
        group_by_field(
          "group_name",
          "type",
          sub_group_bys: [
            ...
          ],
          sort: [
            group_key_sort(:desc)
          ]
        ),
      ]
    ]
  ```
  """
  @doc sort_in_group_bys: :sort_in_group_bys
  @spec group_key_sort(order :: :asc | :desc) :: %Search.GroupKeySort{}
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

  @doc """
  Use in `group_by_field/3` scenario, in ascending/descending order of row(s) count.

  In the following example, the returned results will be sorted in ascending order of the matched row(s):

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      group_bys: [
        group_by_field(
          "group_name",
          "type",
          sub_group_bys: [
            ...
          ],
          sort: [
            row_count_sort(:asc)
          ]
        ),
      ]
    ]
  ```
  """
  @doc sort_in_group_bys: :sort_in_group_bys
  @spec row_count_sort(order :: :asc | :desc) :: %Search.RowCountSort{}
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

  @doc """
  TODO
  """
  @doc sort_in_group_bys: :sort_in_group_bys
  @spec sub_agg_sort(sub_agg_name :: String.t(), order :: :asc | :desc)
    :: %Search.SubAggSort{}
  def sub_agg_sort(sub_agg_name, _)
      when is_bitstring(sub_agg_name) == false
      when sub_agg_name == "" do
    raise ExAliyunOts.RuntimeError, "require sub_agg_name as a string, but input `#{inspect sub_agg_name}`"
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

  @doc """
  """
  @doc sort: :sort
  @spec pk_sort(order :: :asc | :desc) :: %Search.PrimaryKeySort{}
  def pk_sort(order) do
    %Search.PrimaryKeySort{order: map_query_sort_order(order)}
  end

  @doc sort: :sort
  @spec score_sort(order :: :asc | :desc) :: %Search.ScoreSort{}
  def score_sort(order) do
    %Search.ScoreSort{order: map_query_sort_order(order)}
  end

  @doc """
  Sort by the value of a column, use it in the nested `:sort` option of `:search_query` option in `ExAliyunOts.search/4`.

  ```elixir
  search table, index_name,
    search_query: [
      query: ...,
      sort: [
        field_sort("field_a", order: :desc)
      ]
    ]
  ```

  If there's a nested type of search index, and they are a integer or float list, we can use `:mode` to
  sort according to the minimum/maximum/average value of the list, by default it's `:nil`.

  For example, there's a nested type as "values" field, the following query will find "values" field existed
  as matched rows, and sort by the minimum value of list items.

  ```elixir
  search table, index_name,
    search_query: [
      query: exists_query("values"),
      sort: [
        field_sort("values", mode: :min)
      ]
    ]
  ```

  Still for nested type of search index, we can sort by the nested value via `:nested_filter` option, for example,
  sort by the value of "content.header" in `:desc` order.

  ```elixir
  search table, index_name,
    search_query: [
      query: nested_query(
        "content",
        [
          exists_query("content.header")
        ]
      ),
      sort: [
        field_sort("content.header",
          order: :desc,
          nested_filter: nested_filter(
            "content",
            prefix_query("content.header", "header")
          )
        )
      ]
    ]
  ```

  Please ensure that the query criteria matched will participate in sorting, if there exists any not matched case
  will lead to uncertainty of sorting results.

  Options

    * `:mode`, optional, available options are `:min` | `:max` | `:avg`, by default it's `:nil`;
    * `:order`, optional, available options are `:asc` | `:desc`, by default it's `:asc`;
    * `:nested_filter`, optional, see `nested_filter/2` for details.
  """
  @doc sort: :sort
  @spec field_sort(field_name :: String.t(), options :: Keyword.t()) :: %Search.FieldSort{}
  def field_sort(field_name, options \\ []) do
    %Search.FieldSort{
      field_name: field_name,
      order: map_query_sort_order(Keyword.get(options, :order)),
      mode: map_query_sort_mode(Keyword.get(options, :mode)),
      nested_filter: Keyword.get(options, :nested_filter)
    }
  end

  @doc """
  Geographic distance sorting, according to the sum of distances between to the input geographical points,
  sort by the minimum/maximum/average summation value.

  ```elixir
  search table, index_name,
    search_query: [
      query: geo_distance_query("location", 500_000, "5,5"),
      sort: [
        geo_distance_sort("location", ["5.14,5.21"], order: :asc)
      ]
    ]
  ```

  The input points are a list of string, each format as "$latitude,$longitude".

  Options

    * `:order`, optional, available options are `:asc` | `:desc`;
    * `:mode`, optional, used for nested type field within integer or float, as `:min` will sort by the minimum value of
    items, as `:max` will sort by the maximum value of items, as `:avg` will sort by the average value of items, by default
    it's `:nil`;
    * `:distance_type`, optional, available options are `:arc` | `:plane`, as `:arc` means distance calculated by arc surface, as `:plane` means distance calculated by plane.
  """
  @doc sort: :sort
  @spec geo_distance_sort(field_name :: String.t(), options :: list(), options :: Keyword.t())
    :: %Search.GeoDistanceSort{}
  def geo_distance_sort(field_name, points, options) when is_list(points) do
    %Search.GeoDistanceSort{
      field_name: field_name,
      order: map_query_sort_order(Keyword.get(options, :order)),
      mode: map_query_sort_mode(Keyword.get(options, :mode)),
      distance_type: map_query_sort_geo_distance_type(Keyword.get(options, :distance_type)),
      points: points
    }
  end


  @doc """
  Use for the nested type field in `field_sort/2` as `:nested_filter` option, the input `filter`
  is a Query to filter results.

  ```elixir
  search table, index_name,
    search_query: [
      query: nested_query(
        "content",
        [
          exists_query("content.header")
        ]
      ),
      sort: [
        field_sort("content.header",
          order: :desc,
          nested_filter: nested_filter(
            "content",
            prefix_query("content.header", "header")
          )
        )
      ]
    ]
  ```

  Please ensure that the query criteria matched will participate in sorting, if there exists any not matched case
  will lead to uncertainty of sorting results.
  """
  @doc sort: :sort
  @spec nested_filter(path :: String.t(), filter :: map())
    :: %Search.NestedFilter{}
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
  def map_search_options(var, options) do
    Enum.reduce(options, var, fn({key, value}, acc) ->
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
  defp map_search_query_sort_options(var, options) do
    Enum.reduce(options, var, fn({key, value}, acc) ->
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
