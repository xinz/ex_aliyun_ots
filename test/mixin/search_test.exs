defmodule ExAliyunOts.MixinTest.Search do
  use ExUnit.Case
  @instance_key EDCEXTestInstance
  use ExAliyunOts, instance: @instance_key
  require Logger
  alias ExAliyunOts.Client
  alias ExAliyunOts.Var.Search
  alias ExAliyunOtsTest.Support.Search, as: TestSupportSearch

  import ExAliyunOts.SearchTestHelper, only: [assert_search: 4]

  @table "test_search"
  @indexes ["test_search_index", "test_search_index2"]
  @table_group_by "test_search_group_by"
  @index_group_by "test_search_index_group_by"
  @table_text_analyzer "test_search_text_analyzer"
  @index_text_analyzer "test_search_index_text_analyzer"

  setup_all do
    Application.ensure_all_started(:ex_aliyun_ots)
    clean_all()

    TestSupportSearch.init(@instance_key, @table, @indexes,
      table_group_by: @table_group_by,
      index_group_by: @index_group_by,
      table_text_analyzer: @table_text_analyzer,
      index_text_analyzer: @index_text_analyzer
    )

    on_exit(&clean_all/0)

    [index_name: "test_search_index"]
  end

  defp clean_all do
    TestSupportSearch.clean(@instance_key, @table, @indexes)
    TestSupportSearch.clean_group_by(@instance_key, @table_group_by, @index_group_by)

    TestSupportSearch.clean_text_analyzer(
      @instance_key,
      @table_text_analyzer,
      @index_text_analyzer
    )
  end

  test "list search index" do
    {:ok, response} = list_search_index(@table)
    assert length(response.indices) == 2
  end

  test "describe search index" do
    index_name = "test_search_index2"
    {:ok, response} = describe_search_index(@table, index_name)
    schema = response.schema
    field_schemas = schema.field_schemas

    Enum.map(field_schemas, fn field_schema ->
      assert field_schema.field_type == FieldType.nested()

      Enum.with_index(field_schema.field_schemas)
      |> Enum.map(fn {sub_field_schema, index} ->
        cond do
          index == 0 ->
            assert sub_field_schema.field_name == "header"
            assert sub_field_schema.field_type == FieldType.keyword()

          index == 1 ->
            assert sub_field_schema.field_name == "body"
            assert sub_field_schema.field_type == FieldType.keyword()
        end
      end)
    end)
  end

  test "match query", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: {ColumnReturnType.specified(), ["class", "name"]},
          # columns_to_get: ColumnReturnType.none,
          # columns_to_get: ColumnReturnType.all,
          # columns_to_get: ["class"],
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "age",
              text: "28"
            ],
            limit: 1
          ]
        ],
        2
      )

    [{[{_pk_key, pk_value}], attrs}] = response.rows
    assert pk_value == "a2"
    assert length(attrs) == 2
  end

  test "columns_to_get", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: {:specified, ["class", "name"]},
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{[{_pk_key, pk_value}], attrs}] = response.rows
    assert pk_value == "a2"
    assert length(attrs) == 2

    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: :none,
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{_, attrs}] = response.rows
    assert attrs == nil

    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: :all,
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{_, attrs}] = response.rows
    assert length(attrs) > 2

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{_, attrs_2}] = response.rows
    assert attrs == attrs_2

    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: ["class"],
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{_, [{column_name, _, _}]}] = response.rows
    assert column_name == "class"
  end

  test "match query with match_query/3 function", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          columns_to_get: ["class", "name"],
          search_query: [
            query: match_query("age", "28"),
            limit: 1
          ]
        ],
        2
      )

    [{[{_pk_key, pk_value}], attrs}] = response.rows
    assert pk_value == "a2"
    assert length(attrs) == 2
  end

  test "term query", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.term(),
              field_name: "age",
              term: 28
            ]
          ]
        ],
        2
      )

    assert length(response.rows) == 2
  end

  test "term query with term_query/2 function", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("age", 28)
          ]
        ],
        2
      )

    assert length(response.rows) == 2
  end

  test "terms query with sort", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.terms(),
              field_name: "age",
              terms: [22, 26, 27]
            ],
            sort: [
              field_sort("age", order: :asc),
              field_sort("name", order: :asc)
            ]
          ]
        ],
        3
      )

    assert length(response.rows) == 3
  end

  test "terms query with terms_query/2", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: terms_query("age", [22, 26, 27]),
            sort: [
              field_sort("age", order: :asc),
              [type: SortType.field(), field_name: "name", order: SortOrder.asc()]
            ]
          ]
        ],
        3
      )

    assert length(response.rows) == 3
  end

  test "prefix query", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.prefix(),
              field_name: "name",
              prefix: "n"
            ],
            sort: [
              field_sort("age"),
              field_sort("name")
            ]
          ],
          columns_to_get: ["age", "name"]
        ],
        9
      )

    assert length(response.rows) == 9
  end

  test "prefix query with prefix_query/2", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: prefix_query("name", "n"),
            sort: [
              field_sort("age"),
              field_sort("name")
            ]
          ],
          columns_to_get: ["age", "name"]
        ],
        9
      )

    assert length(response.rows) == 9
  end

  test "wildcard query", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.wildcard(),
              field_name: "name",
              value: "n*"
            ],
            sort: [
              field_sort("age"),
              field_sort("name")
            ]
          ],
          columns_to_get: ColumnReturnType.all()
        ],
        9
      )

    assert length(response.rows) == 9
  end

  test "wildcard query with wildcard_query/2 function", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: wildcard_query("name", "n*"),
            sort: [
              field_sort("age"),
              field_sort("name")
            ]
          ],
          columns_to_get: ColumnReturnType.all()
        ],
        9
      )

    assert length(response.rows) == 9
  end

  test "range query", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.range(),
              field_name: "score",
              from: 60,
              to: 80,
              # `include_upper` as true and `include_lower` as true by default
              include_upper: false,
              include_lower: false
            ],
            sort: [
              field_sort("age", order: :desc),
              field_sort("name")
            ]
          ]
        ],
        2
      )

    primary_keys =
      Enum.map(response.rows, fn row ->
        {[{_pk_key, pk_value}], _attrs} = row
        pk_value
      end)

    assert primary_keys == ["a3", "a6"]

    # range_query with collapse
    query = [
      type: QueryType.range(),
      field_name: "age",
      from: 20,
      to: 32,
      include_upper: true,
      include_lower: true
    ]

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: query,
            collapse: "class"
          ]
        ],
        9
      )

    assert length(response.rows) == 5

    # input invalid `collapse` as list will be ignored,
    # only string as field name is allowed.
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: query,
            collapse: ["class"]
          ]
        ],
        9
      )

    assert length(response.rows) == 9
  end

  test "range query with range_query/2", %{index_name: index_name} do
    assert_search(
      @table,
      index_name,
      [
        search_query: [
          query:
            range_query("score",
              from: 60,
              to: 80,
              include_upper: false,
              include_lower: false
            ),
          sort: [
            field_sort("age", order: :desc),
            field_sort("name")
          ]
        ]
      ],
      2
    )
  end

  test "bool query with must/must_not", %{index_name: index_name} do
    # using keyword expression for `query`
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.bool(),
              must: [
                [type: QueryType.range(), field_name: "age", from: 20, to: 32]
              ],
              must_not: [
                [type: QueryType.term(), field_name: "age", term: 28]
              ]
            ],
            sort: [
              field_sort("age", order: :desc),
              field_sort("name")
            ]
          ]
        ],
        7
      )

    assert length(response.rows) == 7

    attr_ages =
      Enum.map(response.rows, fn {[{_pk_key, _pk_value}], attrs} ->
        {"age", age, _ts} = List.first(attrs)
        age
      end)

    assert Enum.sort(attr_ages, &(&1 >= &2)) == attr_ages
    assert 28 not in attr_ages
  end

  test "bool query with bool_query/1", %{index_name: index_name} do
    # using `bool_query` function for `query`
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              bool_query(
                must: [
                  range_query("age", from: 20, to: 32)
                ],
                must_not: term_query("age", 28)
              ),
            sort: [
              field_sort("age", order: :desc),
              field_sort("name")
            ]
          ]
        ],
        7
      )

    assert length(response.rows) == 7

    attr_ages =
      Enum.map(response.rows, fn {[{_pk_key, _pk_value}], attrs} ->
        {"age", age, _ts} = List.first(attrs)
        age
      end)

    assert Enum.sort(attr_ages, &(&1 >= &2)) == attr_ages
    assert 28 not in attr_ages
  end

  test "bool query with should", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.bool(),
              should: [
                [type: QueryType.range(), field_name: "age", from: 20, to: 32],
                [type: QueryType.term(), field_name: "score", term: 66.78]
              ],
              minimum_should_match: 2
            ],
            sort: [
              field_sort("age", order: :desc),
              field_sort("name")
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
    [{[{_pk_key, pk_value}], _attrs}] = response.rows
    assert pk_value == "a3"
  end

  test "bool query - should case with bool_query/1", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              bool_query(
                should: [
                  range_query("age", from: 20, to: 32),
                  term_query("score", 66.78)
                ],
                minimum_should_match: 2
              ),
            sort: [
              field_sort("age", order: :desc),
              field_sort("name")
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
    [{[{_pk_key, pk_value}], _attrs}] = response.rows
    assert pk_value == "a3"
  end

  test "nested query" do
    index_name = "test_search_index2"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.nested(),
              path: "content",
              query: [
                type: QueryType.term(),
                field_name: "content.header",
                term: "header1"
              ]
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    [{[{"partition_key", id}], [{"content", value, _ts}]}] = response.rows

    assert id == "a9"
    assert value == "[{\"body\":\"body1\",\"header\":\"header1\"}]"
  end

  test "nested query with nested_query/3" do
    index_name = "test_search_index2"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              nested_query(
                "content",
                term_query("content.header", "header1")
              )
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    [{[{"partition_key", id}], [{"content", value, _ts}]}] = response.rows

    assert id == "a9"
    assert value == "[{\"body\":\"body1\",\"header\":\"header1\"}]"

    # Another way of expression
    index_name = "test_search_index2"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              nested_query(
                "content",
                type: QueryType.term(),
                field_name: "content.header",
                term: "header1"
              )
          ]
        ],
        1
      )

    assert length(response.rows) == 1
  end

  test "field_sort with nested_filter" do
    index_name = "test_search_index2"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              nested_query(
                "content",
                exists_query("content.header")
              ),
            sort: [
              field_sort("content.header",
                order: :desc,
                nested_filter:
                  nested_filter(
                    "content",
                    prefix_query("content.header", "header")
                  )
              )
            ]
          ]
        ],
        2
      )

    [row1, row2] = response.rows
    {_, [{"content", c1, _}]} = row1
    {_, [{"content", c2, _}]} = row2

    assert c1 == "[{\"body\":\"body2\",\"header\":\"header2\"}]"
    assert c2 == "[{\"body\":\"body1\",\"header\":\"header1\"}]"
  end

  test "field_sort with min/max/avg mode for array", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: exists_query("values"),
            sort: [
              field_sort("values",
                mode: :min
              )
            ]
          ]
        ],
        4
      )

    [row1, row2, row3, row4] = response.rows
    {[{_, id1}], _} = row1
    {[{_, id2}], _} = row2
    {[{_, id3}], _} = row3
    {[{_, id4}], _} = row4
    assert id1 == "a3" and id2 == "a1" and id3 == "a2" and id4 == "a4"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: exists_query("values"),
            sort: [
              field_sort("values",
                mode: :max,
                order: :desc
              )
            ]
          ]
        ],
        4
      )

    [row1, row2, row3, row4] = response.rows
    {[{_, id1}], _} = row1
    {[{_, id2}], _} = row2
    {[{_, id3}], _} = row3
    {[{_, id4}], _} = row4
    assert id1 == "a2" and id2 == "a4" and id3 == "a3" and id4 == "a1"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: exists_query("values"),
            sort: [
              field_sort("values",
                mode: :avg,
                order: :asc
              )
            ]
          ]
        ],
        4
      )

    [row1, row2, row3, row4] = response.rows
    {[{_, id1}], _} = row1
    {[{_, id2}], _} = row2
    {[{_, id3}], _} = row3
    {[{_, id4}], _} = row4
    assert id1 == "a1" and id2 == "a3" and id3 == "a2" and id4 == "a4"
  end

  test "exists query", %{index_name: index_name} do
    # search exists_query for `comment` field
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.exists(),
              field_name: "comment"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) >= 1

    # serach exists_query for `comment` field as nil column
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.bool(),
              must_not: [
                [type: QueryType.exists(), field_name: "comment"]
              ]
            ],
            limit: 100
          ]
        ],
        10
      )

    assert response.next_token == nil
    assert length(response.rows) == 10
  end

  test "exists query with exists_query/1", %{index_name: index_name} do
    # search exists_query for `comment` field
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: exists_query("comment")
          ]
        ],
        1
      )

    assert length(response.rows) >= 1

    # serach exists_query for `comment` field as nil column
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: bool_query(must_not: exists_query("comment")),
            limit: 100
          ]
        ],
        10
      )

    assert response.next_token == nil
    assert length(response.rows) == 10
  end

  test "array keyword query", %{index_name: index_name} do
    # Data Source
    #
    # "a1" => ["1", "2", "3"]
    # "a2" => ["2", "3"]
    # "a3" => ["4", "1"]
    # "a4" => ["4"]
    #

    # contains "1" and "2" both
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              bool_query(
                must: [
                  term_query("tags", "1"),
                  term_query("tags", "2")
                ]
              )
          ]
        ],
        1
      )

    # response.rows as "a1"
    assert length(response.rows) == 1

    # contains "1" or "2"
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: terms_query("tags", ["1", "2"])
          ]
        ],
        3
      )

    # response.rows as "a1", "a2", "a3"
    assert length(response.rows) == 3

    # contains "1"
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("tags", "1")
          ]
        ],
        2
      )

    # response.rows as "a1", "a3"
    assert length(response.rows) == 2

    # contains ("2" or "4") or "1"
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              bool_query(
                should: [
                  terms_query("tags", ["2", "4"]),
                  terms_query("tags", ["1"])
                ]
              )
          ]
        ],
        4
      )

    # response.rows as "a1", "a2", "a3", "a4"
    assert length(response.rows) == 4

    # contains ("2" and "3" both) or ("4" and "1" both)
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query:
              bool_query(
                should: [
                  bool_query(
                    must: [
                      term_query("tags", "2"),
                      term_query("tags", "3")
                    ]
                  ),
                  bool_query(
                    must: [
                      term_query("tags", "4"),
                      term_query("tags", "1")
                    ]
                  )
                ]
              )
          ]
        ],
        3
      )

    # response.rows as "a1", "a2", "a3"
    assert length(response.rows) == 3
  end

  test "avg aggregation", %{index_name: index_name} do
    agg_name = "test_avg_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_avg(agg_name, "score")
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.avg, agg_name) == 74.0275

    missing = 1

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_avg(agg_name, "score", missing: missing)
            ]
          ]
        ],
        6
      )

    total_score =
      Enum.reduce(response.rows, 0, fn {_, attrs}, cal ->
        {"score", score, _} = List.keyfind(attrs, "score", 0)

        cond do
          is_integer(score) -> cal + missing
          is_float(score) -> cal + score
        end
      end)

    assert Map.get(response.aggs.avg, agg_name) == total_score / length(response.rows)
  end

  test "distinct_count aggregation", %{index_name: index_name} do
    agg_name = "test_distinct_count_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_distinct_count(agg_name, "class")
            ]
          ]
        ],
        6
      )

    distinct_count_aggs_map = response.aggs.distinct_count
    distinct_class_types_size = 4

    assert Map.get(distinct_count_aggs_map, agg_name) == distinct_class_types_size

    class_mapset =
      Enum.reduce(response.rows, MapSet.new([]), fn {_, attrs}, acc ->
        {"class", class, _} = List.keyfind(attrs, "class", 0)
        MapSet.put(acc, class)
      end)

    assert MapSet.size(class_mapset) == distinct_class_types_size

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_distinct_count(agg_name, "place")
            ]
          ]
        ],
        6
      )

    distinct_count_aggs_map = response.aggs.distinct_count
    assert Map.get(distinct_count_aggs_map, agg_name) == 5

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_distinct_count(agg_name, "place", missing: 1)
            ]
          ]
        ],
        6
      )

    distinct_count_aggs_map = response.aggs.distinct_count
    total_hits = response.total_hits

    {existed, miss} =
      Enum.reduce(response.rows, {0, 0}, fn {_, attrs}, {existed, miss} ->
        case List.keyfind(attrs, "place", 0) do
          nil ->
            {existed, miss + 1}

          {"place", _place, _} ->
            {existed + 1, miss}
        end
      end)

    assert total_hits == existed + miss
    assert Map.get(distinct_count_aggs_map, agg_name) == total_hits
  end

  test "min aggregation", %{index_name: index_name} do
    agg_name = "test_min_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_min(agg_name, "score")
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.min, agg_name) == 41.01

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: [
              type: QueryType.term(),
              field_name: "is_actived",
              term: true
            ],
            aggs: [
              agg_min(agg_name, "score", missing: 1)
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.min, agg_name) == 1

    min_value_when_miss = -100

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_min(agg_name, "place", missing: min_value_when_miss)
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.min, agg_name) == min_value_when_miss
  end

  test "max aggregation", %{index_name: index_name} do
    agg_name = "test_max_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_max(agg_name, "score")
            ],
            limit: 0
          ]
        ],
        6
      )

    assert response.rows == []
    assert Map.get(response.aggs.max, agg_name) == 99.71

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_max(agg_name, "score", missing: 0)
            ],
            limit: 0
          ]
        ],
        6
      )

    assert response.rows == []
    assert Map.get(response.aggs.max, agg_name) == 99.71

    max_value_when_miss = 1000

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_max(agg_name, "place", missing: max_value_when_miss)
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.max, agg_name) == max_value_when_miss
  end

  test "sum aggregation", %{index_name: index_name} do
    agg_name = "test_sum_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_sum(agg_name, "score")
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.sum, agg_name) == 296.11

    missing = 1

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_sum(agg_name, "score", missing: missing)
            ]
          ]
        ],
        6
      )

    # Since there are 2 records of `score` fields are integer,
    # there will processed as missed case to sum aggregation.
    calculated_score =
      Enum.reduce(response.rows, 0, fn {_, attrs}, cal ->
        {"score", score, _} = List.keyfind(attrs, "score", 0)

        cond do
          is_integer(score) -> cal + missing
          is_float(score) -> cal + score
        end
      end)

    assert Map.get(response.aggs.sum, agg_name) == calculated_score
  end

  test "count aggregation", %{index_name: index_name} do
    # the `score` field of test_search_index index is double type,
    # so aggregation on `score` field will only process double type of this field,
    # the long (integer) type of the `score` field will be ignored when aggregate.
    agg_name = "test_count_agg"

    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_count(agg_name, "score")
            ]
          ]
        ],
        6
      )

    assert Map.get(response.aggs.count, agg_name) == 4
  end

  test "multi aggregations", %{index_name: index_name} do
    response =
      assert_search(
        @table,
        index_name,
        [
          search_query: [
            query: term_query("is_actived", true),
            aggs: [
              agg_count("count_agg_1", "score"),
              agg_sum("sum_agg_1", "score"),
              agg_max("max_agg_1", "place"),
              agg_sum("sum_agg_2", "place"),
              agg_distinct_count("dc_agg_1", "place", missing: 1)
            ]
          ]
        ],
        6
      )

    aggs = response.aggs
    assert Map.get(aggs.count, "count_agg_1") == 4

    aggs_sum_map = aggs.sum
    #
    # the value of max/min/sum aggregation currently return float value even though define
    # this search index field as an integer type.
    #
    assert Map.get(aggs_sum_map, "sum_agg_1") == 296.11
    assert Map.get(aggs_sum_map, "sum_agg_2") == 45.0

    assert Map.get(aggs.max, "max_agg_1") == 20.0

    assert Map.get(aggs.distinct_count, "dc_agg_1") == length(response.rows)
  end

  test "group_by_field" do
    # If you only care about the aggregated data, you can get better performance
    # by setting limit = 0 and not getting the returned rows
    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
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
        ],
        9
      )

    assert response.rows == []

    # `type: type3 and is_actived: true` condition has 2 row records
    group_bys_field = response.group_bys.by_field
    group1 = Map.get(group_bys_field, "group_name")
    [item1, item2, item3] = group1
    assert item1.row_count <= item2.row_count and item2.row_count <= item3.row_count
    item1_sub_group_bys = Map.get(item1.sub_group_bys.by_field, "sub_gn1")
    assert List.first(item1_sub_group_bys).row_count == 2

    # `is_actived: true` has 8 row records
    # `is_actived: false` has 1 row records
    # they are sorted by row count descending
    group2 = Map.get(group_bys_field, "group_name2")
    [item1, item2] = group2
    assert item1.key == "true" and item1.row_count == 8
    assert item2.key == "false" and item2.row_count == 1
  end

  test "group_by_range" do
    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
            group_bys: [
              group_by_range(
                "group_name",
                "price",
                [
                  # [0, 18)
                  {0, 18},
                  # [18, 50)
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
        ],
        9
      )

    [item1, item2] = Map.get(response.group_bys.by_range, "group_name")
    assert item1.from == 0.0 and item1.to == 18.0
    assert trunc(item2.from) == 18 and trunc(item2.to) == 50
    assert item1.row_count == 2 and item2.row_count == 3

    assert Map.get(item1.sub_aggs.distinct_count, "distinct_price") == 1
    assert Map.get(item2.sub_aggs.distinct_count, "distinct_price") == 3

    [sub_group1_item1, sub_group1_item2] = Map.get(item1.sub_group_bys.by_field, "sorted_by_type")
    assert sub_group1_item1.key == "type2" and sub_group1_item2.key == "type3"

    [sub_group2_item1, sub_group2_item2, sub_group2_item3] =
      Map.get(item2.sub_group_bys.by_field, "sorted_by_type")

    assert sub_group2_item1.key == "type1"
    assert sub_group2_item2.key == "type2"
    assert sub_group2_item3.key == "type3"

    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: [
              type: QueryType.match_all()
            ],
            limit: 0,
            group_bys: [
              group_by_range(
                "group_name",
                "price",
                [
                  {0, 20},
                  {20, 50}
                ],
                sub_aggs: [
                  agg_sum("agg_sum", "number"),
                  agg_max("agg_max", "price")
                ]
              )
            ]
          ]
        ],
        9
      )

    [item1, item2] = Map.get(response.group_bys.by_range, "group_name")
    assert trunc(item1.from) == 0 and trunc(item1.to) == 20
    assert trunc(item2.from) == 20 and trunc(item2.to) == 50
    assert Map.get(item1.sub_aggs.max, "agg_max") == 18.0
    assert Map.get(item2.sub_aggs.max, "agg_max") == 32.05
    assert Map.get(item1.sub_aggs.sum, "agg_sum") == 25.0
    assert Map.get(item2.sub_aggs.sum, "agg_sum") == 115.0
  end

  test "group_by_filter" do
    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
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
        ],
        9
      )

    [item1, item2] = Map.get(response.group_bys.by_filter, "group_name")
    assert item1.row_count == 8
    assert item2.row_count == 4

    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
            group_bys: [
              group_by_filter(
                "group_name",
                [
                  range_query("price", from: 1000),
                  term_query("type", "unknow-type")
                ]
              )
            ]
          ]
        ],
        9
      )

    [item1, item2] = Map.get(response.group_bys.by_filter, "group_name")
    assert item1.row_count == 0 and item2.row_count == 0

    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
            group_bys: [
              group_by_filter(
                "group_name",
                [
                  range_query("price", from: 20, to: 100)
                ],
                sub_aggs: [
                  agg_max("max_price", "price")
                ]
              )
            ]
          ]
        ],
        9
      )

    [item] = Map.get(response.group_bys.by_filter, "group_name")
    assert item.row_count == 5
    # Please notice that the value of min/max/sum/avg aggregation is float.
    assert Map.get(item.sub_aggs.max, "max_price") == 100.0
  end

  test "group by with non-existed field" do
    {:error, response} =
      search(@table_group_by, @index_group_by,
        search_query: [
          query: match_all_query(),
          group_bys: [
            group_by_field("group_name", "non_existed_field"),
            group_by_field("group_name2", "is_actived")
          ]
        ]
      )

    assert response.code == "OTSParameterInvalid"
  end

  test "group by histogram: integer/long" do
    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
            group_bys: [
              # Notice:
              # Since the type of `number` column is integer/long,
              # the type of param `interval` & `field_range` & `missing` must be integer/long as well
              group_by_histogram("group_name", "number", 5, {0, 100}, missing: 0)
            ]
          ]
        ],
        9
      )

    [item1 | _] = Map.get(response.group_bys.by_histogram, "group_name")
    assert item1.key == 0
    assert item1.value == 2
  end

  test "group by histogram: double/float" do
    response =
      assert_search(
        @table_group_by,
        @index_group_by,
        [
          search_query: [
            query: match_all_query(),
            limit: 0,
            group_bys: [
              # Notice:
              # Since the type of `price` column is double/float,
              # the type of param `interval` & `field_range` & `missing` must be double/float as well
              group_by_histogram("group_name", "price", 10.0, {0.0, 100.0}, missing: 0.0)
            ]
          ]
        ],
        9
      )

    [item1 | _] = Map.get(response.group_bys.by_histogram, "group_name")
    assert item1.key == 0.0
    assert item1.value == 2
  end

  test "delete search index" do
    index_name = "tmp_search_index1"

    var_request = %Search.CreateSearchIndexRequest{
      table_name: @table,
      index_name: index_name,
      index_schema: %Search.IndexSchema{
        field_schemas: [
          %Search.FieldSchema{
            field_name: "name"
          }
        ]
      }
    }

    {result, _response} = Client.create_search_index(@instance_key, var_request)
    assert result == :ok
    {result, _response} = delete_search_index(@table, index_name)
    assert result == :ok
  end

  test "delete search index2" do
    index_name = "tmp_search_index2"

    {result, _response} =
      create_search_index(@table, index_name,
        field_schemas: [
          field_schema_keyword("name"),
          field_schema_integer("age")
        ]
      )

    assert result == :ok

    delete_search_index(@table, index_name)
  end

  test "describe search index with text analyzer" do
    {:ok, response} = describe_search_index(@table_text_analyzer, @index_text_analyzer)
    schema = response.schema

    schema.field_schemas
    |> Enum.with_index()
    |> Enum.map(fn {field_schema, index} ->
      cond do
        index == 0 ->
          assert field_schema.field_name == "text_single_word_1"
          assert field_schema.field_type == FieldType.text()

        index == 1 ->
          assert field_schema.field_name == "text_single_word_2"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "single_word"
          assert field_schema.analyzer_parameter.case_sensitive == true
          assert field_schema.analyzer_parameter.delimit_word == true

        index == 2 ->
          assert field_schema.field_name == "text_split_1"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "split"

        index == 3 ->
          assert field_schema.field_name == "text_split_2"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "split"
          assert field_schema.analyzer_parameter.delimiter == ":"

        index == 4 ->
          assert field_schema.field_name == "text_fuzzy"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "fuzzy"
          assert field_schema.analyzer_parameter.min_chars == 2
          assert field_schema.analyzer_parameter.max_chars == 7

        index == 5 ->
          assert field_schema.field_name == "text_fuzzy2"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "fuzzy"

        index == 6 ->
          assert field_schema.field_name == "text_min_word"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "min_word"

        index == 7 ->
          assert field_schema.field_name == "text_max_word"
          assert field_schema.field_type == FieldType.text()
          assert field_schema.analyzer == "max_word"
      end
    end)
  end

  test "search - match query single word default (not case_sensitive)" do
    {:ok, response} =
      search(@table_text_analyzer, @index_text_analyzer,
        search_query: [
          query: [
            type: QueryType.match(),
            field_name: "text_single_word_1",
            text: "tincDdunt" |> String.downcase()
          ]
        ]
      )

    assert response.total_hits == 1
    assert length(response.rows) == 1
  end

  test "search - match query single word default (not delimit_word)" do
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_single_word_1",
              text: "loBortis111" |> String.downcase()
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
  end

  test "search - match query single word case_sensitive" do
    # downcase
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_single_word_2",
              text: "Pulvinar" |> String.downcase()
            ]
          ]
        ],
        0
      )

    assert length(response.rows) == 0

    # origin
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_single_word_2",
              text: "Pulvinar"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
  end

  test "search - match query single word delimit_word" do
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_single_word_2",
              text: "Gravida"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_single_word_2",
              text: "999"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
  end

  test "search - match query fuzzy" do
    # cannot get result when text length < 2 since min_chars = 2
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_fuzzy",
              text: "m"
            ]
          ]
        ],
        0
      )

    assert length(response.rows) == 0

    # can get result when text length = 2
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_fuzzy",
              text: "mc"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    # case insensitive
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_fuzzy",
              text: "mattIs" |> String.downcase()
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1
  end

  test "search - match query min_word" do
    # 适用于中文分词
    # 切分出最少的词, 切分后的词不会有重合
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_min_word",
              text: "梨"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_min_word",
              text: "花茶"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    # 切分出了 "花茶", 就不会再有 "梨花"
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_min_word",
              text: "梨花"
            ]
          ]
        ],
        0
      )

    assert length(response.rows) == 0
  end

  test "search - match query max_word" do
    # 适用于中文分词
    # 切分出最多的词, 切分后的词有可能会有重合
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_max_word",
              text: "花茶"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    # 切分出了 "花茶", 但也还有 "梨花"
    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_max_word",
              text: "梨花"
            ]
          ]
        ],
        1
      )

    assert length(response.rows) == 1

    response =
      assert_search(
        @table_text_analyzer,
        @index_text_analyzer,
        [
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "text_max_word",
              text: "梨"
            ]
          ]
        ],
        0
      )

    assert length(response.rows) == 0
  end

  test "search - funzzy match phrase" do
    assert_search(
      @table_text_analyzer,
      @index_text_analyzer,
      [
        search_query: [
          query: [
            type: QueryType.match_phrase(),
            field_name: "text_fuzzy2",
            text: "调音"
          ]
        ]
      ],
      1
    )

    assert_search(
      @table_text_analyzer,
      @index_text_analyzer,
      [
        search_query: [
          query: [
            type: QueryType.match_phrase(),
            field_name: "text_fuzzy2",
            text: "调 音"
          ]
        ]
      ],
      0
    )

    assert_search(
      @table_text_analyzer,
      @index_text_analyzer,
      [
        search_query: [
          query: [
            type: QueryType.match_phrase(),
            field_name: "text_fuzzy2",
            text: "调音师1024x"
          ]
        ]
      ],
      1
    )

    assert_search(
      @table_text_analyzer,
      @index_text_analyzer,
      [
        search_query: [
          query: [
            type: QueryType.match_phrase(),
            field_name: "text_fuzzy2",
            text: "24x768P.mp4"
          ]
        ]
      ],
      1
    )

    assert_search(
      @table_text_analyzer,
      @index_text_analyzer,
      [
        search_query: [
          query: [
            type: QueryType.match_phrase(),
            field_name: "text_fuzzy2",
            text: "24x7 P.mp4"
          ]
        ]
      ],
      0
    )
  end
end
