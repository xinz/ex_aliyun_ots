defmodule ExAliyunOtsTest.Search do
  use ExUnit.Case
  require Logger
  import ExAliyunOts.Search
  alias ExAliyunOts.{Client, Var.Search}
  alias ExAliyunOtsTest.Support.Search, as: TestSupportSearch
  alias ExAliyunOts.Const.Search.{FieldType, ColumnReturnType, SortOrder}
  require FieldType
  require ColumnReturnType
  require SortOrder
  import ExAliyunOts.SearchTestHelper, only: [assert_search_request: 3]

  @instance_key EDCEXTestInstance
  @table "test_search"
  @indexes ["test_search_index", "test_search_index2"]

  setup_all do
    Application.ensure_all_started(:ex_aliyun_ots)
    TestSupportSearch.clean(@instance_key, @table, @indexes)

    TestSupportSearch.init(@instance_key, @table, @indexes)

    on_exit(fn ->
      TestSupportSearch.clean(@instance_key, @table, @indexes)
    end)
  end

  test "list search index" do
    result = Client.list_search_index(@instance_key, @table)

    case result do
      {:ok, response} ->
        assert length(response.indices) == 2

      error ->
        Logger.error("list_search_index occur error: #{inspect(error)}")
    end
  end

  test "describe search index" do
    var_request = %Search.DescribeSearchIndexRequest{
      table_name: @table,
      index_name: "test_search_index2"
    }

    result = Client.describe_search_index(@instance_key, var_request)

    case result do
      {:ok, response} ->
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

      error ->
        Logger.error("describe_search_index occur error: #{inspect(error)}")
    end
  end

  test "search - match query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.MatchQuery{
          field_name: "age",
          text: "28"
        },
        limit: 1
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 2)

    assert response.total_hits == 2

    {_pk, attrs} = List.first(response.rows)

    Enum.map(attrs, fn {field_name, field_value, _timestamp} ->
      cond do
        field_name == "name" ->
          assert field_value == "name_a2"

        field_name == "class" ->
          assert field_value == "class1"

        field_name == "age" ->
          assert field_value == 28
      end
    end)

    var_request2 = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.MatchQuery{
          field_name: "age",
          text: "28"
        },
        limit: 1,
        token: response.next_token
      }
    }

    {:ok, response2} = assert_search_request(@instance_key, var_request2, 2)

    {_pk, attrs2} = List.first(response2.rows)
    assert length(attrs2) > length(attrs)

    Enum.map(attrs2, fn {field_name, field_value, _timestamp} ->
      cond do
        field_name == "name" ->
          assert field_value == "name_a7"

        field_name == "class" ->
          assert field_value == "class1"

        field_name == "age" ->
          assert field_value == 28

        true ->
          :ignore
      end
    end)
  end

  test "search - term query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.TermQuery{
          field_name: "name",
          term: "name_a1"
        },
        #          query: %Search.TermQuery{
        #            field_name: "score",
        #            term: 99.71,
        #          },
        #          query: %Search.TermQuery{
        #            field_name: "is_actived",
        #            term: true,
        #          },
        #          query: %Search.TermQuery{
        #            field_name: "age",
        #            term: 31
        #          },
        limit: 1
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.all()
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 1)
    assert length(response.rows) == 1
  end

  test "search - terms query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.TermsQuery{
          field_name: "age",
          terms: [31, 28]
        },
        limit: 3,
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "is_actived", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 2)

    rows = response.rows
    assert length(rows) == 2

    Enum.map(rows, fn {[{"partition_key", id}], attrs} ->
      case id do
        "a2" ->
          [
            {"age", age, _},
            {"class", class, _},
            {"is_actived", is_actived, _},
            {"name", name, _}
          ] = attrs

          assert age == 28 and class == "class1" and is_actived == false and name == "name_a2"

        "a7" ->
          [
            {"age", age, _},
            {"class", class, _},
            {"is_actived", is_actived, _},
            {"name", name, _}
          ] = attrs

          assert age == 28 and class == "class1" and is_actived == true and name == "name_a7"

        _ ->
          :ignore
      end
    end)

  end

  test "search - prefix query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.PrefixQuery{
          field_name: "name",
          prefix: "name"
        },
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "is_actived", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 9)
    assert length(response.rows) == 9
  end

  test "search - wildcard query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.WildcardQuery{
          field_name: "name",
          value: "name_*"
        },
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "is_actived", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 9)
    assert length(response.rows) == 9
  end

  test "search - range query" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.RangeQuery{
          field_name: "age",
          from: 25,
          to: 28
        },
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "is_actived", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 4)

    assert length(response.rows) == 4
    assert response.total_hits == 4
  end

  test "search - range_query/1 & range_query/2" do
    range_query = range_query("score", from: 1, to: 10)
    assert ^range_query = range_query("score", 1..10)
    assert ^range_query = range_query(1 <= "score" and "score" <= 10)
    assert range_query("score", from: 1) == range_query("score" >= 1)
    assert range_query("score", from: 1, include_lower: false) == range_query("score" > 1)
    assert range_query("score", to: 10) == range_query("score" <= 10)
    assert range_query("score", to: 10, include_upper: false) == range_query("score" < 10)
  end

  test "search - bool query with must/must_not" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.BoolQuery{
          must: [
            %Search.RangeQuery{
              field_name: "score",
              from: 60,
              to: 100
            }
          ],
          must_not: [
            %Search.TermQuery{
              field_name: "age",
              term: "27"
            }
          ]
        },
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.all()
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 3)
    assert length(response.rows) == 3
  end

  test "search - bool query with should" do
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.BoolQuery{
          should: [
            %Search.TermQuery{
              field_name: "age",
              term: "22"
            },
            %Search.TermQuery{
              field_name: "age",
              term: "28"
            }
          ],
          # if not explicitly set this value and `should` list is not empty, will set this value as 1 by default
          minimum_should_match: 1
        },
        sort: [
          %Search.FieldSort{field_name: "age", order: SortOrder.desc()}
        ]
      },
      columns_to_get: %Search.ColumnsToGet{
        return_type: ColumnReturnType.specified(),
        column_names: ["class", "name", "is_actived", "age"]
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 3)
    assert length(response.rows) == 3
  end

  test "search - nested query" do
    # Please ensure the column `content` store value as a json array in string format, for example: "[{}, {}]" (the square bracket "[]" is required)
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index2",
      search_query: %Search.SearchQuery{
        query: %Search.NestedQuery{
          path: "content",
          query: %Search.TermQuery{
            field_name: "content.header",
            term: "header1"
          }
        }
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 1)

    assert length(response.rows) == 1
    {[{"partition_key", id}], [{"content", value, _ts}]} = List.first(response.rows)
    assert id == "a9"
    assert value == "[{\"body\":\"body1\",\"header\":\"header1\"}]"
  end

  test "search - exists query" do
    # search with a fake field (it's non-index attribute column)
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.ExistsQuery{field_name: "fake_nonindex_field"}
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 0)
    assert response.rows == []
    assert response.is_all_succeeded == true

    field_name_comment = "comment"

    # search exists_query for `comment` field
    search_query = %Search.SearchQuery{
      query: %Search.ExistsQuery{field_name: field_name_comment}
    }

    var_request = Map.put(var_request, :search_query, search_query)
    {:ok, response} = assert_search_request(@instance_key, var_request, 1)
    assert length(response.rows) >= 1

    # seach exists_query for `comment` field as nil column
    var_request = %Search.SearchRequest{
      table_name: @table,
      index_name: "test_search_index",
      search_query: %Search.SearchQuery{
        query: %Search.BoolQuery{
          must_not: [
            %Search.ExistsQuery{field_name: field_name_comment}
          ]
        },
        limit: 100
      }
    }

    {:ok, response} = assert_search_request(@instance_key, var_request, 10)
    assert response.next_token == nil
    assert length(response.rows) == 10
  end
end
