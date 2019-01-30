defmodule ExAliyunOtsTest.CreateTableAndBasicRowOperation do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.PKType
  require PKType

  alias ExAliyunOts.Const.Search.{FieldType, ColumnReturnType, SortOrder}
  require FieldType
  require ColumnReturnType
  require SortOrder

  @instance_name "edc-ex-test"

  test "create table and then delete it" do
    table_name = "test_table"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.string}],
    }
    result = ExAliyunOts.Client.create_table(@instance_name, var_create_table)
    assert result == :ok
  end

  test "create search index" do
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: "test_table",
        index_name: "test_search_index",
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
              #field_type: FieldType.keyword, # using as `keyword` field type by default
            },
            %Search.FieldSchema{
              field_name: "age",
              field_type: FieldType.long
            }
          ]
        }
      }
    result = ExAliyunOts.Client.create_search_index(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "create search index - field type as text" do
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
              field_type: FieldType.text
            },
          ]
        }
      }
    result = ExAliyunOts.Client.create_search_index(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "search - match query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index",
        search_query: %Search.SearchQuery{
          query: %Search.MatchQuery{
            field_name: "age",
            text: "28"
          },
          limit: 1
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"

    {:ok, response} = result

    var_request2 =
      %Search.SearchRequest{
        table_name: "test_table",
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
    result2 = ExAliyunOts.Client.search(@instance_name, var_request2)
    Logger.info "#{inspect result2}"
  end

  test "search - term query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        search_query: %Search.SearchQuery{
        #          query: %Search.TermQuery{
        #            field_name: "name",
        #            term: "zouxin",
        #          },
        #          query: %Search.TermQuery{
        #            field_name: "score",
        #            term: 99.71,
        #          },
        #          query: %Search.TermQuery{
        #            field_name: "is_actived",
        #            term: true,
        #          },
          query: %Search.TermQuery{
            field_name: "age",
            term: 31
          },
          limit: 1
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name", "is_actived"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "search - terms query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        search_query: %Search.SearchQuery{
          query: %Search.TermsQuery{
            field_name: "age",
            terms: [31, 28]
          },
          limit: 3,
          sort: [
            %Search.FieldSort{field_name: "age", order: SortOrder.desc}
          ]
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name", "is_actived", "age"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "search - prefix query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        search_query: %Search.SearchQuery{
          query: %Search.PrefixQuery{
            field_name: "name",
            prefix: "z"
          },
          limit: 3,
          sort: [
            %Search.FieldSort{field_name: "age", order: SortOrder.desc}
          ]
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name", "is_actived", "age"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "search - wildcard query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        search_query: %Search.SearchQuery{
          query: %Search.WildcardQuery{
            field_name: "name",
            value: "z*"
          },
          limit: 3,
          sort: [
            %Search.FieldSort{field_name: "age", order: SortOrder.desc}
          ]
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name", "is_actived", "age"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

  test "search - range query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index2",
        search_query: %Search.SearchQuery{
          query: %Search.RangeQuery{
            field_name: "age",
            from: 25,
            to: 28
          },
          sort: [
            %Search.FieldSort{field_name: "age", order: SortOrder.desc}
          ]
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name", "is_actived", "age"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

end
