defmodule ExAliyunOtsTest.CreateTableAndBasicRowOperation do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.PKType
  require PKType

  alias ExAliyunOts.Const.Search.{FieldType, ColumnReturnType}
  require FieldType
  require ColumnReturnType

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

  test "search - match query" do
    var_request =
      %Search.SearchRequest{
        table_name: "test_table",
        index_name: "test_search_index",
        search_query: %Search.SearchQuery{
          query: %Search.MatchQuery{
            field_name: "age",
            text: "28"
          }
        },
        columns_to_get: %Search.ColumnsToGet{
          return_type: ColumnReturnType.specified,
          column_names: ["class", "name"]
        }
      }
    result = ExAliyunOts.Client.search(@instance_name, var_request)
    Logger.info "#{inspect result}"
  end

end
