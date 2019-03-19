defmodule ExAliyunOtsTest.Support.Search do

  require Logger

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.{PKType, RowExistence}
  alias ExAliyunOts.Const.Search.FieldType
  require PKType
  require RowExistence
  require FieldType

  def initialize(instance_name, table, index_names) do
    create_table(instance_name, table)

    create_index(instance_name, table, index_names)

    inseart_test_data(instance_name, table)
  end

  def clean(instance_name, table, useless_index_names) do
    Enum.map(useless_index_names, fn(index_name) ->
      var_request = %Search.DeleteSearchIndexRequest{
        table_name: table,
        index_name: index_name
      }
      {:ok, _response} = Client.delete_search_index(instance_name, var_request)
    end)
    ExAliyunOts.Client.delete_table(instance_name, table)
    Logger.info "clean search_indexes and delete table"
  end

  defp create_table(instance_name, table) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: [{"partition_key", PKType.string}],
    }
    :ok = Client.create_table(instance_name, var_create_table)
    Logger.info "initialized table"
    Process.sleep(5_000)
  end

  defp create_index(instance_name, table, [index1, index2]) do
    create_search_index(instance_name, table, index1)
    create_search_index2(instance_name, table, index2)
    Process.sleep(5_000)
  end

  defp inseart_test_data(instance_name, table) do

    data = [
      %{id: "a1", class: "class1", name: "name_a1", age: 20, score: 99.71, is_actived: true},
      %{id: "a2", class: "class1", name: "name_a2", age: 28, score: 100, is_actived: false},
      %{id: "a3", class: "class2", name: "name_a3", age: 32, score: 66.78, is_actived: true},
      %{id: "a4", class: "class3", name: "name_a4", age: 24, score: 41.01, is_actived: true},
      %{id: "a5", class: "class2", name: "name_a5", age: 26, score: 89, is_actived: true},
      %{id: "a6", class: "class4", name: "name_a6", age: 27, score: 79.99, is_actived: false},
      %{id: "a7", class: "class1", name: "name_a7", age: 28, score: 100, is_actived: true},
      %{id: "a8", class: "class8", name: "name_a8", age: 22, score: 88.61, is_actived: true},
    ]

    Enum.map(data, fn(item) -> 
      var_put_row = %Var.PutRow{
        table_name: table,
        primary_keys: [{"partition_key", item.id}],
        attribute_columns: [{"class", item.class}, {"age", item.age}, {"name", item.name}, {"is_actived", item.is_actived}, {"score", item.score}],
        condition: %Var.Condition{
          row_existence: RowExistence.expect_not_exist
        }
      }
      {:ok, _result} = Client.put_row(instance_name, var_put_row)
    end)

    data = [
      %{id: "a9", content: "[{\"body\":\"body1\",\"header\":\"header1\"}]"},
      %{id: "a10", content: "[{\"body\":\"body2\",\"header\":\"header2\"}]"},
    ]
    Enum.map(data, fn(item) ->
      var_put_row = %Var.PutRow{
        table_name: table,
        primary_keys: [{"partition_key", item.id}],
        attribute_columns: [{"content", item.content}],
        condition: %Var.Condition{
          row_existence: RowExistence.expect_not_exist
        }
      }
      {:ok, _result} = Client.put_row(instance_name, var_put_row)
    end)

    Logger.info "waiting for indexing..."
    Process.sleep(15_000)
  end

  defp create_search_index(instance_name, table, index_name) do
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
              #field_type: FieldType.keyword, # using as `keyword` field type by default
            },
            %Search.FieldSchema{
              field_name: "age",
              field_type: FieldType.long
            },
            %Search.FieldSchema{
              field_name: "score",
              field_type: FieldType.double
            },
            %Search.FieldSchema{
              field_name: "is_actived",
              field_type: FieldType.boolean
            }
          ]
        }
      }
    result = Client.create_search_index(instance_name, var_request)
    Logger.info "create_search_index: #{inspect result}"
  end

  defp create_search_index2(instance_name, table, index_name) do
    sub_nested1 = %Search.FieldSchema{
      field_name: "header",
      field_type: FieldType.keyword,
    }
    sub_nested2 = %Search.FieldSchema{
      field_name: "body",
      field_type: FieldType.keyword,
    }
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "content",
              field_type: FieldType.nested,
              field_schemas: [
                sub_nested1,
                sub_nested2
              ],
            }
          ]
        }
      }
    result = Client.create_search_index(instance_name, var_request)
    Logger.info "create_search_index2: #{inspect result}"
  end

end
