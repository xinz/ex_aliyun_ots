defmodule ExAliyunOtsTest.Support.Search do

  require Logger

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.{PKType, RowExistence}
  alias ExAliyunOts.Const.Search.FieldType
  require PKType
  require RowExistence
  require FieldType

  import ExAliyunOts.Search, only: [field_schema_keyword: 1, field_schema_nested: 2]

  def init(instance_key, table, index_names, opts \\ []) do
    initialize(instance_key, table, index_names)

    table_group_by = Keyword.get(opts, :table_group_by) 
    index_group_by = Keyword.get(opts, :index_group_by)
    if table_group_by != nil and index_group_by != nil do
      initialize_group_by(instance_key, table_group_by, index_group_by)
    end

    sleep()
  end

  defp initialize(instance_key, table, index_names) do
    create_table(instance_key, table)
    Process.sleep(5000)
    create_index(instance_key, table, index_names)

    insert_test_data(instance_key, table)
  end

  defp initialize_group_by(instance_key, table, index_name) do
    create_table_for_group_by(instance_key, table)
    Process.sleep(5000)
    create_search_index_for_gourp_by(instance_key, table, index_name)

    insert_group_by_test_data(instance_key, table)
  end

  defp sleep() do
    sleep = 30_000
    Logger.info "waiting #{sleep} ms for indexing..."
    Process.sleep(sleep)
  end

  def clean(instance_key, table, useless_index_names) do
    Enum.map(useless_index_names, fn(index_name) ->
      var_request = %Search.DeleteSearchIndexRequest{
        table_name: table,
        index_name: index_name
      }
      {:ok, _response} = Client.delete_search_index(instance_key, var_request)
    end)
    ExAliyunOts.Client.delete_table(instance_key, table)
    Logger.info "clean search_indexes and delete `#{table}` table"
  end

  def clean_group_by(instance_key, table, index_name) do
    var_request = %Search.DeleteSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }
    {:ok, _response} = Client.delete_search_index(instance_key, var_request)
    ExAliyunOts.Client.delete_table(instance_key, table)
    Logger.info "clean search_indexes and delete `#{table}` table"
  end

  defp create_table(instance_key, table) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: [{"partition_key", PKType.string}],
    }
    Client.create_table(instance_key, var_create_table)

    sleep = 5_000
    Logger.info "initialized table, waiting for #{sleep} ms"
    Process.sleep(sleep)
  end

  defp create_table_for_group_by(instance_key, table) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: [{"partition_key", PKType.string}]
    }
    Client.create_table(instance_key, var_create_table)

    sleep = 5_000
    Logger.info "initialized group_by table, waiting for #{sleep} ms"
    Process.sleep(sleep)
  end

  defp create_index(instance_key, table, [index1, index2]) do
    create_search_index(instance_key, table, index1)
    create_search_index2(instance_key, table, index2)
    Process.sleep(5_000)
  end

  defp insert_test_data(instance_key, table) do

    data = [
      %{id: "a1", class: "class1", name: "name_a1", age: 20, score: 99.71, is_actived: true, tags: Jason.encode!(["1", "2", "3"]), place: 10, values: Jason.encode!([1, 2, 3])},
      %{id: "a2", class: "class1", name: "name_a2", age: 28, score: 100, is_actived: false, tags: Jason.encode!(["2", "3"]), values: Jason.encode!([4, 5, 10])},
      %{id: "a3", class: "class2", name: "name_a3", age: 32, score: 66.78, is_actived: true, tags: Jason.encode!(["4", "1"]), place: 4, values: Jason.encode!([-3, 6, 8])},
      %{id: "a4", class: "class3", name: "name_a4", age: 24, score: 41.01, is_actived: true, tags: Jason.encode!(["4"]), place: 20, values: Jason.encode!([4, 7, 10])},
      %{id: "a5", class: "class2", name: "name_a5", age: 26, score: 89, is_actived: true, place: 3},
      %{id: "a6", class: "class4", name: "name_a6", age: 27, score: 79.99, is_actived: false},
      %{id: "a7", class: "class1", name: "name_a7", age: 28, score: 100, is_actived: true},
      %{id: "a8", class: "class8", name: "name_a8", age: 22, score: 88.61, is_actived: true, place: 8},
      %{id: "b9", class: "class8", name: "name_b9", age: 21, score: 99, is_actived: false, comment: "comment"},
    ]

    Enum.map(data, fn(item) -> 
      attribute_columns = ExAliyunOts.Utils.attrs_to_row(item)
      var_put_row =
        %Var.PutRow{
          table_name: table,
          primary_keys: [{"partition_key", item.id}],
          attribute_columns: attribute_columns,
          condition: %Var.Condition{
            row_existence: RowExistence.expect_not_exist
          }
        }

      {:ok, _result} = Client.put_row(instance_key, var_put_row)
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
      {:ok, _result} = Client.put_row(instance_key, var_put_row)
    end)

  end

  defp insert_group_by_test_data(instance_key, table) do
    data = [
      %{id: "1", type: "type1", price: 100.0, is_actived: true, number: 10, name: "product-001"},
      %{id: "2", type: "type1", price: 32.05, is_actived: true, number: 100, name: "product-002"},
      %{id: "3", type: "type1", price: 56.15, is_actived: true, number: 3, name: "product-003"},
      %{id: "4", type: "type2", price: 89.99, is_actived: true, number: 32, name: "product-004"},
      %{id: "5", type: "type2", price: 9.99, is_actived: false, number: 5, name: "product-005"},
      %{id: "6", type: "type2", price: 28.99, is_actived: true, number: 15, name: "product-006"},
      %{id: "7", type: "type2", price: 128.99, is_actived: true, number: 1, name: "product-007"},
      %{id: "8", type: "type3", price: 18.0, is_actived: true, number: 10, name: "product-008"},
      %{id: "9", type: "type3", price: 9.99, is_actived: true, number: 10, name: "product-009"},
    ]

    Enum.map(data, fn(item) ->
      attribute_columns = ExAliyunOts.Utils.attrs_to_row(item)
      row =
        %Var.PutRow{
          table_name: table,
          primary_keys: [{"partition_key", item.id}],
          attribute_columns: attribute_columns,
          condition: %Var.Condition{
            row_existence: RowExistence.expect_not_exist
          }
        }

      {:ok, _result} = Client.put_row(instance_key, row)
    end)
  end

  defp create_search_index(instance_key, table, index_name) do
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
            },
            %Search.FieldSchema{
              field_name: "comment"
            },
            %Search.FieldSchema{
              field_name: "tags",
              is_array: true
            },
            %Search.FieldSchema{
              field_name: "class"
            },
            %Search.FieldSchema{
              field_name: "place",
              field_type: FieldType.long
            },
            %Search.FieldSchema{
              field_name: "values",
              is_array: true,
              field_type: FieldType.long
            },
          ]
        }
      }
    result = Client.create_search_index(instance_key, var_request)
    Logger.info "create_search_index: #{inspect result}"
  end

  defp create_search_index_for_gourp_by(instance_key, table, index_name) do
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "type"
            },
            %Search.FieldSchema{
              field_name: "price",
              field_type: FieldType.double
            },
            %Search.FieldSchema{
              field_name: "is_actived",
              field_type: FieldType.boolean
            },
            %Search.FieldSchema{
              field_name: "number",
              field_type: FieldType.long
            },
            %Search.FieldSchema{
              field_name: "name"
            }
          ]
        }
      }
    result = Client.create_search_index(instance_key, var_request)
    Logger.info "create_search_index for group_by test: #{inspect result}"
  end

  defp create_search_index2(instance_key, table, index_name) do

    #
    # These two use cases are the same thing.
    #
    #sub_nested1 = %Search.FieldSchema{
    #  field_name: "header",
    #  field_type: FieldType.keyword,
    #}
    #sub_nested2 = %Search.FieldSchema{
    #  field_name: "body",
    #  field_type: FieldType.keyword,
    #}
    #var_request =
    #  %Search.CreateSearchIndexRequest{
    #    table_name: table,
    #    index_name: index_name,
    #    index_schema: %Search.IndexSchema{
    #      field_schemas: [
    #        %Search.FieldSchema{
    #          field_name: "content",
    #          field_type: FieldType.nested,
    #          field_schemas: [
    #            sub_nested1,
    #            sub_nested2
    #          ],
    #        }
    #      ]
    #    }
    #  }
    #result = Client.create_search_index(instance_key, var_request)
    #
    result =
      ExAliyunOts.create_search_index(instance_key, table, index_name,
        field_schemas: [
          field_schema_nested("content",
            field_schemas: [
              field_schema_keyword("header"),
              field_schema_keyword("body"),
            ]
          )
        ]
      )
    Logger.info "create_search_index2: #{inspect result}"
  end

end
