defmodule ExAliyunOtsTest.Support.SearchGeo do
  import ExAliyunOts.DSL, only: [condition: 1]
  require Logger
  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.PKType
  alias ExAliyunOts.Const.Search.FieldType
  require PKType
  require FieldType

  import ExAliyunOts.Search,
    only: [field_schema_keyword: 1, field_schema_geo_point: 1, field_schema_integer: 1]

  def init(instance_key, table, index_name) do
    create_table(instance_key, table)
    insert_test_data(instance_key, table)
    create_index(instance_key, table, index_name)

    sleep()
  end

  def clean(instance_key, table, index_name) do
    var_request = %Search.DeleteSearchIndexRequest{
      table_name: table,
      index_name: index_name
    }

    {:ok, _response} = Client.delete_search_index(instance_key, var_request)

    ExAliyunOts.Client.delete_table(instance_key, table)
    Logger.info("clean search_indexes and delete `#{table}` table")
  end

  defp create_table(instance_key, table) do
    var_create_table = %Var.CreateTable{
      table_name: table,
      primary_keys: [{"id", PKType.string()}]
    }

    :ok = Client.create_table(instance_key, var_create_table)

    Logger.info("successfully create table: #{table}")
  end

  defp create_index(instance_key, table, index_name) do
    result =
      ExAliyunOts.create_search_index(instance_key, table, index_name,
        field_schemas: [
          field_schema_keyword("name"),
          field_schema_geo_point("location"),
          field_schema_integer("value")
        ]
      )

    Logger.info("create_search_index for GEO test: #{inspect(result)}")
  end

  defp insert_test_data(instance_key, table) do
    data = [
      %{name: "a1", location: "0,0", value: 10},
      %{name: "a2", location: "10,10", value: 10},
      %{name: "a3", location: "13.41,30.41", value: 1},
      %{name: "a4", location: "5.14,5.21", value: 9},
      %{name: "a5", location: "4.31,2.91", value: 3},
      %{name: "a6", location: "0,-10", value: 4},
      %{name: "a7", location: "10,-4", value: 1},
      %{name: "a8", location: "10,20", value: 3},
      %{name: "a9", location: "10.1,30.45", value: 6},
      %{name: "a10", location: "3,10", value: 9}
    ]

    Enum.map(data, fn item ->
      attribute_columns = ExAliyunOts.Utils.attrs_to_row(item)

      var_put_row = %Var.PutRow{
        table_name: table,
        primary_keys: [{"id", item.name}],
        attribute_columns: attribute_columns,
        condition: condition(:ignore)
      }

      {:ok, _result} = Client.put_row(instance_key, var_put_row)
    end)
  end

  defp sleep() do
    sleep = 50_000
    Logger.info("waiting #{sleep} ms for indexing...")
    Process.sleep(sleep)
  end
end
