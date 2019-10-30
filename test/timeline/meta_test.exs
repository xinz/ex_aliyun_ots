defmodule ExAliyunOts.TimelineMateTest do
  use ExUnit.Case

  use ExAliyunOts.Timeline.Meta,
    instance: EDCEXTestInstance

  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Utils

  alias ExAliyunOts.Const.Search.FieldType

  require FieldType

  describe "meta crud" do
    setup do
      index_schema = %Search.IndexSchema{
        field_schemas: [
          %Search.FieldSchema{
            field_name: "name"
          }
        ]
      }

      table_name = "timeline_meta_test"
      index_name = "timeline_meta_test_index"

      meta =
        [table_name: table_name, index_name: index_name, index_schema: index_schema]
        |> new()
        |> add_field("id", :string)

      :ok = create(meta)

      on_exit(fn ->
        drop(meta)
      end)

      {:ok, table_name: table_name, index_name: index_name}
    end

    test "insert / update / read / search / delete", context do
      table_name = context[:table_name]
      index_name = context[:index_name]

      identifier = [{"id", "1001"}]
      timeline_meta = new(table_name: table_name, index_name: index_name, identifier: identifier)

      assert_raise ExAliyunOts.RuntimeError, fn -> insert(timeline_meta) end

      info = [name: "t1", num: 100, type: "M1"]

      timeline_meta_with_info = change_info(timeline_meta, info)
      {status, _} = insert(timeline_meta_with_info)
      assert status == :ok

      {:ok, response} = read(timeline_meta)
      map = Utils.row_to_map(response.row)
      assert map.name == "t1" and map.num == 100 and map.type == "M1"
      assert map.id == "1001"

      timeline_meta = new(table_name: table_name, index_name: index_name, identifier: identifier, info: [name: "updated_t1", num: 101, type: "M1u"])

      {:ok, _response} = update(timeline_meta)

      {:ok, response} = read(timeline_meta)
      map = Utils.row_to_map(response.row)
      assert map.name == "updated_t1" and map.num == 101 and map.type == "M1u"
      assert map.id == "1001"

      table_name = context[:table_name]
      index_name = context[:index_name]

      {:ok, response} =
        search(
          new(table_name: table_name, index_name: index_name),
          search_query: [
            query: [
              type: QueryType.match,
              field_name: "name",
              text: "updated_t1"
            ],
          ]
        )
      assert response.is_all_succeeded == true

      {:ok, _response} = delete(timeline_meta)

      {:ok, response} = read(timeline_meta)

      assert response.row == nil
      assert Utils.row_to_map(response.row) == %{}
    end

  end
end
