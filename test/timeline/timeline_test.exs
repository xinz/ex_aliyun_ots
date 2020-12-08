defmodule ExAliyunOts.TimelineTest do
  use ExUnit.Case
  use ExAliyunOts.Timeline, instance: EDCEXTestInstance
  require ExAliyunOts.Const.Search.FieldType, as: FieldType
  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.{Timeline, Utils}
  alias ExAliyunOts.Timeline.{Entry, BatchWrite}

  test "new timeline" do
    t1 = new()
    assert t1.instance == EDCEXTestInstance
    assert t1.table_name == nil and t1.index_name == nil

    t2 = new(table_name: "tabname2")
    assert t2.table_name == "tabname2" and t2.index_name == nil

    t3 = new(table_name: "tabname3", index_name: "index_name3")
    assert t3.table_name == "tabname3" and t3.index_name == "index_name3"
  end

  test "change_seq_id" do
    t = new(table_name: "table_name", index_name: "index_name")
    t = change_seq_id(t, :manual, "updated_seq_id_col")
    assert t.seq_id_generation == :manual and t.seq_id_col_name == "updated_seq_id_col"

    t = change_seq_id(t, :auto, "updated_seq_id_col2")
    assert t.seq_id_generation == :auto and t.seq_id_col_name == "updated_seq_id_col2"

    assert_raise ExAliyunOts.RuntimeError, ~r/Fail to change sequence_id/, fn ->
      change_seq_id(t, :notsupported)
    end
  end

  test "add field" do
    t =
      new(table_name: "table_name", index_name: "index_name")
      |> add_field("field_name1", :string)
      |> add_field("field_name2", :integer)
      |> add_field("field_name3", :binary)

    assert length(t.fields) == 3

    assert_raise ExAliyunOts.RuntimeError, ~r/Allow up to 3 fields to be added/, fn ->
      new(table_name: "table_name", index_name: "index_name")
      |> add_field("field_name1", :string)
      |> add_field("field_name2", :integer)
      |> add_field("field_name3", :binary)
      |> add_field("field_name4", :string)
    end

    assert_raise ExAliyunOts.RuntimeError, ~r/Add an invalid field/, fn ->
      new(table_name: "table_name")
      |> add_field("field_name1", true)
    end
  end

  test "create fail case" do
    # field(s) and index_schema are required
    t = new(table_name: "table_name", index_name: "index_name")

    assert_raise ExAliyunOts.RuntimeError, ~r/Invalid fields size as 0/, fn ->
      create(t)
    end

    # index_schema is required
    t = add_field(t, "field_name1", :string)

    assert_raise ExAliyunOts.RuntimeError, ~r/Fail to create with invalid timeline/, fn ->
      create(t)
    end
  end

  test "crate and drop timeline" do
    index_schema = %Search.IndexSchema{
      field_schemas: [
        %Search.FieldSchema{
          field_name: "name"
        },
        %Search.FieldSchema{
          field_name: "created_at",
          field_type: FieldType.long()
        },
        %Search.FieldSchema{
          field_name: "is_actived",
          field_type: FieldType.boolean()
        }
      ]
    }

    t =
      [
        table_name: "timeline_test1",
        index_name: "timeline_test1_index",
        index_schema: index_schema
      ]
      |> new()
      |> add_field("id", :string)

    create_result = create(t)
    assert create_result == :ok

    drop_result = drop(t)
    assert drop_result == :ok

    t2 =
      [
        table_name: "timeline_test2",
        index_name: "timeline_test2_index",
        index_schema: index_schema
      ]
      |> new()
      |> change_seq_id(:manual, "custom_seq_id")

    assert_raise ExAliyunOts.RuntimeError, ~r/Invalid fields size as 0/, fn ->
      create(t2)
    end

    t2 = add_field(t2, "id2", :string)
    create_result2 = create(t2)
    assert create_result2 == :ok

    drop_result2 = drop(t2)
    assert drop_result2 == :ok
  end

  test "attrs_to_row" do
    message = [name: "name1", age: 20]
    row = Utils.attrs_to_row(message)
    assert row == [{"name", "name1"}, {"age", 20}]

    message = [{"name", "name1"}, {"age", 20}]
    row = Utils.attrs_to_row(message)
    assert row == message

    message = %{"name" => "name1", "age" => 20}
    row = Utils.attrs_to_row(message)
    assert row == [{"age", 20}, {"name", "name1"}]

    message = %{name: "name1", age: 20}
    row = Utils.attrs_to_row(message)
    assert row == [{"age", 20}, {"name", "name1"}]
  end

  test "store" do
    index_schema = %Search.IndexSchema{
      field_schemas: [
        %Search.FieldSchema{
          field_name: "name"
        },
        %Search.FieldSchema{
          field_name: "created_at",
          field_type: FieldType.long()
        },
        %Search.FieldSchema{
          field_name: "is_actived",
          field_type: FieldType.boolean()
        }
      ]
    }

    table_name = "timeline_test1"
    index_name = "timeline_test1_index"

    t =
      [table_name: table_name, index_name: index_name, index_schema: index_schema]
      |> new()
      |> add_field("id", :string)

    table_name2 = "timeline_test2"
    index_name2 = "timeline_test2_index"

    t2 =
      [
        table_name: table_name2,
        index_name: index_name2,
        index_schema: index_schema,
        seq_id_generation: :manual
      ]
      |> new()
      |> add_field("id", :string)

    create_result = create(t)
    assert create_result == :ok

    create_result2 = create(t2)
    assert create_result2 == :ok

    id_value = "1"
    timeline = new(table_name: table_name, identifier: [{"id", id_value}])
    entry = %Entry{message: [name: "test name", created_at: 1000, is_actived: true]}

    {store_status, response} = store(timeline, entry)

    assert store_status == :ok
    {pks, nil} = response.row
    [{"id", value}, {"sequence_id", value2}] = pks
    assert value == id_value and is_integer(value2)

    manual_seq_id = Timeline.generate_sequence_id()

    timeline2 =
      new(table_name: table_name2, identifier: [{"id", id_value}], seq_id_generation: :manual)

    entry = %Entry{
      message: [name: "test name2", created_at: 999, is_actived: false],
      sequence_id: manual_seq_id
    }

    {store_status2, response2} = store(timeline2, entry)

    assert store_status2 == :ok
    {pks, nil} = response2.row
    [{"id", value}, {"sequence_id", value2}] = pks
    assert value == id_value and value2 == manual_seq_id

    :ok = drop(t)
    :ok = drop(t2)
  end

  test "batch_store" do
    index_schema = %Search.IndexSchema{
      field_schemas: [
        %Search.FieldSchema{
          field_name: "name"
        },
        %Search.FieldSchema{
          field_name: "created_at",
          field_type: FieldType.long()
        },
        %Search.FieldSchema{
          field_name: "is_actived",
          field_type: FieldType.boolean()
        }
      ]
    }

    table_name = "timeline_test1"
    index_name = "timeline_test1_index"

    t =
      [table_name: table_name, index_name: index_name, index_schema: index_schema]
      |> new()
      |> add_field("id", :string)

    table_name2 = "timeline_test2"
    index_name2 = "timeline_test2_index"

    t2 =
      [
        table_name: table_name2,
        index_name: index_name2,
        index_schema: index_schema,
        seq_id_generation: :manual
      ]
      |> new()
      |> add_field("id", :string)

    create_result = create(t)
    assert create_result == :ok

    create_result2 = create(t2)
    assert create_result2 == :ok

    timeline1 = new(table_name: table_name, identifier: [{"id", "1"}])

    timeline2 =
      new(table_name: table_name2, identifier: [{"id", "100"}], seq_id_generation: :manual)

    writes = [
      %BatchWrite{
        timeline: timeline1,
        entry: %Entry{message: [name: "test name", created_at: 1, is_actived: true]}
      },
      %BatchWrite{
        timeline: timeline2,
        entry: %Entry{
          message: [name: "test name2", created_at: 2, is_actived: false],
          sequence_id: Timeline.generate_sequence_id()
        }
      }
    ]

    {batch_store_status, _response} = batch_store(writes)

    assert batch_store_status == :ok

    drop(t)
    drop(t2)
  end

  describe "scan" do
    setup do
      index_schema = %Search.IndexSchema{
        field_schemas: [
          %Search.FieldSchema{
            field_name: "name"
          }
        ]
      }

      table_name = "timeline_scan_test"
      index_name = "timeline_scan_test_index"

      t =
        [table_name: table_name, index_name: index_name, index_schema: index_schema]
        |> new()
        |> add_field("id", :string)

      :ok = create(t)

      on_exit(fn ->
        drop(t)
      end)

      :ok
    end

    test "scan" do
      table_name = "timeline_scan_test"

      timeline = new(table_name: table_name, identifier: [{"id", "100"}])
      entry1 = %Entry{message: [name: "test1", attr2: "attr2_1", is_enable: true]}
      entry2 = %Entry{message: [name: "test2", attr2: "attr2_2", is_enable: false]}
      entry3 = %Entry{message: [name: "test3", attr2: "attr2_3", is_enable: false]}
      entry4 = %Entry{message: [name: "test4", attr2: "attr2_4", is_enable: true]}

      {:ok, response1} = store(timeline, entry1)
      {[{"id", "100"}, {"sequence_id", seq1}], nil} = response1.row
      {:ok, response2} = store(timeline, entry2)
      {[{"id", "100"}, {"sequence_id", seq2}], nil} = response2.row
      {:ok, response3} = store(timeline, entry3)
      {[{"id", "100"}, {"sequence_id", seq3}], nil} = response3.row
      {:ok, response4} = store(timeline, entry4)
      {[{"id", "100"}, {"sequence_id", seq4}], nil} = response4.row

      ## forward
      #
      {:ok, response} = scan_forward(timeline, seq2, :max)

      assert length(response.rows) == 3
      assert response.next_start_primary_key == nil

      [row1, row2, _row3] = response.rows

      {[{"id", "100"}, {"sequence_id", ^seq2}], row1_attrs} = row1
      [{"attr2", val1, _}, {"is_enable", _, _}, {"name", val2, _}] = row1_attrs
      assert val1 == "attr2_2" and val2 == "test2"

      {[{"id", "100"}, {"sequence_id", ^seq3}], row2_attrs} = row2
      [{"attr2", val1, _}, {"is_enable", _, _}, {"name", val2, _}] = row2_attrs
      assert val1 == "attr2_3" and val2 == "test3"

      {:ok, response} = scan_forward(timeline, :min, seq2, columns_to_get: ["name"])

      [row] = response.rows

      {[{"id", "100"}, {"sequence_id", ^seq1}], row_attrs} = row
      [{"name", val, _}] = row_attrs
      assert val == "test1"

      assert_raise ExAliyunOts.RuntimeError, ~r/Fail to scan forward timeline/, fn ->
        scan_forward(timeline, seq2, seq1)
      end

      {:ok, response} = scan_forward(timeline, seq1, seq4, filter: filter("is_enable" == true))

      assert length(response.rows) == 1

      [row] = response.rows

      {[{"id", "100"}, {"sequence_id", ^seq1}], row_attrs} = row
      [{"attr2", val1, _}, {"is_enable", true, _}, {"name", val2, _}] = row_attrs
      assert val1 == "attr2_1" and val2 == "test1"

      {:ok, response} = scan_forward(timeline, seq1, seq4, limit: 1)

      next_start_primary_key = response.next_start_primary_key

      assert next_start_primary_key != nil

      {:ok, response} =
        scan_forward(timeline, seq1, seq4, next_start_primary_key: next_start_primary_key)

      assert length(response.rows) == 2 and response.next_start_primary_key == nil

      ## backward
      #
      {:ok, response} = scan_backward(timeline, seq2, :min)
      assert length(response.rows) == 2
      assert response.next_start_primary_key == nil

      [row2, row1] = response.rows
      {[{"id", "100"}, {"sequence_id", ^seq2}], _attrs} = row2
      {[{"id", "100"}, {"sequence_id", ^seq1}], _attrs} = row1

      {:ok, response} = scan_backward(timeline, :max, seq2)
      assert length(response.rows) == 2
      assert response.next_start_primary_key == nil

      [row4, row3] = response.rows
      {[{"id", "100"}, {"sequence_id", ^seq4}], _attrs} = row4
      {[{"id", "100"}, {"sequence_id", ^seq3}], _attrs} = row3

      assert_raise ExAliyunOts.RuntimeError, ~r/Fail to scan backward timeline/, fn ->
        scan_backward(timeline, seq1, seq2)
      end

      {:ok, response} = scan_backward(timeline, seq4, seq1, limit: 1)

      next_start_primary_key = response.next_start_primary_key

      assert next_start_primary_key != nil

      {:ok, response} =
        scan_backward(timeline, seq4, seq1, next_start_primary_key: next_start_primary_key)

      assert length(response.rows) == 2 and response.next_start_primary_key == nil
    end
  end

  describe "timeline CRUD operations" do
    setup do
      index_schema = %Search.IndexSchema{
        field_schemas: [
          %Search.FieldSchema{
            field_name: "name"
          }
        ]
      }

      table_name = "timeline_update_test"
      index_name = "timeline_update_test_index"

      t =
        [table_name: table_name, index_name: index_name, index_schema: index_schema]
        |> new()
        |> add_field("id", :string)

      :ok = create(t)

      on_exit(fn ->
        drop(t)
      end)

      {:ok, table_name: table_name, index_name: index_name}
    end

    test "store / update / get / search / delete", context do
      table_name = context[:table_name]
      index_name = context[:index_name]

      timeline = new(table_name: table_name, identifier: [{"id", "1"}])

      entry1 = %Entry{message: [name: "test1", type: "t1", num: 10]}

      {:ok, response} = store(timeline, entry1)
      {[{"id", "1"}, {"sequence_id", sequence_id}], nil} = response.row

      assert_raise ExAliyunOts.RuntimeError,
                   ~r/Fail to update timeline with invalid sequence_id/,
                   fn ->
                     update(timeline, %Entry{message: [name: "newtest1", new_field: 97.1, num: 1]})
                   end

      {:ok, search_response} =
        search(
          new(table_name: table_name, index_name: index_name),
          search_query: [
            query: [
              type: QueryType.match(),
              field_name: "name",
              text: "test1"
            ]
          ]
        )

      assert search_response.is_all_succeeded == true

      {:ok, _update_response} =
        update(timeline, %Entry{
          message: [name: "newtest1", new_field: 97.1, num: 1],
          sequence_id: sequence_id
        })

      {:ok, getrow_response} = get(timeline, sequence_id)

      map = Utils.row_to_map(getrow_response.row)

      assert map.new_field == 97.1 and map.name == "newtest1" and map.num == 1 and map.id == "1" and
               map.type == "t1" and map.sequence_id == sequence_id

      {:ok, _delete_response} = delete(timeline, sequence_id)

      {:ok, getrow_response} = get(timeline, sequence_id)

      assert getrow_response.row == nil
    end
  end
end
