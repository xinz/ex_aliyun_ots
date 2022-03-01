defmodule ExAliyunOts.MixinTest.SearchVirtualFieldTest do
  use ExUnit.Case
  use ExAliyunOts, instance: EDCEXTestInstance
  require Logger

  @table "test_search_virtual_field"
  @index "test_search_virtual_field_index"

  setup_all do
    Application.ensure_all_started(:ex_aliyun_ots)
    clean_all()
    init()
    on_exit(&clean_all/0)
  end

  defp init() do
    create_table(@table, [{"id", :string}])
    insert_test_data()

    create_search_index(@table, @index,
      field_schemas: [
        field_schema_keyword("id"),
        field_schema_integer("int"),
        field_schema_float("double"),
        field_schema_keyword("array", is_array: true),
        field_schema_keyword("text"),
        field_schema_integer("id_virtual_integer", is_virtual_field: true, source_field_name: "id"),
        field_schema_keyword("int_virtual_keyword",
          is_virtual_field: true,
          source_field_name: "int"
        ),
        field_schema_keyword("double_virtual_keyword",
          is_virtual_field: true,
          source_field_name: "double"
        ),
        field_schema_integer("array_virtual_integer",
          is_array: true,
          is_virtual_field: true,
          source_field_name: "array"
        ),
        field_schema_text("text_virtual_text",
          is_virtual_field: true,
          source_field_name: "text",
          analyzer: "fuzzy",
          analyzer_parameter: [min_chars: 1, max_chars: 5]
        )
      ]
    )

    # wait for search index enable
    Process.sleep(60_000)
  end

  defp insert_test_data() do
    data = [
      %{
        id: "1",
        int: 1,
        double: 1.1,
        array: Jason.encode!(["1", "10", "100"]),
        text: "hello world"
      },
      %{
        id: "2",
        int: 2,
        double: 2.2,
        array: Jason.encode!(["2", "20", "200"]),
        text: "hello world 2"
      },
      %{
        id: "3",
        int: 3,
        double: 3.3,
        array: Jason.encode!(["3", "30", "300"]),
        text: "hello world 3"
      },
      %{
        id: "4",
        int: 4,
        double: 4.4,
        array: Jason.encode!(["4", "40", "400"]),
        text: "hello world 4"
      },
      %{
        id: "5",
        int: 5,
        double: 5.5,
        array: Jason.encode!(["5", "50", "500"]),
        text: "hello world 5"
      }
    ]

    Enum.map(data, fn item ->
      fields =
        ExAliyunOts.Utils.attrs_to_row(item)
        |> Keyword.drop(["id"])

      pks = [{"id", item.id}]
      put_row(@table, pks, fields, condition: condition(:expect_not_exist))
    end)
  end

  defp clean_all() do
    delete_search_index(@table, @index)
    delete_table(@table)
  end

  test "normal search index" do
    opts = [
      search_query: [
        query: [
          terms_query("array", ["100", "200"])
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 2
  end

  test "virtual: string 2 integer" do
    opts = [
      search_query: [
        query: [
          term_query("id_virtual_integer", 5)
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 1
  end

  test "virtual: integer 2 string" do
    opts = [
      search_query: [
        query: [
          term_query("int_virtual_keyword", "3")
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 1
  end

  test "virtual: double 2 string" do
    opts = [
      search_query: [
        query: [
          terms_query("double_virtual_keyword", ["2.2", "4.4"])
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 2
  end

  test "virtual: string 2 integer (array)" do
    opts = [
      search_query: [
        query: [
          term_query("array_virtual_integer", 300)
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 1
  end

  test "virtual: string 2 text" do
    opts = [
      search_query: [
        query: [
          match_query("text_virtual_text", "hel")
        ]
      ]
    ]

    {:ok, %{rows: rows}} = search(@table, @index, opts)
    assert length(rows) == 5
  end
end
