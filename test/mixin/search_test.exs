defmodule ExAliyunOts.MixinTest.Search do
  
  use ExUnit.Case
  use ExAliyunOts.Mixin

  require Logger
  alias ExAliyunOts.Const.Search.{ColumnReturnType, QueryType}
  require ColumnReturnType
  require QueryType

  @instance_name "edc-ex-test"

  test "match query" do
    table_name = "test_table"
    index_name = "test_search_index"
    result = 
      search @instance_name, table_name, index_name,
      #columns_to_get: {ColumnReturnType.specified, ["class", "name"]},
      #columns_to_get: ColumnReturnType.none,
      #columns_to_get: ColumnReturnType.all,
        columns_to_get: ["class"],
        search_query: [
          query: [
            type: QueryType.match,
            field_name: "age",
            text: "28"
          ],
          limit: 1
        ]
    Logger.info "result: #{inspect result}"
  end

end
