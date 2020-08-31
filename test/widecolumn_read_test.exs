defmodule ExAliyunOtsTest.WideColumnRead do
  use ExUnit.Case

  use ExAliyunOts,
    instance: EDCEXTestInstance

  require Logger
  alias ExAliyunOts.Const.PKType
  require PKType

  test "wide column read" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_wcr_#{cur_timestamp}"

    create_table_result = create_table(table_name, [{"key", PKType.string()}])

    assert create_table_result == :ok

    {:ok, _putrow_response} =
      put_row(
        table_name,
        [{"key", "1"}],
        [
          {"bcol", "bc"},
          {"ecol", "ec"},
          {"dcol", "dc"},
          {"acol", "ac"},
          {"fcol", "fc"},
          {"ccol", "cc"},
          {"room_a", "room_a1"},
          {"room_c", "room_c2"},
          {"room_b", "room_b3"},
          {"room_x", "room_x4"},
          {"room_g", "room_g5"},
          {"room_e", "room_e6"}
        ],
        condition: condition(:expect_not_exist)
      )

    # wide column read by start/end
    # using `start_column` as "room", `end_column` as "room|" will get all "room_*" attribute columns
    {:ok, response} =
      get_row(table_name, [{"key", "1"}],
        start_column: "room",
        end_column: "room|"
      )

    {_key, attrs} = response.row
    assert length(attrs) == 6
    assert {"room_a", "room_a1", _} = Enum.at(attrs, 0)
    assert {"room_b", "room_b3", _} = Enum.at(attrs, 1)
    assert {"room_c", "room_c2", _} = Enum.at(attrs, 2)
    assert {"room_e", "room_e6", _} = Enum.at(attrs, 3)
    assert {"room_g", "room_g5", _} = Enum.at(attrs, 4)
    assert {"room_x", "room_x4", _} = Enum.at(attrs, 5)

    # wide column read by filter
    {:ok, response} =
      get_row(table_name, [{"key", "1"}],
        start_column: "room",
        filter: pagination(offset: 0, limit: 3)
      )

    {_key, attrs} = response.row
    assert length(attrs) == 3
    assert {"room_a", "room_a1", _} = Enum.at(attrs, 0)
    assert {"room_b", "room_b3", _} = Enum.at(attrs, 1)
    assert {"room_c", "room_c2", _} = Enum.at(attrs, 2)

    # delete table
    del_result = delete_table(table_name)
    assert del_result == :ok
  end
end
