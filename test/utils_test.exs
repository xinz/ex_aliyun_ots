defmodule ExAliyunOtsTest.UtilsTest do
  use ExUnit.Case

  alias ExAliyunOts.Utils

  test "attrs to row" do
    attrs = [name: "name", age: 100, class: nil]
    result = Utils.attrs_to_row(attrs)
    assert result == [{"name", "name"}, {"age", 100}]

    attrs = [{"name", "name"}, {"age", 100}]
    result = Utils.attrs_to_row(attrs)
    assert result == [{"name", "name"}, {"age", 100}]

    attrs = %{name: "name", age: 100, class: nil}
    result = Utils.attrs_to_row(attrs)
    assert result == [{"age", 100}, {"name", "name"}]
    attrs = %{c: 100, a: "name", b: true, class: nil}
    result = Utils.attrs_to_row(attrs)
    assert result == [{"a", "name"}, {"b", true}, {"c", 100}]
  end

end
