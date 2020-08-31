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

  test "valid geo_point" do
    assert Utils.valid_geo_point?("-1,0") == true
    assert Utils.valid_geo_point?("10.0,20.123") == true
    assert Utils.valid_geo_point?("-4.51,-98.1") == true
    assert Utils.valid_geo_point?("20,-15") == true
    assert Utils.valid_geo_point?("9,9") == true
    assert Utils.valid_geo_point?("5,9.9813") == true

    assert Utils.valid_geo_point?(",10.01") == false
    assert Utils.valid_geo_point?("23") == false
    assert Utils.valid_geo_point?("31,") == false
    assert Utils.valid_geo_point?("-1,") == false
    assert Utils.valid_geo_point?(",-1") == false
  end
end
