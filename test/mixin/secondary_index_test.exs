defmodule ExAliyunOts.MixinTest.SecondaryIndexTest do
  use ExAliyunOts,
    instance: EDCEXTestInstance

  use ExUnit.Case

  @tag :skip

  @index_name "TestIndex"

  test "global secondary index test" do
    {:ok, item} = get_row(@index_name, [{"name", "namea"}, {"key", "1"}])
    assert item != nil

    inclusive = [{"name", "namea"}, {"key", :inf_min}]
    exclusive = [{"name", "namea"}, {"key", :inf_max}]
    {:ok, response} = get_range(@index_name, inclusive, exclusive)

    [{pks, _attr}] = response.rows
    [{"name", "namea"}, {"key", key}] = pks
    assert key == "1"
  end
end
