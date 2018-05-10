defmodule ExAliyunOts.MixinTest.ConditionAndFilter do
  use ExUnit.Case
  use ExAliyunOts.Mixin
  require Logger

  test "bind variables" do
    table_name1 = "table_name1"
    key2_attr2_ts = "123"
    value1 = "attr21"
    value2 = "attr3"
    condition_result = condition :expect_exist, "attr2" == value1
    Logger.info "condition_result: #{inspect condition_result}"

    value1 = "updated_attr21"
    filter_result = filter(("name[ignore_if_missing: true, latest_version_only: true]" == value1 and "age" > 1) or ("class" == "1"))
    Logger.info "filter_result: #{inspect filter_result}"
  end
end
