defmodule ExAliyunOts.MixinTest.ConditionAndFilter do
  use ExUnit.Case

  import ExAliyunOts.Mixin

  test "bind variables" do
    table_name1 = "table_name1"
    key2_attr2_ts = "123"
    value1 = "attr21"
    value2 = "attr3"
    condition_result = condition :expect_exist, "attr2" == value1

    assert condition_result == %ExAliyunOts.Var.Condition{column_condition: %ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.SingleColumnValueFilter{column_name: "attr2", column_value: "attr21", comparator: :CT_EQUAL, ignore_if_missing: false, latest_version_only: true}, filter_type: :FT_SINGLE_COLUMN_VALUE}, row_existence: :EXPECT_EXIST}

    value1 = "updated_attr21"
    filter_result = filter(("name[ignore_if_missing: true, latest_version_only: true]" == value1 and "age" > 1) or ("class" == "1"))

    assert filter_result == %ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.CompositeColumnValueFilter{combinator: :LO_OR, sub_filters: [%ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.CompositeColumnValueFilter{combinator: :LO_AND, sub_filters: [%ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.SingleColumnValueFilter{column_name: "name", column_value: "updated_attr21", comparator: :CT_EQUAL, ignore_if_missing: true, latest_version_only: true}, filter_type: :FT_SINGLE_COLUMN_VALUE}, %ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.SingleColumnValueFilter{column_name: "age", column_value: 1, comparator: :CT_GREATER_THAN, ignore_if_missing: false, latest_version_only: true}, filter_type: :FT_SINGLE_COLUMN_VALUE}]}, filter_type: :FT_COMPOSITE_COLUMN_VALUE}, %ExAliyunOts.Var.Filter{filter: %ExAliyunOts.Var.SingleColumnValueFilter{column_name: "class", column_value: "1", comparator: :CT_EQUAL, ignore_if_missing: false, latest_version_only: true}, filter_type: :FT_SINGLE_COLUMN_VALUE}]}, filter_type: :FT_COMPOSITE_COLUMN_VALUE}
  end
end
