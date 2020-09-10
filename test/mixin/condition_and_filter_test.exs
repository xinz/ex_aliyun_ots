defmodule ExAliyunOts.MixinTest.ConditionAndFilter do
  use ExUnit.Case

  import ExAliyunOts, only: [condition: 2, filter: 1]

  def value() do
    1
  end

  test "bind variables" do
    value1 = "attr21"
    condition_result = condition(:expect_exist, "attr2" == value1)

    assert condition_result == %ExAliyunOts.Var.Condition{
             column_condition: %ExAliyunOts.Var.Filter{
               filter: %ExAliyunOts.Var.SingleColumnValueFilter{
                 column_name: "attr2",
                 column_value: "attr21",
                 comparator: :CT_EQUAL,
                 ignore_if_missing: false,
                 latest_version_only: true
               },
               filter_type: :FT_SINGLE_COLUMN_VALUE
             },
             row_existence: :EXPECT_EXIST
           }

    key = "attr2"
    condition_result2 = condition(:expect_exist, key == value1)

    assert condition_result == condition_result2

    assert_raise ExAliyunOts.RuntimeError, ~r/Invalid expression `value\(\)`/, fn ->
      condition(:expect_exist, "attr2" == value())
    end

    value1 = "updated_attr21"
    class_field = "class"
    age_field = "age"
    name_field = "name[ignore_if_missing: true, latest_version_only: true]"

    filter_result =
      filter(
        ("name[ignore_if_missing: true, latest_version_only: true]" == value1 and "age" > 1) or
          "class" == "1"
      )

    filter_result_1 =
      filter(
        ("name[ignore_if_missing: true, latest_version_only: true]" == value1 and "age" > 1) or
          class_field == "1"
      )

    filter_result_2 =
      filter(
        ("name[ignore_if_missing: true, latest_version_only: true]" == value1 and age_field > 1) or
          class_field == "1"
      )

    filter_result_3 =
      filter(
        (name_field == value1 and age_field > 1) or
          class_field == "1"
      )

    assert filter_result == %ExAliyunOts.Var.Filter{
             filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
               combinator: :LO_OR,
               sub_filters: [
                 %ExAliyunOts.Var.Filter{
                   filter: %ExAliyunOts.Var.CompositeColumnValueFilter{
                     combinator: :LO_AND,
                     sub_filters: [
                       %ExAliyunOts.Var.Filter{
                         filter: %ExAliyunOts.Var.SingleColumnValueFilter{
                           column_name: "name",
                           column_value: "updated_attr21",
                           comparator: :CT_EQUAL,
                           ignore_if_missing: true,
                           latest_version_only: true
                         },
                         filter_type: :FT_SINGLE_COLUMN_VALUE
                       },
                       %ExAliyunOts.Var.Filter{
                         filter: %ExAliyunOts.Var.SingleColumnValueFilter{
                           column_name: "age",
                           column_value: 1,
                           comparator: :CT_GREATER_THAN,
                           ignore_if_missing: false,
                           latest_version_only: true
                         },
                         filter_type: :FT_SINGLE_COLUMN_VALUE
                       }
                     ]
                   },
                   filter_type: :FT_COMPOSITE_COLUMN_VALUE
                 },
                 %ExAliyunOts.Var.Filter{
                   filter: %ExAliyunOts.Var.SingleColumnValueFilter{
                     column_name: "class",
                     column_value: "1",
                     comparator: :CT_EQUAL,
                     ignore_if_missing: false,
                     latest_version_only: true
                   },
                   filter_type: :FT_SINGLE_COLUMN_VALUE
                 }
               ]
             },
             filter_type: :FT_COMPOSITE_COLUMN_VALUE
           }

    assert filter_result == filter_result_1
    assert filter_result == filter_result_2
    assert filter_result == filter_result_3
  end

end
