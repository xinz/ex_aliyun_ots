defmodule ExAliyunOts.MixinTest.Filter do
  use ExUnit.Case
  import ExAliyunOts.DSL, only: [filter: 1]

  @result %ExAliyunOts.Var.Filter{
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

  test "filter" do
    value1 = "updated_attr21"
    class_field = "class"
    age_field = "age"
    options = [ignore_if_missing: true, latest_version_only: true]

    filter_result =
      filter(
        ({"name", [ignore_if_missing: true, latest_version_only: true]} == value1 and "age" > 1) or
          "class" == "1"
      )

    name_expr = filter({"name", options} == value1)

    filter_result_1 = filter((name_expr and "age" > 1) or class_field == "1")

    age_expr = filter(age_field > 1)

    name_with_age_expr = filter(name_expr and age_expr)

    filter_result_2 = filter(name_with_age_expr or class_field == "1")

    assert filter_result == @result
    assert filter_result_1 == @result
    assert filter_result_2 == @result
  end
end
