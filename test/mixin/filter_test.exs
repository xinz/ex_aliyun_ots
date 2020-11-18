defmodule ExAliyunOts.MixinTest.Filter do
  use ExUnit.Case
  use ExAliyunOts.Constants
  import ExAliyunOts.DSL, only: [filter: 1]

  alias ExAliyunOts.TableStoreFilter.{
    Filter,
    SingleColumnValueFilter,
    CompositeColumnValueFilter
  }

  @result %Filter{
    filter: %CompositeColumnValueFilter{
      combinator: LogicOperator.or(),
      sub_filters: [
        %Filter{
          filter: %CompositeColumnValueFilter{
            combinator: LogicOperator.and(),
            sub_filters: [
              %Filter{
                filter: %SingleColumnValueFilter{
                  column_name: "name",
                  column_value: "updated_attr21",
                  comparator: ComparatorType.equal(),
                  filter_if_missing: false,
                  latest_version_only: true
                },
                type: FilterType.single_column()
              },
              %Filter{
                filter: %SingleColumnValueFilter{
                  column_name: "age",
                  column_value: 1,
                  comparator: ComparatorType.greater_than(),
                  filter_if_missing: true,
                  latest_version_only: true
                },
                type: FilterType.single_column()
              }
            ]
          },
          type: FilterType.composite_column()
        },
        %Filter{
          filter: %SingleColumnValueFilter{
            column_name: "class",
            column_value: "1",
            comparator: ComparatorType.equal(),
            filter_if_missing: true,
            latest_version_only: true
          },
          type: FilterType.single_column()
        }
      ]
    },
    type: FilterType.composite_column()
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
