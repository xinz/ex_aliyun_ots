defmodule ExAliyunOts.MixinTest.ConditionAndFilter do
  use ExUnit.Case
  use ExAliyunOts.Constants
  import ExAliyunOts.DSL, only: [condition: 2, filter: 1]
  alias ExAliyunOts.TableStoreFilter.{Filter, CompositeColumnValueFilter, SingleColumnValueFilter}
  alias ExAliyunOts.TableStore.Condition

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

  def value(), do: "attr21"

  test "bind variables" do
    value1 = "attr21"
    condition_result = condition(:expect_exist, "attr2" == value1)

    assert condition_result == %Condition{
             column_condition: %Filter{
               filter: %SingleColumnValueFilter{
                 column_name: "attr2",
                 column_value: "attr21",
                 comparator: ComparatorType.equal(),
                 filter_if_missing: true,
                 latest_version_only: true
               },
               type: FilterType.single_column()
             },
             row_existence: RowExistence.expect_exist()
           }

    key = "attr2"
    condition_result2 = condition(:expect_exist, key == value1)

    assert condition_result == condition_result2
    assert condition_result == condition(:expect_exist, "attr2" == value())

    value1 = "updated_attr21"
    class_field = "class"
    age_field = "age"
    name_field = {"name", ignore_if_missing: true, latest_version_only: true}

    filter_result =
      filter(
        ({"name", ignore_if_missing: true, latest_version_only: true} == value1 and "age" > 1) or
          "class" == "1"
      )

    filter_result_1 =
      filter(
        ({"name", ignore_if_missing: true, latest_version_only: true} == value1 and "age" > 1) or
          class_field == "1"
      )

    filter_result_2 =
      filter(
        ({"name", ignore_if_missing: true, latest_version_only: true} == value1 and age_field > 1) or
          class_field == "1"
      )

    filter_result_3 =
      filter(
        (name_field == value1 and age_field > 1) or
          class_field == "1"
      )

    assert filter_result == @result
    assert filter_result_1 == @result
    assert filter_result_2 == @result
    assert filter_result_3 == @result
  end
end
