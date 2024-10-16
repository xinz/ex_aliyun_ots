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

    assert filter("class" == "1" and "age" >= 1) == filter("class" == "1" && "age" >= 1)
    assert filter("class" == "1" or "age" >= 1) == filter("class" == "1" || "age" >= 1)
    assert filter_result == @result
    assert filter_result_1 == @result
    assert filter_result_2 == @result
  end

  test "filter value_trans_rule with valid cases" do
    value_age = 20
    regex = ~r/^((1[0-5])|[1-9])?\d$/

    result =
      filter({"age", [value_trans_rule: {regex, :integer}]} < value_age)

    assert result.filter.value_trans_rule.regex == Regex.source(regex)

    regex = ~r/type#[a-z]/

    result =
      filter({"name", value_trans_rule: {regex, :string}} <= "c")

    assert result.filter.value_trans_rule.regex == Regex.source(regex)

    regex = ~r/[0-9]*\.?[0-9]+/

    result =
      filter({"price", value_trans_rule: {regex, :double}} <= 10.1)

    assert result.filter.value_trans_rule.regex == Regex.source(regex)
  end

  test "filter value_trans_rule with unknown cast type" do
    assert_raise ExAliyunOts.RuntimeError,
                 ~r/Invalid cast type: :unknown to value_trans_rule/,
                 fn ->
                   filter({"field", value_trans_rule: {~r/foo/, :unknown}} <= "xyz")
                 end
  end

  test "filter value_trans_rule must be with a regex expression" do
    regex_str = "foo"

    assert_raise ExAliyunOts.RuntimeError,
                 ~r(Invalid value_trans_rule: {\\\"foo\\\", :double}),
                 fn ->
                   filter({"price", value_trans_rule: {regex_str, :double}} <= 10.1)
                 end
  end

  test "build filter to ensure successfully quoted" do
    {%ExAliyunOts.TableStoreFilter.Filter{filter: filter, type: type}, _} =
      ExAliyunOts.Filter.build_filter({:==, "some", [:a, 1]}) |> Code.eval_quoted()

    assert filter.column_name == :a and filter.column_value == 1 and
             type == :FT_SINGLE_COLUMN_VALUE

    {%ExAliyunOts.TableStoreFilter.Filter{filter: filter, type: type}, _} =
      ExAliyunOts.Filter.build_filter(
        {:and, "some", [{:==, "some", [:b, 2]}, {:>=, "some", [:c, 10]}]}
      )
      |> Code.eval_quoted()

    assert type == :FT_COMPOSITE_COLUMN_VALUE
    [%{filter: sub1}, %{filter: sub2}] = filter.sub_filters
    assert sub1.comparator == :CT_EQUAL and sub1.column_name == :b and sub1.column_value == 2

    assert sub2.comparator == :CT_GREATER_EQUAL and sub2.column_name == :c and
             sub2.column_value == 10
  end
end
