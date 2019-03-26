defmodule ExAliyunOtsTest.Filter do
  use ExUnit.Case
  
  require Logger

  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence, FilterType, ComparatorType, LogicOperator}
  require PKType
  require OperationType
  require ReturnType
  require RowExistence
  require FilterType
  require ComparatorType
  require LogicOperator

  @instance_key EDCEXTestInstance

  test "filter" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_filter#{cur_timestamp}"
    #table_name = "test_filter"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"id", PKType.string}],
    }
    result = ExAliyunOts.Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    Logger.info "waiting for table created..."
    Process.sleep(5_000)

    # test SingleColumnValueFilter
    # `ignore_if_missing`: false, if the filter is not matched/existed, we will get error message `OTSConditionCheckFailCondition check failed` and prevent the corresponding update.
    # `ignore_if_missing`: true, if the filter is not matched/existed, we will ignore this filter condition, and make the corresponding update continuing happened.
    id = "1"
    filter = %Var.Filter{
      filter_type: FilterType.single_column,
      filter: %Var.SingleColumnValueFilter{
        comparator: ComparatorType.eq,
        column_name: "counter",
        column_value: 1,
        ignore_if_missing: false
      }
    }
    condition = %Var.Condition{
      row_existence: RowExistence.ignore,
      column_condition: filter
    }
    counter = 1
    var_update_row = %Var.UpdateRow{
      table_name: table_name,
      primary_keys: [{"id", id}],
      updates: %{
        OperationType.put => [{"counter", counter}]
      },
      condition: condition,
      return_type: ReturnType.pk
    }
    result = ExAliyunOts.Client.update_row(@instance_key, var_update_row)
    assert result == {:error, "OTSConditionCheckFailCondition check failed."}

    id = "2"
    filter = %{filter | filter: %{filter.filter | ignore_if_missing: true}}
    condition = %{condition | column_condition: filter}
    var_update_row = %{var_update_row | updates: %{OperationType.put => [{"counter", 2}]}, condition: condition, primary_keys: [{"id", id}]}
    {:ok, response} = ExAliyunOts.Client.update_row(@instance_key, var_update_row)
    assert response.row == {[{"id", "2"}], nil}

    # test CompositeColumnValueFilter
    id = "3"
    var_put_row = %Var.PutRow{
      table_name: table_name,
      primary_keys: [{"id", id}],
      attribute_columns: [{"counter", 2}, {"name", "tmp_name"}],
      condition: %Var.Condition{
        row_existence: RowExistence.ignore
      }
    }
    {:ok, _result} = ExAliyunOts.Client.put_row(@instance_key, var_put_row)
    filter = %Var.Filter{
      filter_type: FilterType.composite_column,
      filter: %Var.CompositeColumnValueFilter{
        combinator: LogicOperator.and,
        sub_filters: [
          %Var.Filter{
            filter_type: FilterType.single_column,
            filter: %Var.SingleColumnValueFilter{
              comparator: ComparatorType.eq,
              column_name: "name",
              column_value: "tmp_name"
            }
          },
          %Var.Filter{
            filter_type: FilterType.single_column,
            filter: %Var.SingleColumnValueFilter{
              comparator: ComparatorType.gt,
              column_name: "counter",
              column_value: 1
            }
          }
        ]
      }
    }
    condition = %Var.Condition{
      row_existence: RowExistence.ignore,
      column_condition: filter
    }
    var_update_row = %Var.UpdateRow{
      table_name: table_name,
      primary_keys: [{"id", id}],
      updates: %{
        OperationType.put => [{"new_added", "newaddedfield"}],
      },
      condition: condition,
      return_type: ReturnType.pk
    }
    {:ok, response} = ExAliyunOts.Client.update_row(@instance_key, var_update_row)
    assert response.row == {[{"id", "3"}], nil}

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name)
    assert result == :ok
  end

end
