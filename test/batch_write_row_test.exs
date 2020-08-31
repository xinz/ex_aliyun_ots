defmodule ExAliyunOtsTest.BatchWriteRow do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Const.{PKType, OperationType, ReturnType, RowExistence}
  require PKType
  require OperationType
  require ReturnType
  require RowExistence

  test "batch get row" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name = "test_table_batch_write_row_1_#{cur_timestamp}"
    # table_name = "test_table_batch_write_row"
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [
        {"pkey1", PKType.integer()},
        {"pkey2", PKType.integer()}
      ]
    }

    result = Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    condition = %Var.Condition{
      row_existence: RowExistence.expect_not_exist()
    }

    for partition_key <- 1..10 do
      var_put_row = %Var.PutRow{
        table_name: table_name,
        primary_keys: [{"pkey1", partition_key}, {"pkey2", partition_key}],
        attribute_columns: [
          {"product_name", "pn_#{partition_key}"},
          {"size", partition_key},
          {"value", 1.1 * partition_key}
        ],
        condition: condition,
        return_type: ReturnType.pk()
      }

      {:ok, _result} = Client.put_row(@instance_key, var_put_row)
    end

    condition_exist = %Var.Condition{
      row_existence: RowExistence.expect_exist()
    }

    condition_not_exist = %Var.Condition{
      row_existence: RowExistence.expect_not_exist()
    }

    batch_write_request = [
      %Var.BatchWriteRequest{
        table_name: table_name,
        rows: [
          %Var.RowInBatchWriteRequest{
            type: OperationType.put(),
            primary_keys: [{"pkey1", 1}, {"pkey2", 1}],
            updates: [{"new_added1_1", 100}, {"new_added1_2", 101}],
            condition: condition_exist
          },
          %Var.RowInBatchWriteRequest{
            type: OperationType.update(),
            primary_keys: [{"pkey1", 2}, {"pkey2", 2}],
            updates: %{
              OperationType.put() => [{"size", 1002}, {"product_name", "updated_pn2"}],
              OperationType.delete_all() => ["value"]
            },
            condition: condition_exist,
            return_type: ReturnType.pk()
          },
          %Var.RowInBatchWriteRequest{
            type: OperationType.put(),
            primary_keys: [{"pkey1", 20}, {"pkey2", 20}],
            updates: [{"name", "test name"}, {"size", 55.1}],
            condition: condition_not_exist,
            return_type: ReturnType.pk()
          },
          %Var.RowInBatchWriteRequest{
            type: OperationType.delete(),
            primary_keys: [{"pkey1", 3}, {"pkey2", 3}],
            condition: condition_exist
          }
        ]
      }
    ]

    {:ok, response} = Client.batch_write_row(@instance_key, batch_write_request)
    tables = response.tables
    assert length(tables) == 1

    Enum.with_index(tables)
    |> Enum.map(fn {table_in_reponse, _index} ->
      Enum.with_index(table_in_reponse.rows)
      |> Enum.map(fn {row_in_response, row_index} ->
        case row_index do
          0 ->
            assert row_in_response.row == nil
            assert row_in_response.is_ok == true

          1 ->
            assert row_in_response.row == {[{"pkey1", 2}, {"pkey2", 2}], nil}

          2 ->
            assert row_in_response.row == {[{"pkey1", 20}, {"pkey2", 20}], nil}
            assert row_in_response.is_ok == true

          _ ->
            :ok
        end
      end)
    end)

    # `pkey1` as 3 should be deleted, cannot get it any more.
    var_get_row = %Var.GetRow{
      table_name: table_name,
      primary_keys: [{"pkey1", 3}, {"pkey2", 3}]
    }

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)
    assert {:ok, get_row_response} = get_row_result
    assert get_row_response.row == nil

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name)
    assert result == :ok
  end
end
