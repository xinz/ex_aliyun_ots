defmodule ExAliyunOtsTest.Transaction do
  use ExUnit.Case
  import ExAliyunOts.DSL, only: [condition: 1]
  require Logger
  alias ExAliyunOts.{Client, Var}
  alias ExAliyunOts.Var.Transaction.StartLocalTransactionRequest
  alias ExAliyunOts.Const.{OperationType, PKType}
  require OperationType
  require PKType

  @instance_key EDCEXTestInstance
  @table "test_txn"
  @table_range "test_txn_range"

  setup_all do
    on_exit(fn ->
      condition = condition(:ignore)

      var_delete_row1 = %Var.DeleteRow{
        table_name: @table,
        primary_keys: [{"key", "key1"}],
        condition: condition
      }

      Client.delete_row(@instance_key, var_delete_row1)

      var_delete_row2 = %Var.DeleteRow{
        table_name: @table,
        primary_keys: [{"key", "key2"}],
        condition: condition
      }

      Client.delete_row(@instance_key, var_delete_row2)

      for index <- 1..3 do
        var_delete_range = %Var.DeleteRow{
          table_name: @table_range,
          primary_keys: [{"key", "key1"}, {"key2", index}],
          condition: condition
        }

        Client.delete_row(@instance_key, var_delete_range)
      end

      var_delete_range = %Var.DeleteRow{
        table_name: @table_range,
        primary_keys: [{"key", "key2"}, {"key2", 1}],
        condition: condition
      }

      Client.delete_row(@instance_key, var_delete_range)
    end)

    :ok
  end

  test "start and abort" do
    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: {"key", "key1"}
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)

    transaction_id = response.transaction_id
    abort_response = Client.abort_transaction(@instance_key, transaction_id)
    Logger.info("#{inspect(abort_response)}")
  end

  test "put row with transaction_id and commit" do
    partition_key = {"key", "key1"}

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)

    transaction_id = response.transaction_id

    condition = condition(:ignore)

    var_put_row = %Var.PutRow{
      table_name: @table,
      primary_keys: [partition_key],
      attribute_columns: [{"attr1", "1"}],
      condition: condition
    }

    {:error, error} = Client.put_row(@instance_key, var_put_row)
    Logger.info("put row after it's locked with other operation, result: #{inspect(error)}")
    assert error.code == "OTSRowOperationConflict"

    updated_var_put_row = Map.put(var_put_row, :transaction_id, transaction_id)

    result = Client.put_row(@instance_key, updated_var_put_row)
    Logger.info("put row with transaction_id, result: #{inspect(result)}, will commit it.")

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key],
      columns_to_get: ["attr1"]
    }

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "get row before the final transaction commited, will not find any data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row == nil

    Client.commit_transaction(@instance_key, transaction_id)

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "get row after the final transaction commited, will find matched record as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row != nil
  end

  test "put row with transaction_id and abort it" do
    partition_key = {"key", "key2"}

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)

    transaction_id = response.transaction_id

    condition = condition(:ignore)

    var_put_row = %Var.PutRow{
      table_name: @table,
      primary_keys: [partition_key],
      attribute_columns: [{"attr2", "2"}],
      condition: condition,
      transaction_id: transaction_id
    }

    result = Client.put_row(@instance_key, var_put_row)
    Logger.info("put row with transaction_id, result: #{inspect(result)}, will abort it.")

    Client.abort_transaction(@instance_key, transaction_id)

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key],
      columns_to_get: ["attr2"]
    }

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "get row after transaction aborted, will not find any data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row == nil
  end

  test "update row with transaction_id and commit" do
    partition_key = {"key", "key1"}

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)
    transaction_id = response.transaction_id

    condition = condition(:ignore)

    var_update_row = %Var.UpdateRow{
      table_name: @table,
      primary_keys: [partition_key],
      updates: %{
        OperationType.put() => [{"new_attr1", "a1"}],
        OperationType.delete_all() => ["level", "size"]
      },
      condition: condition
    }

    {:error, error} = Client.update_row(@instance_key, var_update_row)
    Logger.info("update row after it's locked in other operation, result: #{inspect(error)}")

    assert error.code == "OTSRowOperationConflict" and
             error.message == "Data is being modified by the other request."

    updated_var_update_row = Map.put(var_update_row, :transaction_id, transaction_id)
    Client.update_row(@instance_key, updated_var_update_row)

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key],
      columns_to_get: ["new_attr1"]
    }

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "get row before the final transaction commit, will not find any data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row == nil

    Client.commit_transaction(@instance_key, transaction_id)

    get_row_result = ExAliyunOts.Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "get row after the final transaction commited, will find matched data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row != nil
  end

  test "delete row with transaction_id and commit" do
    partition_key = {"key", "key1"}

    Client.put_row(@instance_key, %Var.PutRow{
      table_name: @table,
      primary_keys: [partition_key],
      attribute_columns: [{"attr1", "attr1"}],
      condition: condition(:ignore)
    })

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)
    transaction_id = response.transaction_id

    condition = condition(:ignore)

    var_delete_row = %Var.DeleteRow{
      table_name: @table,
      primary_keys: [partition_key],
      condition: condition
    }

    {:error, error} = Client.delete_row(@instance_key, var_delete_row)
    Logger.info("delete row after it's locked in other operation, result: #{inspect(error)}")
    assert error.code == "OTSRowOperationConflict"

    updated_var_delete_row = Map.put(var_delete_row, :transaction_id, transaction_id)
    Client.delete_row(@instance_key, updated_var_delete_row)

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key],
      columns_to_get: ["attr1"]
    }

    get_row_result = Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "since delete row transaction is not commited, we still can find matched data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row != nil

    Client.commit_transaction(@instance_key, transaction_id)

    get_row_result = Client.get_row(@instance_key, var_get_row)

    Logger.info(
      "since delete row transaction is commited, we can not find data as expcted: #{
        inspect(get_row_result)
      }"
    )

    {:ok, response} = get_row_result
    assert response.row == nil
  end

  test "batch write with transaction_id and commit" do
    partition_key = {"key", "key1"}

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)

    transaction_id = response.transaction_id

    condition = condition(:ignore)

    # If you have multi primary keys,
    # you can use partition_key (ONLY one) to batch write multi rows with transaction
    batch_write_request = %Var.BatchWriteRequest{
      table_name: @table,
      rows: [
        %Var.RowInBatchWriteRequest{
          type: OperationType.put(),
          primary_keys: [partition_key],
          updates: [{"new_added1", 100}, {"new_added2", 101}],
          condition: condition
        }
      ]
    }

    Client.batch_write_row(@instance_key, batch_write_request, transaction_id: transaction_id)

    Client.commit_transaction(@instance_key, transaction_id)

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key]
    }

    {:ok, response} = Client.get_row(@instance_key, var_get_row)
    Logger.info("get row after batch write with transaction_id, result: #{inspect(response)}")

    {_pks, attrs} = response.row

    Enum.map(attrs, fn {key, value, _ts} ->
      case key do
        "new_added1" -> assert value == 100
        "new_added2" -> assert value == 101
        true -> :ok
      end
    end)
  end

  test "get row with transaction_id" do
    partition_key = {"key", "key1"}

    Client.put_row(@instance_key, %Var.PutRow{
      table_name: @table,
      primary_keys: [partition_key],
      attribute_columns: [{"attr1", "attr1"}],
      condition: condition(:ignore)
    })

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)

    transaction_id = response.transaction_id

    var_get_row = %Var.GetRow{
      table_name: @table,
      primary_keys: [partition_key],
      transaction_id: transaction_id
    }

    {:ok, response} = Client.get_row(@instance_key, var_get_row)
    assert response.row != nil

    {:error, error} =
      Client.update_row(@instance_key, %Var.UpdateRow{
        table_name: @table,
        primary_keys: [partition_key],
        updates: %{
          OperationType.put() => [{"attr_1_v2", "attr_1_v2"}]
        },
        condition: condition(:ignore)
      })

    assert error.code == "OTSRowOperationConflict"

    Client.abort_transaction(@instance_key, transaction_id)
  end

  test "get range with transaction_id" do
    partition_key = {"key", "key1"}

    Client.put_row(@instance_key, %Var.PutRow{
      table_name: @table,
      primary_keys: [partition_key],
      attribute_columns: [{"attr1", "attr1"}],
      condition: condition(:ignore)
    })

    request = %StartLocalTransactionRequest{
      table_name: @table,
      partition_key: partition_key
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)
    transaction_id = response.transaction_id

    var_get_range = %Var.GetRange{
      table_name: @table,
      inclusive_start_primary_keys: [partition_key],
      exclusive_end_primary_keys: [{"key", "key2"}],
      limit: 3,
      transaction_id: "fake_transaction_id"
    }

    {:error, error} = Client.get_range(@instance_key, var_get_range)
    assert error.code == "OTSParameterInvalid"

    var_get_range = Map.put(var_get_range, :transaction_id, transaction_id)

    {:error, error} = Client.get_range(@instance_key, var_get_range)

    assert error.code == "OTSDataOutOfRange"

    assert error.message ==
             "Data out of scope of transaction. Transaction PartKey:key1. Data PartKey:key2"

    Client.abort_transaction(@instance_key, transaction_id)
  end

  test "get range with transaction_id and batch write update" do
    condition = condition(:ignore)

    for index <- 1..3 do
      Client.put_row(@instance_key, %Var.PutRow{
        table_name: @table_range,
        primary_keys: [{"key", "key1"}, {"key2", index}],
        attribute_columns: [{"attr", "attr#{index}"}],
        condition: condition
      })
    end

    Client.put_row(@instance_key, %Var.PutRow{
      table_name: @table_range,
      primary_keys: [{"key", "key2"}, {"key2", 1}],
      attribute_columns: [{"attr", "key2attr1"}],
      condition: condition
    })

    request = %StartLocalTransactionRequest{
      table_name: @table_range,
      partition_key: {"key", "key1"}
    }

    {:ok, response} = Client.start_local_transaction(@instance_key, request)
    transaction_id = response.transaction_id

    var_get_range = %Var.GetRange{
      table_name: @table_range,
      inclusive_start_primary_keys: [{"key", "key1"}, {"key2", PKType.inf_min()}],
      exclusive_end_primary_keys: [{"key", "key1"}, {"key2", PKType.inf_max()}]
    }

    {:ok, response} =
      Client.get_range(@instance_key, Map.put(var_get_range, :transaction_id, transaction_id))

    range_rows = response.rows

    rows_to_batch_write =
      Enum.map(range_rows, fn {pks, _attrs} ->
        %Var.RowInBatchWriteRequest{
          type: OperationType.update(),
          primary_keys: pks,
          updates: %{
            OperationType.put() => [{"new_added", true}]
          },
          condition: condition
        }
      end)

    batch_write_request = %Var.BatchWriteRequest{
      table_name: @table_range,
      rows: rows_to_batch_write
    }

    Client.batch_write_row(@instance_key, batch_write_request, transaction_id: transaction_id)

    rows_to_batch_write_failed_case =
      Enum.map(response.rows, fn {pks, _attrs} ->
        %Var.RowInBatchWriteRequest{
          type: OperationType.update(),
          primary_keys: pks,
          updates: %{
            OperationType.put() => [{"new_added2", true}]
          },
          condition: condition
        }
      end)

    batch_write_request_failed = %Var.BatchWriteRequest{
      table_name: @table_range,
      rows: rows_to_batch_write_failed_case
    }

    {:ok, response} = Client.batch_write_row(@instance_key, batch_write_request_failed)

    Enum.map(response.tables, fn table_response ->
      Enum.map(table_response.rows, fn row_response ->
        assert row_response.error.code == "OTSRowOperationConflict"
      end)
    end)

    rows_to_batch_write =
      Enum.map(range_rows, fn {pks, _attrs} ->
        %Var.RowInBatchWriteRequest{
          type: OperationType.update(),
          primary_keys: pks,
          updates: %{
            OperationType.put() => [{"new_added2", "new_added2"}]
          },
          condition: condition
        }
      end)

    batch_write_request = %Var.BatchWriteRequest{
      table_name: @table_range,
      rows: rows_to_batch_write
    }

    # can update multi times before commit this transaction
    Client.batch_write_row(@instance_key, batch_write_request, transaction_id: transaction_id)

    Client.commit_transaction(@instance_key, transaction_id)

    {:ok, response} = Client.get_range(@instance_key, var_get_range)

    Enum.map(response.rows, fn {_pks, attrs} ->
      assert length(attrs) == 3
    end)
  end
end
