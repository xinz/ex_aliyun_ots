defmodule ExAliyunOts.MixinTest.Transaction do
  use ExUnit.Case

  use ExAliyunOts,
    instance: EDCEXTestInstance

  require Logger

  @table "test_txn"
  @table_range "test_txn_range"

  setup_all do
    on_exit(fn ->
      delete_row @table, [{"key", "key1"}], condition: condition(:ignore)
      delete_row @table, [{"key", "key2"}], condition: condition(:ignore)
    end)
    :ok
  end

  test "start and abort" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    abort_transaction(response.transaction_id)
  end

  test "put row with transaction_id and commit" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:error, message} =
      put_row @table, [partition_key],
        [{"attr1", "1"}],
        condition: condition(:ignore)

    assert String.contains?(message, "OTSRowOperationConflict,Data is being modified by the other request") == true

    {:ok, _response} =
      put_row @table, [partition_key],
        [{"attr1", "1"}],
        condition: condition(:ignore),
        transaction_id: transaction_id

    commit_transaction(transaction_id)

    {:ok, response} =
      get_row @table, [partition_key],
        columns_to_get: ["attr1"]
    assert response.row != nil
  end

  test "put row with transaction_id and abort" do
    partition_key = {"key", "key2"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id
    
    {:ok, _response} =
      put_row @table, [partition_key],
        [{"attr2", "2"}],
        condition: condition(:ignore),
        transaction_id: transaction_id

    abort_transaction(transaction_id)

    {:ok, response} =
      get_row @table, [partition_key],
        columns_to_get: ["attr2"]

    assert response.row == nil
  end

  test "update row with transaction_id and commit" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:error, message} =
      update_row @table, [partition_key],
        put: [{"new_attr1", "a1"}],
        delete_all: ["level", "size"],
        condition: condition(:ignore)

    assert String.contains?(message, "OTSRowOperationConflict,Data is being modified by the other request") == true

    {:ok, _response} =
      update_row @table, [partition_key],
        put: [{"new_attr1", "a1"}],
        delete_all: ["level", "size"],
        condition: condition(:ignore),
        transaction_id: transaction_id

    commit_transaction(transaction_id)

    {:ok, response} =
      get_row @table, [partition_key],
        columns_to_get: ["new_attr1"]
    assert response.row != nil
  end

  test "delete row with transaction_id and commit" do
    partition_key = {"key", "key1"}

    put_row @table, [partition_key],
      [{"attr1", "attr1"}],
      condition: condition(:ignore)

    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:error, message} =
      delete_row @table, [partition_key], condition: condition(:ignore)

    assert String.contains?(message, "OTSRowOperationConflict,Data is being modified by the other request") == true

    {:ok, _response} =
      delete_row @table, [partition_key],
        condition: condition(:ignore),
        transaction_id: transaction_id

    {:ok, response} =
      get_row @table, [partition_key],
        columns_to_get: ["attr1"]
    assert response.row != nil

    commit_transaction(transaction_id)

    {:ok, response} =
      get_row @table, [partition_key],
        columns_to_get: ["attr1"]
    assert response.row == nil
  end

  test "batch write with transaction_id and commit" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    batch_write {
      @table,
      [
        write_update([partition_key],
          put: [{"new_added1", 100}, {"new_added2", 101}],
          condition: condition(:ignore)
        )
      ]}, transaction_id: transaction_id

    commit_transaction(transaction_id)

    {:ok, response} = get_row @table, [partition_key]
    {_pks, attrs} = response.row
    Enum.map(attrs, fn({key, value, _ts}) ->
      case key do
        "new_added1" -> assert value == 100
        "new_added2" -> assert value == 101
        true -> :ok
      end
    end)
  end

  test "get row with transaction_id" do
    partition_key = {"key", "key1"}

    put_row @table, [partition_key],
      [{"attr1", "1"}],
      condition: condition(:ignore)

    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:ok, _response} = get_row @table, [partition_key], transaction_id: transaction_id

    {:error, message} =
      update_row @table, [partition_key],
        put: [{"attr_1_v2", "attr_1_v2"}],
        condition: condition(:ignore)

    assert String.contains?(message, "OTSRowOperationConflict,Data is being modified by the other request") == true

    abort_transaction(transaction_id)
  end

  test "get range with transaction_id and batch write update" do
    for index <- 1..3 do
      put_row @table_range, [{"key", "key1"}, {"key2", index}],
        [{"attr", "attr#{index}"}],
        condition: condition(:ignore)
    end

    put_row @table_range, [{"key", "key2"}, {"key2", 1}],
      [{"attr", "key2attr1"}],
      condition: condition(:ignore)

    partition_key = {"key", "key1"}

    {:ok, response} = start_local_transaction(@table_range, partition_key)
    transaction_id = response.transaction_id

    {:ok, response} =
      get_range @table_range,
        [{"key", "key1"}, {"key2", PKType.inf_min}],
        [{"key", "key1"}, {"key2", PKType.inf_max}],
        transaction_id: transaction_id

    range_rows = response.rows

    rows_to_batch_write =
      Enum.map(range_rows, fn({pks, _attrs}) ->
        write_update(pks, put: [{"new_added", true}], condition: condition(:ignore)) 
      end)

    batch_write {@table_range, rows_to_batch_write}, transaction_id: transaction_id

    rows_to_batch_write_failed =
      Enum.map(range_rows, fn({pks, _attrs}) ->
        write_update(pks, put: [{"new_added2", true}], condition: condition(:ignore))
      end)

    {:ok, response} =
      batch_write {@table_range, rows_to_batch_write_failed}

    Enum.map(response.tables, fn(table_response) ->
      Enum.map(table_response.rows, fn(row_response) ->
        assert String.contains?(row_response.error.message, "Data is being modified by the other request") == true
      end)
    end)


    rows_to_batch_write2 = 
      Enum.map(range_rows, fn({pks, _attrs}) ->
        write_update(pks, put: [{"new_added2", "new_added2"}], condition: condition(:ignore))
      end)

    # can update multi times before commit this transaction
    batch_write {@table_range, rows_to_batch_write2}, transaction_id: transaction_id

    commit_transaction(transaction_id)

    {:ok, response} =
      get_range @table_range,
        [{"key", "key1"}, {"key2", PKType.inf_min}],
        [{"key", "key1"}, {"key2", PKType.inf_max}]
    Enum.map(response.rows, fn({_pks, attrs}) ->
      assert length(attrs) == 3
    end)
  end

end
