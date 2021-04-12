defmodule ExAliyunOts.MixinTest.Transaction do
  use ExUnit.Case

  use ExAliyunOts,
    instance: EDCEXTestInstance

  require Logger

  @table "test_txn"
  @table_range "test_txn_range"

  setup_all do
    on_exit(fn ->
      delete_row(@table, [{"key", "key1"}], condition: condition(:ignore))
      delete_row(@table, [{"key", "key2"}], condition: condition(:ignore))
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

    {:error, error} =
      put_row(@table, [partition_key], [{"attr1", "1"}], condition: condition(:ignore))

    assert error.code == "OTSRowOperationConflict"

    {:ok, _response} =
      put_row(@table, [partition_key], [{"attr1", "1"}],
        condition: condition(:ignore),
        transaction_id: transaction_id
      )

    commit_transaction(transaction_id)

    {:ok, response} = get_row(@table, [partition_key], columns_to_get: ["attr1"])
    assert response.row != nil
  end

  test "put row with transaction_id and abort" do
    partition_key = {"key", "key2"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:ok, _response} =
      put_row(@table, [partition_key], [{"attr2", "2"}],
        condition: condition(:ignore),
        transaction_id: transaction_id
      )

    abort_transaction(transaction_id)

    {:ok, response} = get_row(@table, [partition_key], columns_to_get: ["attr2"])

    assert response.row == nil
  end

  test "update row with transaction_id and commit" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:error, error} =
      update_row(@table, [partition_key],
        put: [{"new_attr1", "a1"}],
        delete_all: ["level", "size"],
        condition: condition(:ignore)
      )

    assert error.code == "OTSRowOperationConflict"

    {:ok, _response} =
      update_row(@table, [partition_key],
        put: [{"new_attr1", "a1"}],
        delete_all: ["level", "size"],
        condition: condition(:ignore),
        transaction_id: transaction_id
      )

    commit_transaction(transaction_id)

    {:ok, response} = get_row(@table, [partition_key], columns_to_get: ["new_attr1"])
    assert response.row != nil
  end

  test "delete row with transaction_id and commit" do
    partition_key = {"key", "key1"}

    put_row(@table, [partition_key], [{"attr1", "attr1"}], condition: condition(:ignore))

    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:error, error} = delete_row(@table, [partition_key], condition: condition(:ignore))

    assert error.code == "OTSRowOperationConflict"

    {:ok, _response} =
      delete_row(@table, [partition_key],
        condition: condition(:ignore),
        transaction_id: transaction_id
      )

    {:ok, response} = get_row(@table, [partition_key], columns_to_get: ["attr1"])
    assert response.row != nil

    commit_transaction(transaction_id)

    {:ok, response} = get_row(@table, [partition_key], columns_to_get: ["attr1"])
    assert response.row == nil
  end

  test "batch write with transaction_id and commit" do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    batch_write(
      {
        @table,
        [
          write_update([partition_key],
            put: [{"new_added1", 100}, {"new_added2", 101}],
            condition: condition(:ignore)
          )
        ]
      },
      transaction_id: transaction_id
    )

    commit_transaction(transaction_id)

    {:ok, response} = get_row(@table, [partition_key])
    {_pks, attrs} = response.row

    Enum.map(attrs, fn {key, value, _ts} ->
      case key do
        "new_added1" -> assert value == 100
        "new_added2" -> assert value == 101
        true -> :ok
      end
    end)
  end

  test "batch write with transaction_id and is_atomic" do
    partition_key = {"key", "atomic_key"}
    {:ok, response} = start_local_transaction(@table_range, partition_key)
    transaction_id = response.transaction_id

    {:ok, response} =
      batch_write(
        {
          @table_range,
          [
            write_update(
              [partition_key, {"key2", 1}],
              put: [{"field1", 1}, {"field2", 2}],
              condition: condition(:ignore)
            ),
            write_update(
              [partition_key, {"key2", 2}],
              put: [{"field1", 3}, {"field2", 4}],
              condition: condition(:ignore)
            ),
            write_delete(
              [partition_key, {"key2", 0}],
              condition: condition(:ignore)
            )
          ]
        },
        transaction_id: transaction_id,
        is_atomic: true
      )

    [table] = response.tables

    Enum.map(table.rows, fn(row) ->
      assert row.is_ok == true
    end)

    commit_transaction(transaction_id)

    {:ok, response} = get_row(@table_range, [partition_key, {"key2", 1}])
    {_pks, attrs} = response.row

    Enum.map(attrs, fn {key, value, _ts} ->
      case key do
        "field1" -> assert value == 1
        "field2" -> assert value == 2
        true -> :ok
      end
    end)

  end

  test "get row with transaction_id" do
    partition_key = {"key", "key1"}

    put_row(@table, [partition_key], [{"attr1", "1"}], condition: condition(:ignore))

    {:ok, response} = start_local_transaction(@table, partition_key)
    transaction_id = response.transaction_id

    {:ok, _response} = get_row(@table, [partition_key], transaction_id: transaction_id)

    {:error, error} =
      update_row(@table, [partition_key],
        put: [{"attr_1_v2", "attr_1_v2"}],
        condition: condition(:ignore)
      )

    assert error.code == "OTSRowOperationConflict"

    abort_transaction(transaction_id)
  end

  test "get range with transaction_id and batch write update" do
    for index <- 1..3 do
      put_row(@table_range, [{"key", "key1"}, {"key2", index}], [{"attr", "attr#{index}"}],
        condition: condition(:ignore)
      )
    end

    put_row(@table_range, [{"key", "key2"}, {"key2", 1}], [{"attr", "key2attr1"}],
      condition: condition(:ignore)
    )

    partition_key = {"key", "key1"}

    {:ok, response} = start_local_transaction(@table_range, partition_key)
    transaction_id = response.transaction_id

    {:ok, response} =
      get_range(
        @table_range,
        [{"key", "key1"}, {"key2", PKType.inf_min()}],
        [{"key", "key1"}, {"key2", PKType.inf_max()}],
        transaction_id: transaction_id
      )

    range_rows = response.rows

    rows_to_batch_write =
      Enum.map(range_rows, fn {pks, _attrs} ->
        write_update(pks, put: [{"new_added", true}], condition: condition(:ignore))
      end)

    batch_write({@table_range, rows_to_batch_write}, transaction_id: transaction_id)

    rows_to_batch_write_failed =
      Enum.map(range_rows, fn {pks, _attrs} ->
        write_update(pks, put: [{"new_added2", true}], condition: condition(:ignore))
      end)

    {:ok, response} = batch_write({@table_range, rows_to_batch_write_failed})

    Enum.map(response.tables, fn table_response ->
      Enum.map(table_response.rows, fn row_response ->
        assert row_response.error.code == "OTSRowOperationConflict"
      end)
    end)

    rows_to_batch_write2 =
      Enum.map(range_rows, fn {pks, _attrs} ->
        write_update(pks, put: [{"new_added2", "new_added2"}], condition: condition(:ignore))
      end)

    # can update multi times before commit this transaction
    batch_write({@table_range, rows_to_batch_write2}, transaction_id: transaction_id)

    commit_transaction(transaction_id)

    {:ok, response} =
      get_range(
        @table_range,
        [{"key", "key1"}, {"key2", PKType.inf_min()}],
        [{"key", "key1"}, {"key2", PKType.inf_max()}]
      )

    Enum.map(response.rows, fn {_pks, attrs} ->
      assert length(attrs) == 3
    end)
  end

  test "parallel write with local transaction" do
    {suc, fail} =
      [1, 2, 3]
      |> Task.async_stream(
        fn trace_id ->
          insert_with_transaction(trace_id, "1")
        end,
        timeout: :infinity
      )
      |> Enum.reduce({0, 0}, fn {:ok, result}, {suc_cnt, fail_cnt} ->
        case result do
          :ok ->
            {suc_cnt + 1, fail_cnt}

          {:error, error} ->
            assert error.code == "OTSRowOperationConflict"
            {suc_cnt, fail_cnt + 1}
        end
      end)

    assert suc == 1 and fail == 2
  end

  defp insert_with_transaction(trace_id, id) do
    partition_key = {"key", "test_pw_#{id}"}
    Logger.info("trace_id: #{trace_id} <> id: #{id}, pid: #{inspect(self())}")

    case start_local_transaction(@table, partition_key) do
      {:ok, response} ->
        transaction_id = response.transaction_id
        Logger.info("trace_id: #{trace_id} get transaction_id: #{transaction_id}")

        receive do
          :work_is_done -> :ok
        after
          1_000 ->
            # mock to do some calculation
            abort_transaction(transaction_id)
        end

        :ok

      {:error, error} ->
        Logger.error(
          "trace_id: #{trace_id} get parallel write failed with error: #{inspect(error)}"
        )

        {:error, error}
    end
  end
end
