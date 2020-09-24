defmodule ExAliyunOts.Client do
  @moduledoc false

  alias ExAliyunOts.Client.{Table, Row, Search, Transaction, Tunnel}
  alias ExAliyunOts.{PlainBuffer, Config}

  def create_table(instance_key, var_create_table) do
    Table.remote_create_table(Config.get(instance_key), var_create_table)
  end

  def create_index(instance_key, var_create_index) do
    Table.remote_create_index(Config.get(instance_key), var_create_index)
  end

  def delete_index(instance_key, table_name, index_name) do
    Table.remote_delete_index(Config.get(instance_key), table_name, index_name)
  end

  def list_table(instance_key) do
    Table.remote_list_table(Config.get(instance_key))
  end

  def delete_table(instance_key, table_name) do
    Table.remote_delete_table(Config.get(instance_key), table_name)
  end

  def update_table(instance_key, var_update_table) do
    Table.remote_update_table(Config.get(instance_key), var_update_table)
  end

  def describe_table(instance_key, table_name) do
    Table.remote_describe_table(Config.get(instance_key), table_name)
  end

  def put_row(instance_key, var_put_row) do
    Row.remote_put_row(Config.get(instance_key), var_put_row)
  end

  def get_row(instance_key, var_get_row) do
    Row.remote_get_row(Config.get(instance_key), var_get_row)
  end

  def update_row(instance_key, var_update_row) do
    Row.remote_update_row(Config.get(instance_key), var_update_row)
  end

  def delete_row(instance_key, var_delete_row) do
    Row.remote_delete_row(Config.get(instance_key), var_delete_row)
  end

  def get_range(instance_key, var_get_range, next_start_primary_key \\ nil) do
    remote_get_range(Config.get(instance_key), var_get_range, next_start_primary_key)
  end

  def stream_range(instance_key, var_get_range) do
    remote_stream_range(Config.get(instance_key), var_get_range)
  end

  def iterate_get_all_range(instance_key, var_get_range) do
    iterate_remote_get_range(Config.get(instance_key), var_get_range)
  end

  def batch_get_row(instance_key, vars_batch_get_row) when is_list(vars_batch_get_row) do
    Row.remote_batch_get_row(Config.get(instance_key), vars_batch_get_row)
  end

  def batch_write_row(instance_key, var_batch_write_row, options \\ [transaction_id: nil])
      when is_map(var_batch_write_row) and is_list(options)
      when is_list(var_batch_write_row) and is_list(options) do
    transaction_id = Keyword.get(options, :transaction_id, nil)
    Row.remote_batch_write_row(Config.get(instance_key), var_batch_write_row, transaction_id)
  end

  def create_search_index(instance_key, var_create_search_index) do
    Search.remote_create_search_index(Config.get(instance_key), var_create_search_index)
  end

  def search(instance_key, var_search_request) do
    case Search.remote_search(Config.get(instance_key), var_search_request) do
      {:ok, search_result_response} ->
        {:ok, %{search_result_response | rows: decode_rows(search_result_response.rows)}}

      error_result ->
        error_result
    end
  end

  def delete_search_index(instance_key, var_delete_search_index) do
    Search.remote_delete_search_index(Config.get(instance_key), var_delete_search_index)
  end

  def list_search_index(instance_key, table_name) do
    Search.remote_list_search_index(Config.get(instance_key), table_name)
  end

  def describe_search_index(instance_key, var_describe_search_index) do
    Search.remote_describe_search_index(Config.get(instance_key), var_describe_search_index)
  end

  def start_local_transaction(instance_key, var_start_local_transaction) do
    Transaction.remote_start_local_transaction(Config.get(instance_key), var_start_local_transaction)
  end

  def commit_transaction(instance_key, transaction_id) do
    Transaction.remote_commit_transaction(Config.get(instance_key), transaction_id)
  end

  def abort_transaction(instance_key, transaction_id) do
    Transaction.remote_abort_transaction(Config.get(instance_key), transaction_id)
  end

  def create_tunnel(instance_key, options) do
    Tunnel.remote_create_tunnel(Config.get(instance_key), options)
  end

  def delete_tunnel(instance_key, options) do
    Tunnel.remote_delete_tunnel(Config.get(instance_key), options)
  end

  def list_tunnel(instance_key, table_name) do
    Tunnel.remote_list_tunnel(Config.get(instance_key), table_name)
  end

  def describe_tunnel(instance_key, options) do
    Tunnel.remote_describe_tunnel(Config.get(instance_key), options)
  end

  def connect_tunnel(instance_key, options) do
    Tunnel.remote_connect_tunnel(Config.get(instance_key), options)
  end

  def heartbeat(instance_key, options) do
    Tunnel.remote_heartbeat(Config.get(instance_key), options)
  end

  def shutdown_tunnel(instance_key, options) do
    Tunnel.remote_shutdown(Config.get(instance_key), options)
  end

  def get_checkpoint(instance_key, options) do
    Tunnel.remote_get_checkpoint(Config.get(instance_key), options)
  end

  def read_records(instance_key, options) do
    Tunnel.remote_readrecords(Config.get(instance_key), options)
  end

  def checkpoint(instance_key, options) do
    Tunnel.remote_checkpoint(Config.get(instance_key), options)
  end

  defp remote_get_range(instance, var_get_range, next_start_primary_key) do
    case Row.remote_get_range(instance, var_get_range, next_start_primary_key) do
      {:ok, get_range_response} ->
        {:ok, %{get_range_response | rows: decode_rows(get_range_response.rows)}}

      result ->
        result
    end
  end

  defp remote_stream_range(instance, var_get_range) do
    Stream.unfold("initialize", fn
      "initialize" ->
        response = Row.remote_get_range(instance, var_get_range)
        decode_rows_per_get_range_response(response)

      nil ->
        nil

      next_start_primary_key ->
        response = Row.remote_get_range(instance, var_get_range, next_start_primary_key)
        decode_rows_per_get_range_response(response)
    end)
  end

  defp iterate_remote_get_range(instance, var_get_range) do
    instance
    |> remote_stream_range(var_get_range)
    |> Enum.reduce(nil, fn
      {:ok, response}, nil ->
        response = merge_get_range_response(response, nil)
        {:ok, response}

      {:ok, response}, {:ok, acc} ->
        response = merge_get_range_response(response, acc)
        {:ok, response}

      {:error, _error} = response, _acc ->
        response
    end)
  end

  defp decode_rows_per_get_range_response(
         {:ok, %{next_start_primary_key: next_start_primary_key} = response}
       ) do
    {
      {:ok, %{response | rows: decode_rows(response.rows)}},
      next_start_primary_key
    }
  end

  defp decode_rows_per_get_range_response({:error, _error} = response) do
    {response, nil}
  end

  defp merge_get_range_response(response, nil) do
    %{response | rows: response.rows}
  end

  defp merge_get_range_response(
         response,
         %{consumed: consumed, rows: merged_rows} = merged_response
       ) do
    cu = response.consumed.capacity_unit
    consumed_read = cu.read
    consumed_write = cu.write
    rows = response.rows

    summarized_cu = consumed.capacity_unit

    updated_cu = %{
      summarized_cu
      | read: summarized_cu.read + consumed_read,
        write: summarized_cu.write + consumed_write
    }

    updated_consumed = %{consumed | capacity_unit: updated_cu}

    merged_response
    |> Map.put(:consumed, updated_consumed)
    |> Map.put(:rows, merged_rows ++ rows)
    |> Map.put(:next_start_primary_key, response.next_start_primary_key)
  end

  defp decode_rows(binary_rows) when is_bitstring(binary_rows) do
    PlainBuffer.deserialize_rows(binary_rows)
  end

  defp decode_rows(binary_rows_list) when is_list(binary_rows_list) do
    stream =
      Task.async_stream(
        binary_rows_list,
        fn rows ->
          PlainBuffer.deserialize_rows(rows)
        end,
        timeout: :infinity
      )

    Enum.reduce(stream, [], fn {:ok, readable_rows}, acc ->
      acc ++ readable_rows
    end)
  end
end
