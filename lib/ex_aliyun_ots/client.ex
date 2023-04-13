defmodule ExAliyunOts.Client do
  @moduledoc false

  alias ExAliyunOts.Client.{Table, Row, Search, Transaction, Tunnel, SQL}
  alias ExAliyunOts.{PlainBuffer, Config}

  def create_table(instance_key, var_create_table) do
    Table.remote_create_table(Config.get(instance_key), var_create_table)
  end

  def create_index(instance_key, table_name, index_name, primary_keys, defined_columns, options) do
    Table.remote_create_index(
      Config.get(instance_key),
      table_name,
      index_name,
      primary_keys,
      defined_columns,
      options
    )
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
    case Table.remote_describe_table(Config.get(instance_key), table_name) do
      {:ok, response} ->
        {:ok, %{response | shard_splits: decode_rows(response.shard_splits)}}

      error ->
        error
    end
  end

  def compute_split_points_by_size(instance_key, table_name, split_size) do
    encoded_request = Table.request_to_compute_split_points_by_size(table_name, split_size)

    case Table.remote_compute_split_points_by_size(Config.get(instance_key), encoded_request) do
      {:ok, response} ->
        {:ok, %{response | split_points: decode_rows(response.split_points)}}

      error ->
        error
    end
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

  def batch_write_row(instance_key, var_batch_write_row, options \\ [])
      when is_map(var_batch_write_row) and is_list(options)
      when is_list(var_batch_write_row) and is_list(options) do
    Row.remote_batch_write_row(Config.get(instance_key), var_batch_write_row, options)
  end

  def create_search_index(instance_key, var_create_search_index) do
    Search.remote_create_search_index(Config.get(instance_key), var_create_search_index)
  end

  def search(instance_key, var_search_request) do
    remote_search(Config.get(instance_key), var_search_request)
  end

  def stream_search(instance_key, var_search_request) do
    remote_stream_search(Config.get(instance_key), var_search_request)
  end

  def iterate_search(instance_key, var_search_request) do
    Config.get(instance_key)
    |> remote_stream_search(var_search_request)
    |> Enum.reduce(nil, fn
      {:ok, response}, nil ->
        {:ok, response}

      {:ok, response}, {:ok, acc} ->
        response = merge_search_response(response, acc)
        {:ok, response}

      {:error, _error} = response, _acc ->
        response
    end)
  end

  def compute_splits(instance_key, table_name, index_name) do
    encoded_request = Search.request_to_compute_splits(table_name, index_name)
    Search.remote_compute_splits(Config.get(instance_key), encoded_request)
  end

  def parallel_scan(instance_key, var_scan_request) do
    encoded_request = Search.request_to_parallel_scan(var_scan_request)

    case Search.remote_parallel_scan(Config.get(instance_key), encoded_request) do
      {:ok, response} ->
        {:ok, %{response | rows: decode_rows(response.rows)}}

      error ->
        error
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
    Transaction.remote_start_local_transaction(
      Config.get(instance_key),
      var_start_local_transaction
    )
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

  def sql_query(instance_key, query) do
    case SQL.remote_sql_query(Config.get(instance_key), query) do
      {:ok, response} ->
        {:ok, %{response | rows: decode_rows(response.rows)}}

      error ->
        error
    end
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
        {:ok, response}

      {:ok, response}, {:ok, acc} ->
        response = merge_get_range_response(response, acc)
        {:ok, response}

      {:error, _error} = response, _acc ->
        response
    end)
  end

  defp remote_search(instance, var_search_request) do
    case Search.remote_search(instance, var_search_request) do
      {:ok, response} ->
        {:ok, %{response | rows: decode_rows(response.rows)}}

      error ->
        error
    end
  end

  defp remote_stream_search(instance, var_search_request) do
    # remove [sort, collapse, ...]
    # can't set [sort, collapse] when token is not null
    next_var_search_request = %{
      var_search_request
      | search_query: %{var_search_request.search_query | sort: nil, collapse: nil}
    }

    Stream.unfold(
      var_search_request.search_query.token || :init,
      fn
        nil ->
          nil

        :init ->
          do_remote_stream_search(instance, var_search_request)

        token ->
          next_var_search_request = %{
            next_var_search_request
            | search_query: %{next_var_search_request.search_query | token: token}
          }

          do_remote_stream_search(instance, next_var_search_request)
      end
    )
  end

  defp do_remote_stream_search(instance, var_search_request) do
    case remote_search(instance, var_search_request) do
      {:ok, %{next_token: token}} = result -> {result, token}
      error -> {error, nil}
    end
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

  defp merge_get_range_response(
         response,
         %{consumed: consumed, rows: merged_rows} = merged_response
       ) do
    cu = response.consumed.capacity_unit
    summarized_cu = consumed.capacity_unit

    updated_cu = %{
      summarized_cu
      | read: summarized_cu.read + cu.read,
        write: summarized_cu.write + cu.write
    }

    updated_consumed = %{consumed | capacity_unit: updated_cu}

    Map.merge(merged_response, %{
      consumed: updated_consumed,
      rows: merged_rows ++ response.rows,
      next_start_primary_key: response.next_start_primary_key
    })
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

  defp merge_search_response(response, merged_response) do
    # not merge total_hits / is_all_succeeded / aggs / group_bys
    %{
      merged_response
      | rows: merged_response.rows ++ response.rows,
        next_token: response.next_token
    }
  end
end
