defmodule ExAliyunOts.Client do

  use GenServer

  @request_timeout 60_000

  alias ExAliyunOts.Client.{Table, Row, Search, Transaction, Tunnel}

  alias ExAliyunOts.PlainBuffer

  import ExAliyunOts.Logger, only: [error: 1]

  defstruct [instance: nil]

  def start_link(instance), do: GenServer.start_link(__MODULE__, instance)

  def init(instance) do
    {:ok, %__MODULE__{instance: instance}}
  end

  def create_table(instance_key, var_create_table, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Table.request_to_create_table(var_create_table)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:create_table, encoded_request}, request_timeout)
  end

  def list_table(instance_key, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Table.request_to_list_table()
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:list_table, encoded_request}, request_timeout)
  end

  def delete_table(instance_key, table_name, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Table.request_to_delete_table(table_name)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:delete_table, encoded_request}, request_timeout)
  end

  def update_table(instance_key, var_update_table, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Table.request_to_update_table(var_update_table)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:update_table, encoded_request}, request_timeout)
  end

  def describe_table(instance_key, var_describe_table, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Table.request_to_describe_table(var_describe_table)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:describe_table, encoded_request}, request_timeout)
  end

  def put_row(instance_key, var_put_row, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Row.request_to_put_row(var_put_row)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:put_row, encoded_request}, request_timeout)
  end

  def get_row(instance_key, var_get_row, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Row.request_to_get_row(var_get_row)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:get_row, encoded_request}, request_timeout)
  end

  def update_row(instance_key, var_update_row, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Row.request_to_update_row(var_update_row)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:update_row, encoded_request}, request_timeout)
  end

  def delete_row(instance_key, var_delete_row, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Row.request_to_delete_row(var_delete_row)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:delete_row, encoded_request}, request_timeout)
  end

  def get_range(instance_key, var_get_range, next_start_primary_key \\ nil, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Row.request_to_get_range(var_get_range, next_start_primary_key)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:get_range, encoded_request}, request_timeout)
  end

  def iterate_get_all_range(instance_key, var_get_range, options \\ [request_timeout: :infinity]) do
    encoded_request = Row.request_to_get_range(var_get_range)
    request_timeout = Keyword.get(options, :request_timeout, :infinity)
    call_transaction(instance_key, {:iterate_get_all_range, encoded_request, var_get_range}, request_timeout)
  end

  def batch_get_row(instance_key, vars_batch_get_row, options \\ [request_timeout: :infinity]) when is_list(vars_batch_get_row) do
    encoded_request = Row.request_to_batch_get_row(vars_batch_get_row)
    request_timeout = Keyword.get(options, :request_timeout, :infinity)
    call_transaction(instance_key, {:batch_get_row, encoded_request}, request_timeout)
  end

  def batch_write_row(instance_key, var_batch_write_row, options \\ [transaction_id: nil, request_timeout: :infinity])
  def batch_write_row(instance_key, var_batch_write_row, request_timeout)
    when is_list(var_batch_write_row) and request_timeout == :infinity
    when is_list(var_batch_write_row) and is_integer(request_timeout) do
    encoded_request = Row.request_to_batch_write_row(var_batch_write_row, nil)
    call_transaction(instance_key, {:batch_write_row, encoded_request}, request_timeout)
  end
  def batch_write_row(instance_key, var_batch_write_row, options)
    when is_map(var_batch_write_row) and is_list(options)
    when is_list(var_batch_write_row) and is_list(options) do
    request_timeout = Keyword.get(options, :request_timeout, :infinity)
    transaction_id = Keyword.get(options, :transaction_id, nil)
    encoded_request = Row.request_to_batch_write_row(var_batch_write_row, transaction_id)
    call_transaction(instance_key, {:batch_write_row, encoded_request}, request_timeout)
  end

  def create_search_index(instance_key, var_create_search_index, options \\ [request_timeout: :infinity]) do
    encoded_request = Search.request_to_create_search_index(var_create_search_index)
    request_timeout = Keyword.get(options, :request_timeout, :infinity)
    call_transaction(instance_key, {:create_search_index, encoded_request}, request_timeout)
  end

  def search(instance_key, var_search_request, options \\ [request_timeout: :infinity]) do
    encoded_request = Search.request_to_search(var_search_request)
    request_timeout = Keyword.get(options, :request_timeout, :infinity)
    call_transaction(instance_key, {:search, encoded_request}, request_timeout)
  end

  def delete_search_index(instance_key, var_delete_search_index, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Search.request_to_delete_search_index(var_delete_search_index)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:delete_search_index, encoded_request}, request_timeout)
  end

  def list_search_index(instance_key, table_name, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Search.request_to_list_search_index(table_name)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:list_search_index, encoded_request}, request_timeout)
  end

  def describe_search_index(instance_key, var_describe_search_index, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Search.request_to_describe_search_index(var_describe_search_index)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:describe_search_index, encoded_request}, request_timeout)
  end

  def start_local_transaction(instance_key, var_start_local_transaction, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Transaction.request_to_start_local_transaction(var_start_local_transaction)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:start_local_transaction, encoded_request}, request_timeout)
  end

  def commit_transaction(instance_key, transaction_id, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Transaction.request_to_commit_transaction(transaction_id)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:commit_transaction, encoded_request}, request_timeout)
  end

  def abort_transaction(instance_key, transaction_id, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Transaction.request_to_abort_transaction(transaction_id)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:abort_transaction, encoded_request}, request_timeout)
  end

  def create_tunnel(instance_key, options) do
    encoded_request = Tunnel.request_to_create_tunnel(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:create_tunnel, encoded_request}, request_timeout)
  end

  def delete_tunnel(instance_key, options) do
    encoded_request = Tunnel.request_to_delete_tunnel(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:delete_tunnel, encoded_request}, request_timeout)
  end

  def list_tunnel(instance_key, table_name, options \\ [request_timeout: @request_timeout]) do
    encoded_request = Tunnel.request_to_list_tunnel(table_name)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:list_tunnel, encoded_request}, request_timeout)
  end

  def describe_tunnel(instance_key, options) do
    encoded_request = Tunnel.request_to_describe_tunnel(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:describe_tunnel, encoded_request}, request_timeout)
  end

  def connect_tunnel(instance_key, options) do
    encoded_request = Tunnel.request_to_connect_tunnel(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:connect_tunnel, encoded_request}, request_timeout)
  end

  def heartbeat(instance_key, options) do
    encoded_request = Tunnel.request_to_heartbeat(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:heartbeat, encoded_request}, request_timeout)
  end

  def shutdown_tunnel(instance_key, options) do
    encoded_request = Tunnel.request_to_shutdown(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:shutdown, encoded_request}, request_timeout)
  end

  def get_checkpoint(instance_key, options) do
    encoded_request = Tunnel.request_to_get_checkpoint(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:get_checkpoint, encoded_request}, request_timeout)
  end

  def read_records(instance_key, options) do
    encoded_request = Tunnel.request_to_readrecords(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:read_records, encoded_request}, request_timeout)
  end

  def checkpoint(instance_key, options) do
    encoded_request = Tunnel.request_to_checkpoint(options)
    request_timeout = Keyword.get(options, :request_timeout, @request_timeout)
    call_transaction(instance_key, {:checkpoint, encoded_request}, request_timeout)
  end

  def handle_call({:create_table, request_body}, _from, state) do
    result = Table.remote_create_table(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:list_table, request_body}, _from, state) do
    result = Table.remote_list_table(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:delete_table, request_body}, _from, state) do
    result = Table.remote_delete_table(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:update_table, request_body}, _from, state) do
    result = Table.remote_update_table(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:describe_table, request_body}, _from, state) do
    result = Table.remote_describe_table(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:put_row, request_body}, _from, state) do
    result = Row.remote_put_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:get_row, request_body}, _from, state) do
    result = Row.remote_get_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:update_row, request_body}, _from, state) do
    result = Row.remote_update_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:delete_row, request_body}, _from, state) do
    result = Row.remote_delete_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:get_range, request_body}, _from, state) do
    result = remote_get_range(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:iterate_get_all_range, request_body, var_get_range}, _from, state) do
    result = iterate_remote_get_range(state.instance, request_body, var_get_range, nil)
    {:reply, result, state}
  end
  def handle_call({:batch_get_row, request_body}, _from, state) do
    result = Row.remote_batch_get_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:batch_write_row, request_body}, _from, state) do
    result = Row.remote_batch_write_row(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:create_search_index, request_body}, _from, state) do
    result = Search.remote_create_search_index(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:search, request_body}, _from, state) do
    result = Search.remote_search(state.instance, request_body)
    prepared =
      case result do
        {:ok, search_result_response} ->
          {:ok, %{search_result_response | rows: decode_rows(search_result_response.rows)}}
        _ ->
          result
      end
    {:reply, prepared, state}
  end
  def handle_call({:delete_search_index, request_body}, _from, state) do
    result = Search.remote_delete_search_index(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:list_search_index, request_body}, _from, state) do
    result = Search.remote_list_search_index(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:describe_search_index, request_body}, _from, state) do
    result = Search.remote_describe_search_index(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:start_local_transaction, request_body}, _from, state) do
    result = Transaction.remote_start_local_transaction(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:commit_transaction, request_body}, _from, state) do
    result = Transaction.remote_commit_transaction(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:abort_transaction, request_body}, _from, state) do
    result = Transaction.remote_abort_transaction(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:create_tunnel, request_body}, _from, state) do
    result = Tunnel.remote_create_tunnel(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:delete_tunnel, request_body}, _from, state) do
    result = Tunnel.remote_delete_tunnel(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:list_tunnel, request_body}, _from, state) do
    result = Tunnel.remote_list_tunnel(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:describe_tunnel, request_body}, _from, state) do
    result = Tunnel.remote_describe_tunnel(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:connect_tunnel, request_body}, _from, state) do
    result = Tunnel.remote_connect_tunnel(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:heartbeat, request_body}, _from, state) do
    result = Tunnel.remote_heartbeat(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:shutdown, request_body}, _from, state) do
    result = Tunnel.remote_shutdown(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:get_checkpoint, request_body}, _from, state) do
    result = Tunnel.remote_get_checkpoint(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:read_records, request_body}, _from, state) do
    result = Tunnel.remote_readrecords(state.instance, request_body)
    {:reply, result, state}
  end
  def handle_call({:checkpoint, request_body}, _from, state) do
    result = Tunnel.remote_checkpoint(state.instance, request_body)
    {:reply, result, state}
  end

  defp call_transaction(instance_key, request, request_timeout) do
    :poolboy.transaction(
      instance_key,
      (fn(worker) -> 
        try do
          case GenServer.call(worker, request, request_timeout) do
            {:error, :timeout} ->
              call_transaction(instance_key, request, request_timeout)
            result ->
              result
          end
        catch
          :exit, {:timeout, _} ->
            {request_operation, _request_body} = request
            error(fn ->
              [
                "** ExAliyunOts occur timeout error when call ",
                inspect(request_operation),
                ", will retry it."
              ]
            end)
            call_transaction(instance_key, request, request_timeout)
          error ->
            error(fn ->
              [
                "** ExAliyunOts occur an unexpected error: ",
                inspect(error)
              ]
            end)
            error
        end
      end),
      :infinity
    )
  end

  defp remote_get_range(instance, request_body) do
    result = Row.remote_get_range(instance, request_body)
    case result do
      {:ok, get_range_response} ->
        {:ok, %{get_range_response | rows: decode_rows(get_range_response.rows)}}
      _ ->
        result
    end
  end

  defp iterate_remote_get_range(instance, request_body, var_get_range, summarized_response, next_start_primary_key \\ nil) do
    response_result =
      if next_start_primary_key == nil do
        Row.remote_get_range(instance, request_body)
      else
        request_body_with_next_start_primary_key = Row.request_to_get_range(var_get_range, next_start_primary_key)
        Row.remote_get_range(instance, request_body_with_next_start_primary_key)
      end

    case response_result do
      {:ok, get_range_response} ->
        new_next_start_primary_key = get_range_response.next_start_primary_key
        if new_next_start_primary_key == nil do
          iterated_response = sum_get_range_response(get_range_response, summarized_response)
          {:ok, %{iterated_response | rows: decode_rows(iterated_response.rows)}}
        else
          updated_summarized_response = sum_get_range_response(get_range_response, summarized_response)
          iterate_remote_get_range(instance, request_body, var_get_range, updated_summarized_response, new_next_start_primary_key)
        end
      _ ->
        response_result
    end
  end

  defp sum_get_range_response(response, nil) do
    %{response | rows: [response.rows]}
  end
  defp sum_get_range_response(response, summarized_response) do
    cu = response.consumed.capacity_unit
    consumed_read = cu.read
    consumed_write = cu.write
    rows = response.rows

    summarized_cu = summarized_response.consumed.capacity_unit
    updated_cu = %{summarized_cu | read: (summarized_cu.read + consumed_read), write: (summarized_cu.write + consumed_write)}
    updated_consumed = %{summarized_response.consumed | capacity_unit: updated_cu}

    summarized_response
    |> Map.put(:consumed, updated_consumed)
    |> Map.put(:rows, summarized_response.rows ++ [rows])
    |> Map.put(:next_start_primary_key, response.next_start_primary_key)
  end

  defp decode_rows(binary_rows) when is_bitstring(binary_rows) do
    PlainBuffer.deserialize_rows(binary_rows)
  end
  defp decode_rows(binary_rows_list) when is_list(binary_rows_list) do
    stream = Task.async_stream(binary_rows_list, fn(rows) ->
      PlainBuffer.deserialize_rows(rows)
    end, timeout: :infinity)
    Enum.reduce(stream, [], fn({:ok, readable_rows}, acc) ->
      acc ++ readable_rows
    end)
  end

end
