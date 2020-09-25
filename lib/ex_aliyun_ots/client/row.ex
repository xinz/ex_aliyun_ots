defmodule ExAliyunOts.Client.Row do
  @moduledoc false

  import ExAliyunOts.Logger, only: [debug: 1]

  alias ExAliyunOts.TableStore.{
    PutRowRequest,
    PutRowResponse,
    ReturnContent,
    TimeRange,
    GetRowRequest,
    GetRowResponse,
    UpdateRowRequest,
    UpdateRowResponse,
    GetRangeRequest,
    GetRangeResponse,
    BatchGetRowRequest,
    TableInBatchGetRowRequest,
    BatchGetRowResponse,
    BatchWriteRowRequest,
    TableInBatchWriteRowRequest,
    RowInBatchWriteRowRequest,
    BatchWriteRowResponse,
    DeleteRowRequest,
    DeleteRowResponse
  }

  alias ExAliyunOts.TableStoreFilter.{
    Filter,
    ColumnPaginationFilter,
    CompositeColumnValueFilter,
    SingleColumnValueFilter
  }

  alias ExAliyunOts.{PlainBuffer, Var, Http}
  alias ExAliyunOts.Const.{OperationType, ReturnType}

  require OperationType
  require ReturnType

  @batch_write_limit_per_request 200

  defp request_to_put_row(var_put_row) do
    proto_condition = Map.update!(var_put_row.condition, :column_condition, &filter_to_bytes/1)

    serialized_row =
      PlainBuffer.serialize_for_put_row(var_put_row.primary_keys, var_put_row.attribute_columns)

    [
      table_name: var_put_row.table_name,
      row: serialized_row,
      condition: proto_condition,
      transaction_id: var_put_row.transaction_id
    ]
    |> PutRowRequest.new()
    |> map_return_content(var_put_row.return_type, nil)
    |> PutRowRequest.encode()
  end

  def remote_put_row(instance, var_put_row) do
    request_body = request_to_put_row(var_put_row)

    result =
      instance
      |> Http.client("/PutRow", request_body, &PutRowResponse.decode/1)
      |> Http.post()

    debug(fn -> ["put_row result: ", inspect(result)] end)

    result
  end

  defp request_to_get_row(var_get_row) do
    primary_keys = PlainBuffer.serialize_primary_keys(var_get_row.primary_keys)
    filter = filter_to_bytes(var_get_row.filter)

    get_row_request =
      GetRowRequest.new(
        table_name: var_get_row.table_name,
        primary_key: primary_keys,
        columns_to_get: var_get_row.columns_to_get,
        filter: filter,
        start_column: var_get_row.start_column,
        end_column: var_get_row.end_column,
        transaction_id: var_get_row.transaction_id
      )

    parameter_time_range = var_get_row.time_range

    get_row_request =
      case parameter_time_range do
        %Var.TimeRange{} ->
          time_range = prepare_time_range(parameter_time_range)
          Map.put(get_row_request, :time_range, time_range)

        nil ->
          Map.put(get_row_request, :max_versions, var_get_row.max_versions)
      end

    GetRowRequest.encode(get_row_request)
  end

  def remote_get_row(instance, var_get_row) do
    request_body = request_to_get_row(var_get_row)

    result =
      instance
      |> Http.client("/GetRow", request_body, &GetRowResponse.decode/1)
      |> Http.post()

    debug(fn -> ["get_row result: ", inspect(result)] end)

    result
  end

  defp request_to_update_row(var_update_row) do
    serialized_row =
      PlainBuffer.serialize_for_update_row(var_update_row.primary_keys, var_update_row.updates)

    proto_condition = Map.update!(var_update_row.condition, :column_condition, &filter_to_bytes/1)

    [
      table_name: var_update_row.table_name,
      row_change: serialized_row,
      condition: proto_condition,
      transaction_id: var_update_row.transaction_id
    ]
    |> UpdateRowRequest.new()
    |> map_return_content(var_update_row.return_type, var_update_row.return_columns)
    |> UpdateRowRequest.encode()
  end

  def remote_update_row(instance, var_update_row) do
    request_body = request_to_update_row(var_update_row)

    result =
      instance
      |> Http.client("/UpdateRow", request_body, &UpdateRowResponse.decode/1)
      |> Http.post()

    debug(fn -> ["update_row result: ", inspect(result)] end)

    result
  end

  defp request_to_delete_row(var_delete_row) do
    serialized_primary_keys = PlainBuffer.serialize_for_delete_row(var_delete_row.primary_keys)

    proto_condition = Map.update!(var_delete_row.condition, :column_condition, &filter_to_bytes/1)

    [
      table_name: var_delete_row.table_name,
      primary_key: serialized_primary_keys,
      condition: proto_condition,
      transaction_id: var_delete_row.transaction_id
    ]
    |> DeleteRowRequest.new()
    |> map_return_content(var_delete_row.return_type, nil)
    |> DeleteRowRequest.encode()
  end

  def remote_delete_row(instance, var_delete_row) do
    request_body = request_to_delete_row(var_delete_row)

    result =
      instance
      |> Http.client("/DeleteRow", request_body, &DeleteRowResponse.decode/1)
      |> Http.post()

    debug(fn -> ["delete_row result: ", inspect(result)] end)

    result
  end

  defp request_to_get_range(var_get_range, next_start_primary_key) do
    parameter_time_range = var_get_range.time_range

    prepared_inclusive_start_primary_keys =
      if next_start_primary_key == nil do
        PlainBuffer.serialize_primary_keys(var_get_range.inclusive_start_primary_keys)
      else
        next_start_primary_key
      end

    prepared_exclusive_end_primary_keys =
      PlainBuffer.serialize_primary_keys(var_get_range.exclusive_end_primary_keys)

    filter = filter_to_bytes(var_get_range.filter)

    get_range_request =
      GetRangeRequest.new(
        table_name: var_get_range.table_name,
        direction: var_get_range.direction,
        columns_to_get: var_get_range.columns_to_get,
        limit: var_get_range.limit,
        inclusive_start_primary_key: prepared_inclusive_start_primary_keys,
        exclusive_end_primary_key: prepared_exclusive_end_primary_keys,
        filter: filter,
        start_column: var_get_range.start_column,
        end_column: var_get_range.end_column,
        transaction_id: var_get_range.transaction_id
      )

    get_range_request =
      case parameter_time_range do
        %Var.TimeRange{} ->
          time_range = prepare_time_range(parameter_time_range)
          Map.put(get_range_request, :time_range, time_range)

        nil ->
          Map.put(get_range_request, :max_versions, var_get_range.max_versions)
      end

    GetRangeRequest.encode(get_range_request)
  end

  def remote_get_range(instance, var_get_range, next_start_primary_key \\ nil) do
    request_body = request_to_get_range(var_get_range, next_start_primary_key)

    result =
      instance
      |> Http.client("/GetRange", request_body, &GetRangeResponse.decode/1)
      |> Http.post()

    debug(fn -> ["get_range result: ", inspect(result)] end)

    result
  end

  defp request_to_batch_get_row(vars_batch_get_row) do
    stream =
      Task.async_stream(
        vars_batch_get_row,
        fn var_get_range ->
          do_request_to_batch_get_row(var_get_range)
        end,
        timeout: :infinity
      )

    encoded_tables = Enum.map(stream, fn {:ok, request} -> request end)
    request = BatchGetRowRequest.new(tables: encoded_tables)
    BatchGetRowRequest.encode(request)
  end

  defp do_request_to_batch_get_row(var_batch_get_row) do
    bytes_primary_keys = pks_to_batch_get_row(var_batch_get_row.primary_keys)
    filter = filter_to_bytes(var_batch_get_row.filter)

    batch_get_row_request =
      TableInBatchGetRowRequest.new(
        table_name: var_batch_get_row.table_name,
        primary_key: bytes_primary_keys,
        columns_to_get: var_batch_get_row.columns_to_get,
        filter: filter,
        start_column: var_batch_get_row.start_column,
        end_column: var_batch_get_row.end_column
      )

    parameter_time_range = var_batch_get_row.time_range

    case parameter_time_range do
      %Var.TimeRange{} ->
        time_range = prepare_time_range(parameter_time_range)
        Map.put(batch_get_row_request, :time_range, time_range)

      nil ->
        Map.put(batch_get_row_request, :max_versions, var_batch_get_row.max_versions)
    end
  end

  defp pks_to_batch_get_row(primary_keys) when length(primary_keys) == 1 do
    [PlainBuffer.serialize_primary_keys(primary_keys)]
  end

  defp pks_to_batch_get_row(primary_keys) when length(primary_keys) > 1 do
    primary_keys
    |> Task.async_stream(
      fn primary_keys_query_group ->
        if is_list(primary_keys_query_group) do
          PlainBuffer.serialize_primary_keys(primary_keys_query_group)
        else
          raise ExAliyunOts.RuntimeError,
                "Invalid primary_keys group #{inspect(primary_keys_query_group)}, expect it as list"
        end
      end,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, bytes_primary_keys} -> bytes_primary_keys end)
  end

  defp pks_to_batch_get_row(primary_keys) do
    raise ExAliyunOts.RuntimeError,
          "Invalid primary_keys #{inspect(primary_keys)}, expect it as list"
  end

  def remote_batch_get_row(instance, vars_batch_get_row) do
    request_body = request_to_batch_get_row(vars_batch_get_row)

    result =
      instance
      |> Http.client("/BatchGetRow", request_body, fn response_body ->
        decoded = BatchGetRowResponse.decode(response_body)
        readable_tables = decode_rows_from_batch_get_row(decoded.tables)
        %{decoded | tables: readable_tables}
      end)
      |> Http.post()

    debug(fn -> ["batch_get_row result: ", inspect(result)] end)

    result
  end

  defp decode_rows_from_batch_get_row(tables) do
    tables
    |> Task.async_stream(
      fn table_in_batch_get_row_response ->
        readable_rows =
          Enum.map(table_in_batch_get_row_response.rows, fn row_in_batch_get_row_response ->
            decode_row_from_batch(row_in_batch_get_row_response)
          end)

        %{table_in_batch_get_row_response | rows: readable_rows}
      end,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, response} -> response end)
  end

  defp request_to_batch_write_row(vars_batch_write_row, nil) when is_list(vars_batch_write_row) do
    # BatchWriteRow for multi tables with local transaction are not supported.
    tables =
      Enum.map(vars_batch_write_row, fn var_batch_write_row ->
        map_table_in_batch_write_row_request(var_batch_write_row)
      end)

    request = BatchWriteRowRequest.new(tables: tables)
    BatchWriteRowRequest.encode(request)
  end

  defp request_to_batch_write_row([var_batch_write_row], transaction_id)
       when is_map(var_batch_write_row) do
    request_to_batch_write_row(var_batch_write_row, transaction_id)
  end

  defp request_to_batch_write_row(var_batch_write_row, transaction_id)
       when is_map(var_batch_write_row) do
    table = map_table_in_batch_write_row_request(var_batch_write_row)
    request = BatchWriteRowRequest.new(tables: [table], transaction_id: transaction_id)
    BatchWriteRowRequest.encode(request)
  end

  defp map_table_in_batch_write_row_request(var_batch_write_row) do
    rows = var_batch_write_row.rows

    if length(rows) > @batch_write_limit_per_request,
      do:
        raise(
          ExAliyunOts.RuntimeError,
          "The number of rows in BatchWriteRow exceeds the maximun #{
            @batch_write_limit_per_request
          } limit"
        )

    encoded_rows =
      rows
      |> Task.async_stream(
        fn var_row_in_request ->
          do_request_to_batch_write_row(var_row_in_request)
        end,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, request} -> request end)

    TableInBatchWriteRowRequest.new(
      table_name: var_batch_write_row.table_name,
      rows: encoded_rows
    )
  end

  defp do_request_to_batch_write_row(var_row_in_request) do
    proto_condition =
      Map.update!(var_row_in_request.condition, :column_condition, &filter_to_bytes/1)

    type = var_row_in_request.type

    serialized_row =
      case type do
        OperationType.update() ->
          PlainBuffer.serialize_for_update_row(
            var_row_in_request.primary_keys,
            var_row_in_request.updates
          )

        OperationType.put() ->
          PlainBuffer.serialize_for_put_row(
            var_row_in_request.primary_keys,
            var_row_in_request.updates
          )

        OperationType.delete() ->
          PlainBuffer.serialize_for_delete_row(var_row_in_request.primary_keys)

        _ ->
          raise ExAliyunOts.RuntimeError,
                "Invalid OperationType: #{inspect(type)}, please ensure the operation type of BatchWriteRow as [UPDATE, PUT, DELETE]"
      end

    [type: var_row_in_request.type, row_change: serialized_row, condition: proto_condition]
    |> RowInBatchWriteRowRequest.new()
    |> map_return_content(var_row_in_request.return_type, var_row_in_request.return_columns)
  end

  def remote_batch_write_row(instance, var_batch_write_row, transaction_id) do
    request_body = request_to_batch_write_row(var_batch_write_row, transaction_id)

    result =
      instance
      |> Http.client("/BatchWriteRow", request_body, fn response_body ->
        decoded = BatchWriteRowResponse.decode(response_body)
        readable_tables = decode_row_from_batch_write_row(decoded.tables)
        %{decoded | tables: readable_tables}
      end)
      |> Http.post()

    debug(fn ->
      [
        "batch_write_row result: ",
        inspect(result)
      ]
    end)

    result
  end

  defp decode_row_from_batch_write_row(tables) do
    tables
    |> Task.async_stream(
      fn table_in_batch_write_row_response ->
        readable_rows =
          Enum.map(table_in_batch_write_row_response.rows, fn row_in_batch_write_row_response ->
            decode_row_from_batch(row_in_batch_write_row_response)
          end)

        %{table_in_batch_write_row_response | rows: readable_rows}
      end,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, response} -> response end)
  end

  defp filter_to_bytes(nil) do
    nil
  end

  defp filter_to_bytes(
         %Var.Filter{filter: %Var.CompositeColumnValueFilter{}, filter_type: filter_type} =
           var_filter
       ) do
    encoded_filter = var_filter |> filter_to_protobuf() |> CompositeColumnValueFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end

  defp filter_to_bytes(
         %Var.Filter{filter: %Var.ColumnPaginationFilter{}, filter_type: filter_type} = var_filter
       ) do
    encoded_filter = var_filter |> filter_to_protobuf() |> ColumnPaginationFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end

  defp filter_to_bytes(
         %Var.Filter{filter: %Var.SingleColumnValueFilter{}, filter_type: filter_type} =
           var_filter
       ) do
    encoded_filter = var_filter |> filter_to_protobuf() |> SingleColumnValueFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end

  defp filter_to_bytes(%Var.Filter{filter: invalid_filter}) do
    raise ExAliyunOts.RuntimeError, "Not supported filter: #{inspect(invalid_filter)}"
  end

  defp filter_to_protobuf(
         %Var.Filter{filter: filter, filter_type: filter_type},
         is_sub_filter \\ false
       ) do
    case filter do
      %Var.CompositeColumnValueFilter{} ->
        prepared_sub_filters =
          Enum.map(filter.sub_filters, fn sub_filter ->
            filter_to_protobuf(sub_filter, true)
          end)

        if prepared_sub_filters == [],
          do:
            raise(
              ExAliyunOts.RuntimeError,
              "Invalid filter for CompositeColumnValueFilter: #{inspect(filter)}"
            )

        proto_filter =
          CompositeColumnValueFilter.new(
            combinator: filter.combinator,
            sub_filters: prepared_sub_filters
          )

        if is_sub_filter do
          Filter.new(type: filter_type, filter: CompositeColumnValueFilter.encode(proto_filter))
        else
          proto_filter
        end

      %Var.ColumnPaginationFilter{} ->
        proto_filter = ColumnPaginationFilter.new(offset: filter.offset, limit: filter.limit)

        if is_sub_filter do
          Filter.new(type: filter_type, filter: ColumnPaginationFilter.encode(proto_filter))
        else
          proto_filter
        end

      %Var.SingleColumnValueFilter{} ->
        column_value = PlainBuffer.serialize_column_value(filter.column_value)

        proto_filter =
          SingleColumnValueFilter.new(
            comparator: filter.comparator,
            column_name: filter.column_name,
            column_value: column_value,
            filter_if_missing: not filter.ignore_if_missing,
            latest_version_only: filter.latest_version_only
          )

        if is_sub_filter do
          Filter.new(type: filter_type, filter: SingleColumnValueFilter.encode(proto_filter))
        else
          proto_filter
        end
    end
  end

  defp prepare_time_range(%Var.TimeRange{
         start_time: start_time,
         end_time: end_time,
         specific_time: specific_time
       }) do
    cond do
      is_integer(start_time) and is_integer(end_time) ->
        TimeRange.new(start_time: start_time, end_time: end_time)

      is_integer(specific_time) ->
        TimeRange.new(specific_time: specific_time)

      true ->
        raise ExAliyunOts.RuntimeError,
              "Invalid time_range, start_time: #{inspect(start_time)}, end_time: #{
                inspect(end_time)
              }, specific: #{inspect(specific_time)}"
    end
  end

  defp decode_row_from_batch(row_in_batch_response) do
    if row_in_batch_response.is_ok and row_in_batch_response.row != nil do
      readable_row = PlainBuffer.deserialize_row(row_in_batch_response.row)
      %{row_in_batch_response | row: readable_row}
    else
      row_in_batch_response
    end
  end

  defp map_return_content(request, ReturnType.pk() = return_type, _return_columns) do
    Map.put(request, :return_content, ReturnContent.new(return_type: return_type))
  end

  defp map_return_content(request, ReturnType.after_modify() = return_type, return_columns)
       when length(return_columns) > 0 do
    Map.put(
      request,
      :return_content,
      ReturnContent.new(return_type: return_type, return_column_names: return_columns)
    )
  end

  defp map_return_content(request, _return_type, _return_columns) do
    request
  end
end
