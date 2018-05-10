defmodule ExAliyunOts.Client.Row do
  require Logger

  alias ExAliyunOts.TableStore.{PutRowRequest, Condition, PutRowResponse, ReturnContent,
    TimeRange, GetRowRequest, GetRowResponse, UpdateRowRequest, UpdateRowResponse, GetRangeRequest,
    GetRangeResponse, BatchGetRowRequest, TableInBatchGetRowRequest, BatchGetRowResponse, BatchWriteRowRequest,
    TableInBatchWriteRowRequest, RowInBatchWriteRowRequest, BatchWriteRowResponse, DeleteRowRequest, DeleteRowResponse}

  alias ExAliyunOts.TableStoreFilter.{Filter, ColumnPaginationFilter, CompositeColumnValueFilter,
    SingleColumnValueFilter}

  alias ExAliyunOts.{PlainBuffer, Var, Http}
  alias ExAliyunOts.Const.{OperationType, ReturnType, RowExistence}

  require OperationType
  require ReturnType
  require RowExistence

  @batch_write_limit_per_request 200

  def request_to_put_row(var_put_row) do
    %Var.Condition{row_existence: row_existence, column_condition: column_condition} = var_put_row.condition
    if row_existence not in RowExistence.supported do
      raise ExAliyunOts.Error, "Invalid row_existence: #{inspect row_existence}"
    end
    column_condition = filter_to_bytes(column_condition)
    proto_condition = Condition.new(row_existence: row_existence, column_condition: column_condition)
    serialized_row = PlainBuffer.serialize_for_put_row(var_put_row.primary_keys, var_put_row.attribute_columns)
    put_row_request = PutRowRequest.new(table_name: var_put_row.table_name, row: serialized_row, condition: proto_condition)
    put_row_request = 
      if var_put_row.return_type != ReturnType.none do
        return_content = ReturnContent.new(return_type: var_put_row.return_type)
        Map.put(put_row_request, :return_content, return_content)
      else
        put_row_request
      end
    PutRowRequest.encode(put_row_request)
  end

  def remote_put_row(instance, request_body) do
    result =
      instance
      |> Http.client("/PutRow", request_body, &PutRowResponse.decode/1)
      |> Http.post()
    Logger.debug(fn -> "put_row result: #{inspect result}" end)
    result
  end

  def request_to_get_row(var_get_row) do
    primary_keys = PlainBuffer.serialize_primary_keys(var_get_row.primary_keys)
    filter = filter_to_bytes(var_get_row.filter)

    get_row_request = GetRowRequest.new(
      table_name: var_get_row.table_name,
      primary_key: primary_keys,
      columns_to_get: var_get_row.columns_to_get,
      filter: filter,
      start_column: var_get_row.start_column,
      end_column: var_get_row.end_column
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

  def remote_get_row(instance, request_body) do
    result =
      instance
      |> Http.client("/GetRow", request_body, &GetRowResponse.decode/1)
      |> Http.post()
    Logger.debug(fn -> "get_row result: #{inspect result}" end)
    result
  end

  def request_to_update_row(var_update_row) do
    serialized_row = PlainBuffer.serialize_for_update_row(var_update_row.primary_keys, var_update_row.updates)
    %Var.Condition{row_existence: row_existence, column_condition: column_condition} = var_update_row.condition
    if row_existence not in RowExistence.supported do
      raise ExAliyunOts.Error, "Invalid row_existence: #{inspect row_existence}"
    end
    column_condition = filter_to_bytes(column_condition)
    proto_condition = Condition.new(row_existence: row_existence, column_condition: column_condition)
    update_row_request = UpdateRowRequest.new(table_name: var_update_row.table_name, row_change: serialized_row, condition: proto_condition)
    update_row_request =
      if var_update_row.return_type != ReturnType.none do
        return_content = ReturnContent.new(return_type: var_update_row.return_type)
        Map.put(update_row_request, :return_content, return_content)
      else
        update_row_request
      end
    UpdateRowRequest.encode(update_row_request)
  end

  def remote_update_row(instance, request_body) do
    result =
      instance
      |> Http.client("/UpdateRow", request_body, &UpdateRowResponse.decode/1)
      |> Http.post()
    Logger.debug(fn -> "update_row result: #{inspect result}" end)
    result
  end

  def request_to_delete_row(var_delete_row) do
    serialized_primary_keys = PlainBuffer.serialize_for_delete_row(var_delete_row.primary_keys)
    %Var.Condition{row_existence: row_existence, column_condition: column_condition} = var_delete_row.condition
    if row_existence not in RowExistence.supported do
      raise ExAliyunOts.Error, "Invalid row_existence: #{inspect row_existence}"
    end
    column_condition = filter_to_bytes(column_condition)
    proto_condition = Condition.new(row_existence: row_existence, column_condition: column_condition)
    delete_row_request = DeleteRowRequest.new(table_name: var_delete_row.table_name, primary_key: serialized_primary_keys, condition: proto_condition)
    delete_row_request =
      if var_delete_row.return_type != ReturnType.none do
        return_content = ReturnContent.new(return_type: var_delete_row.return_type)
        Map.put(delete_row_request, :return_content, return_content)
      else
        delete_row_request
      end
    DeleteRowRequest.encode(delete_row_request)
  end

  def remote_delete_row(instance, request_body) do
    result =
      instance
      |> Http.client("/DeleteRow", request_body, &DeleteRowResponse.decode/1)
      |> Http.post()
    Logger.debug(fn -> "delete_row result: #{inspect result}" end)
    result
  end

  def request_to_get_range(var_get_range, next_start_primary_key \\ nil) do
    parameter_time_range = var_get_range.time_range

    prepared_inclusive_start_primary_keys =
      if next_start_primary_key == nil do
        PlainBuffer.serialize_primary_keys(var_get_range.inclusive_start_primary_keys)
      else
        next_start_primary_key
      end
    prepared_exclusive_end_primary_keys = PlainBuffer.serialize_primary_keys(var_get_range.exclusive_end_primary_keys)

    filter = filter_to_bytes(var_get_range.filter)

    get_range_request = GetRangeRequest.new(
      table_name: var_get_range.table_name,
      direction: var_get_range.direction,
      columns_to_get: var_get_range.columns_to_get,
      limit: var_get_range.limit,
      inclusive_start_primary_key: prepared_inclusive_start_primary_keys,
      exclusive_end_primary_key: prepared_exclusive_end_primary_keys,
      filter: filter,
      start_column: var_get_range.start_column,
      end_column: var_get_range.end_column
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

  def remote_get_range(instance, request_body) do
    result =
      instance
      |> Http.client("/GetRange", request_body, &GetRangeResponse.decode/1)
      |> Http.post()
    Logger.debug(fn -> "get_range result: #{inspect result}" end)
    result
  end

  def request_to_batch_get_row(vars_batch_get_row) do
    stream = Task.async_stream(vars_batch_get_row, fn(var_get_range) ->
      do_request_to_batch_get_row(var_get_range)
    end, timeout: :infinity)
    encoded_tables = Enum.map(stream, fn({:ok, request}) -> request end)
    request = BatchGetRowRequest.new(tables: encoded_tables)
    BatchGetRowRequest.encode(request)
  end

  defp do_request_to_batch_get_row(var_batch_get_row) do
    bytes_primary_keys = pks_to_batch_get_row(var_batch_get_row.primary_keys)
    filter = filter_to_bytes(var_batch_get_row.filter)

    batch_get_row_request = TableInBatchGetRowRequest.new(
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
    |> Task.async_stream(fn(primary_keys_query_group) ->
      if is_list(primary_keys_query_group) do
        PlainBuffer.serialize_primary_keys(primary_keys_query_group)
      else
        raise ExAliyunOts.Error, "Invalid primary_keys group #{inspect primary_keys_query_group}, expect it as list"
      end
    end, timeout: :infinity)
    |> Enum.map(fn({:ok, bytes_primary_keys}) -> bytes_primary_keys end)
  end
  defp pks_to_batch_get_row(primary_keys) do
    raise ExAliyunOts.Error, "Invalid primary_keys #{inspect primary_keys}, expect it as list"
  end

  def remote_batch_get_row(instance, request_body) do
    result =
      instance
      |> Http.client("/BatchGetRow", request_body, fn(response_body) ->
        decoded = BatchGetRowResponse.decode(response_body)
        readable_tables = decode_rows_from_batch_get_row(decoded.tables)
        %{decoded | tables: readable_tables}
      end)
      |> Http.post()
    Logger.debug(fn -> "batch_get_row result: #{inspect result}" end)
    result
  end

  defp decode_rows_from_batch_get_row(tables) do
    tables
    |> Task.async_stream(fn(table_in_batch_get_row_response) ->
      readable_rows =
        Enum.map(table_in_batch_get_row_response.rows, fn(row_in_batch_get_row_response) ->
          decode_row_from_batch(row_in_batch_get_row_response)
        end)
      %{table_in_batch_get_row_response | rows: readable_rows}
    end, timeout: :infinity)
    |> Enum.map(fn({:ok, response}) -> response end)
  end

  def request_to_batch_write_row(vars_batch_write_row) do
    tables =
      Enum.map(vars_batch_write_row, fn(var_batch_write_row) ->
        rows = var_batch_write_row.rows
        if length(rows) > @batch_write_limit_per_request, do: raise ExAliyunOts.Error, "The number of rows in BatchWriteRow exceeds the maximun #{@batch_write_limit_per_request} limit"
        stream = Task.async_stream(var_batch_write_row.rows, fn(var_row_in_request) ->
          do_request_to_batch_write_row(var_row_in_request)
        end, timeout: :infinity)
        encoded_rows = Enum.map(stream, fn({:ok, request}) -> request end)
        table_name = var_batch_write_row.table_name
        TableInBatchWriteRowRequest.new(
          table_name: table_name,
          rows: encoded_rows
        )
      end)
    request = BatchWriteRowRequest.new(tables: tables)
    BatchWriteRowRequest.encode(request)
  end

  defp do_request_to_batch_write_row(var_row_in_request) do
    %Var.Condition{row_existence: row_existence, column_condition: column_condition} = var_row_in_request.condition
    if row_existence not in RowExistence.supported do
      raise ExAliyunOts.Error, "Invalid row_existence: #{inspect row_existence}"
    end
    column_condition = filter_to_bytes(column_condition)
    proto_condition = Condition.new(row_existence: row_existence, column_condition: column_condition)
    type = var_row_in_request.type
    serialized_row =
      case type do
        OperationType.update ->
          PlainBuffer.serialize_for_update_row(var_row_in_request.primary_keys, var_row_in_request.updates)
        OperationType.put ->
          PlainBuffer.serialize_for_put_row(var_row_in_request.primary_keys, var_row_in_request.updates)
        OperationType.delete ->
          PlainBuffer.serialize_for_delete_row(var_row_in_request.primary_keys)
        _ ->
          raise ExAliyunOts.Error, "Invalid OperationType: #{inspect type}, please ensure the operation type of BatchWriteRow as [UPDATE, PUT, DELETE]"
      end

    row_in_batch_write_row_request = RowInBatchWriteRowRequest.new(
      type: var_row_in_request.type,
      row_change: serialized_row,
      condition: proto_condition
    )
    var_return_type = var_row_in_request.return_type
    if var_return_type != ReturnType.none do
      return_content = ReturnContent.new(return_type: var_return_type)
      Map.put(row_in_batch_write_row_request, :return_content, return_content)
    else
      row_in_batch_write_row_request
    end
  end

  def remote_batch_write_row(instance, request_body) do
    result =
      instance
      |> Http.client("/BatchWriteRow", request_body, fn(response_body) ->
        decoded = BatchWriteRowResponse.decode(response_body)
        readable_tables = decode_row_from_batch_write_row(decoded.tables)
        %{decoded | tables: readable_tables}
      end)
      |> Http.post()
    Logger.debug(fn -> "batch_write_row result: #{inspect result}" end)
    result
  end

  defp decode_row_from_batch_write_row(tables) do
    tables
    |> Task.async_stream(fn(table_in_batch_write_row_response) ->
      readable_rows =
        Enum.map(table_in_batch_write_row_response.rows, fn(row_in_batch_write_row_response) ->
          decode_row_from_batch(row_in_batch_write_row_response)
        end)
      %{table_in_batch_write_row_response | rows: readable_rows}
    end, timeout: :infinity)
    |> Enum.map(fn({:ok, response}) -> response end)
  end

  defp filter_to_bytes(nil) do
    nil
  end
  defp filter_to_bytes(%Var.Filter{filter: %Var.CompositeColumnValueFilter{}, filter_type: filter_type} = var_filter) do
    encoded_filter = var_filter |> filter_to_protobuf() |> CompositeColumnValueFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end
  defp filter_to_bytes(%Var.Filter{filter: %Var.ColumnPaginationFilter{}, filter_type: filter_type} = var_filter) do
    encoded_filter = var_filter |> filter_to_protobuf() |> ColumnPaginationFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end
  defp filter_to_bytes(%Var.Filter{filter: %Var.SingleColumnValueFilter{}, filter_type: filter_type} = var_filter) do
    encoded_filter = var_filter |> filter_to_protobuf() |> SingleColumnValueFilter.encode()
    Filter.encode(Filter.new(type: filter_type, filter: encoded_filter))
  end
  defp filter_to_bytes(%Var.Filter{filter: invalid_filter}) do
    raise ExAliyunOts.Error, "Not supported filter: #{inspect invalid_filter}"
  end

  defp filter_to_protobuf(%Var.Filter{filter: filter, filter_type: filter_type}, is_sub_filter \\ false) do
    case filter do
      %Var.CompositeColumnValueFilter{} ->
        prepared_sub_filters =
          Enum.map(filter.sub_filters, fn(sub_filter) ->
            filter_to_protobuf(sub_filter, true)
          end)
        if prepared_sub_filters == [], do: raise ExAliyunOts.Error, "Invalid filter for CompositeColumnValueFilter: #{inspect filter}"
        proto_filter = CompositeColumnValueFilter.new(combinator: filter.combinator, sub_filters: prepared_sub_filters)
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

  defp prepare_time_range(%Var.TimeRange{start_time: start_time, end_time: end_time, specific_time: specific_time}) do
    cond do
      is_integer(start_time) and is_integer(end_time) ->
        TimeRange.new(start_time: start_time, end_time: end_time)
      is_integer(specific_time) ->
        TimeRange.new(specific_time: specific_time)
      true ->
        raise ExAliyunOts.Error, "Invalid time_range, start_time: #{inspect start_time}, end_time: #{inspect end_time}, specific: #{inspect specific_time}"
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

end
