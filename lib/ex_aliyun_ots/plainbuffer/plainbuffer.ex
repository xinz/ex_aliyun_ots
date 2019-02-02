defmodule ExAliyunOts.PlainBuffer do
  require Logger

  @header 0x75

  # tag type
  @tag_row_pk 0x1
  @tag_row_data 0x2
  @tag_cell 0x3
  @tag_cell_name 0x4
  @tag_cell_value 0x5
  @tag_cell_type 0x6
  @tag_cell_timestamp 0x7
  @tag_delete_row_marker 0x8
  @tag_row_checksum 0x9
  @tag_cell_checksum 0x0A

  # cell op type
  @op_delete_all_version 0x1
  @op_delete_one_version 0x3

  # variant type
  @vt_integer 0x0
  @vt_double 0x1
  @vt_boolean 0x2
  @vt_string 0x3
  # @vt_null 0x6
  @vt_blob 0x7
  @vt_inf_min 0x9
  @vt_inf_max 0xA
  @vt_auto_increment 0xB

  # other
  @little_endian_32_size 4
  @little_endian_64_size 8

  @row_data_marker <<(<<@tag_row_data::integer>>), <<@tag_cell::integer>>,
                     (<<@tag_cell_name::integer>>)>>
  @pk_tag_marker <<(<<@tag_row_pk::integer>>), <<@tag_cell::integer>>,
                   (<<@tag_cell_name::integer>>)>>

  @sum_endian_64_size @little_endian_64_size + 1

  alias ExAliyunOts.CRC
  alias ExAliyunOts.Const.{PKType, OperationType}
  require PKType
  require OperationType
  require PKType

  def serialize_for_put_row(primary_keys, attribute_columns) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys) |> columns(attribute_columns)
    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum) |> buffer_list_to_str()
  end

  def serialize_primary_keys(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys)
    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum) |> buffer_list_to_str()
  end

  def serialize_column_value(value) do
    value |> process_column_value() |> buffer_list_to_str()
  end

  def serialize_for_update_row(primary_keys, attribute_columns) when is_map(attribute_columns) do
    # validation
    for {key, value} <- attribute_columns do
      if key not in OperationType.updates_supported() do
        raise ExAliyunOts.Error,
              "Unsupported update type: #{inspect(key)}, in attribute_columns: #{
                inspect(attribute_columns)
              }"
      end

      if not is_list(value) do
        raise ExAliyunOts.Error,
              "Unsupported update value: #{inspect(value)} to key: #{inspect(key)}, expect value as list"
      end
    end

    {buffer, row_checksum} =
      header() |> primary_keys(primary_keys) |> update_grouping_columns(attribute_columns)

    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum) |> buffer_list_to_str()
  end

  def serialize_for_delete_row(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys) |> process_delete_marker()
    buffer |> process_row_checksum(row_checksum) |> buffer_list_to_str()
  end

  def deserialize_row(<<>>) do
    nil
  end

  def deserialize_row(row) do
    Logger.debug(fn -> "** deserialize_row: #{inspect(row, limit: :infinity)}" end)
    row |> deserialize_filter_header() |> deserialize_process_row()
  end

  def deserialize_rows(<<>>) do
    nil
  end

  def deserialize_rows(rows) do
    Logger.debug(fn -> "** deserialize_rows: #{inspect(rows, limit: :infinity)}" end)
    rows |> deserialize_filter_header() |> slice_rows()
  end

  defp header() do
    row_checksum = 0
    {[<<@header::little-integer-size(32)>>], row_checksum}
  end

  defp primary_keys({buffer, row_checksum}, primary_keys) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_row_pk)]

    Enum.reduce(primary_keys, {updated_buffer, row_checksum}, fn primary_keys_item, acc ->
      case primary_keys_item do
        {pk_name, pk_value} ->
          primary_key_column(acc, {pk_name, pk_value})

        primary_keys_item when is_list(primary_keys_item) ->
          # nested primary_keys are used for batch operation with multiple pks
          Enum.reduce(primary_keys_item, acc, fn {pk_name, pk_value}, inner_acc ->
            primary_key_column(inner_acc, {pk_name, pk_value})
          end)

        _ ->
          raise ExAliyunOts.Error, "Invalid primary_keys: #{inspect(primary_keys)}"
      end
    end)
  end

  defp columns({buffer, row_checksum}, columns) when is_list(columns) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_row_data)]

    Enum.reduce(columns, {updated_buffer, row_checksum}, fn column, acc ->
      case column do
        {column_name, column_value, timestamp} ->
          process_column(acc, column_name, column_value, timestamp)

        {column_name, column_value} ->
          process_column(acc, column_name, column_value)

        _ ->
          raise ExAliyunOts.Error, "Invalid column: #{inspect(column)} is not a tuple"
      end
    end)
  end

  defp update_grouping_columns({buffer, row_checksum}, grouping_columns)
       when is_map(grouping_columns) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_row_data)]

    grouping_columns
    |> Map.keys()
    |> Enum.reduce({updated_buffer, row_checksum}, fn update_type, acc ->
      columns = Map.get(grouping_columns, update_type)

      Enum.reduce(columns, acc, fn column, acc_inner ->
        case column do
          column when is_bitstring(column) ->
            process_update_column(acc_inner, update_type, column, {nil, nil})

          {column_name, column_value} ->
            process_update_column(acc_inner, update_type, column_name, {column_value, nil})

          {column_name, column_value, timestamp} ->
            process_update_column(acc_inner, update_type, column_name, {column_value, timestamp})

          _ ->
            raise ExAliyunOts.Error,
                  "Unsupported column when update grouping columns: #{inspect(column)}"
        end
      end)
    end)
  end

  defp primary_key_column({buffer, row_checksum}, {pk_name, pk_value}) do
    cell_checksum = 0
    updated_buffer = buffer ++ [byte_to_binary(@tag_cell)]

    {updated_buffer, cell_checksum} =
      {updated_buffer, cell_checksum}
      |> process_cell_name(pk_name)
      |> process_primary_key_value(pk_value)

    updated_buffer =
      updated_buffer ++ [byte_to_binary(@tag_cell_checksum)] ++ [byte_to_binary(cell_checksum)]

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {updated_buffer, row_checksum}
  end

  defp process_cell_name({buffer, cell_checksum}, name) do
    updated_buffer =
      buffer ++
        [byte_to_binary(@tag_cell_name)] ++
        [<<String.length(name)::little-integer-size(32)>>] ++ [name]

    cell_checksum = CRC.crc_string(cell_checksum, name)
    {updated_buffer, cell_checksum}
  end

  defp process_primary_key_value({buffer, cell_checksum}, value) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_cell_value)]

    cond do
      value == PKType.inf_min() ->
        updated_buffer =
          updated_buffer ++ [<<1::little-integer-size(32)>>] ++ [byte_to_binary(@vt_inf_min)]

        cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_min)
        {updated_buffer, cell_checksum}

      value == PKType.inf_max() ->
        updated_buffer =
          updated_buffer ++ [<<1::little-integer-size(32)>>] ++ [byte_to_binary(@vt_inf_max)]

        cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_max)
        {updated_buffer, cell_checksum}

      value == PKType.auto_increment() ->
        updated_buffer =
          updated_buffer ++
            [<<1::little-integer-size(32)>>] ++ [byte_to_binary(@vt_auto_increment)]

        cell_checksum = CRC.crc_int8(cell_checksum, @vt_auto_increment)
        {updated_buffer, cell_checksum}

      is_integer(value) ->
        updated_buffer =
          updated_buffer ++
            [<<1 + @little_endian_64_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_integer)] ++ [<<value::little-integer-size(64)>>]

        cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
        {updated_buffer, cell_checksum}

      is_binary(value) ->
        prefix_length = @little_endian_32_size + 1
        value_size = byte_size(value)

        updated_buffer =
          updated_buffer ++
            [<<prefix_length + value_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_string)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

        cell_checksum =
          cell_checksum
          |> CRC.crc_int8(@vt_string)
          |> CRC.crc_int32(value_size)
          |> CRC.crc_string(value)

        {updated_buffer, cell_checksum}

      is_bitstring(value) ->
        prefix_length = @little_endian_32_size + 1
        value_size = byte_size(value)

        updated_buffer =
          updated_buffer ++
            [<<prefix_length + value_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_blob)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

        cell_checksum =
          cell_checksum
          |> CRC.crc_int8(@vt_blob)
          |> CRC.crc_int32(value_size)
          |> CRC.crc_string(value)

        {updated_buffer, cell_checksum}

      true ->
        raise ExAliyunOts.Error, "Unsupported primary key for value: #{inspect(value)}"
    end
  end

  defp process_column({buffer, row_checksum}, column_name, column_value, timestamp \\ nil) do
    cell_checksum = 0
    updated_buffer = buffer ++ [byte_to_binary(@tag_cell)]

    {updated_buffer, cell_checksum} =
      {updated_buffer, cell_checksum}
      |> process_cell_name(column_name)
      |> process_column_value_with_checksum(column_value)

    {updated_buffer, cell_checksum} =
      if timestamp != nil do
        updated_buffer =
          updated_buffer ++
            [byte_to_binary(@tag_cell_timestamp)] ++ [<<timestamp::little-integer-size(64)>>]

        cell_checksum = CRC.crc_int64(cell_checksum, timestamp)
        {updated_buffer, cell_checksum}
      else
        {updated_buffer, cell_checksum}
      end

    updated_buffer =
      updated_buffer ++ [byte_to_binary(@tag_cell_checksum)] ++ [byte_to_binary(cell_checksum)]

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {updated_buffer, row_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_cell_value)]

    cond do
      is_boolean(value) ->
        updated_buffer =
          updated_buffer ++
            [<<2::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_boolean)] ++ [boolean_to_integer(value)]

        cell_checksum = CRC.crc_int8(cell_checksum, @vt_boolean)

        cell_checksum =
          if value do
            CRC.crc_int8(cell_checksum, 1)
          else
            CRC.crc_int8(cell_checksum, 0)
          end

        {updated_buffer, cell_checksum}

      is_integer(value) ->
        updated_buffer =
          updated_buffer ++
            [<<1 + @little_endian_64_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_integer)] ++ [<<value::little-integer-size(64)>>]

        cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
        {updated_buffer, cell_checksum}

      is_float(value) ->
        value_to_binary = <<value::float-little>>
        <<long::unsigned-little-integer-64>> = value_to_binary

        updated_buffer =
          updated_buffer ++
            [<<1 + @little_endian_64_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_double)] ++ [value_to_binary]

        cell_checksum = cell_checksum |> CRC.crc_int8(@vt_double) |> CRC.crc_int64(long)
        {updated_buffer, cell_checksum}

      is_binary(value) ->
        prefix_length = @little_endian_32_size + 1
        value_size = byte_size(value)

        updated_buffer =
          updated_buffer ++
            [<<prefix_length + value_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_string)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

        cell_checksum =
          cell_checksum
          |> CRC.crc_int8(@vt_string)
          |> CRC.crc_int32(value_size)
          |> CRC.crc_string(value)

        {updated_buffer, cell_checksum}

      is_bitstring(value) ->
        prefix_length = @little_endian_32_size + 1
        value_size = byte_size(value)

        updated_buffer =
          updated_buffer ++
            [<<prefix_length + value_size::little-integer-size(32)>>] ++
            [byte_to_binary(@vt_blob)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

        cell_checksum =
          cell_checksum
          |> CRC.crc_int8(@vt_blob)
          |> CRC.crc_int32(value_size)
          |> CRC.crc_string(value)

        {updated_buffer, cell_checksum}

      true ->
        raise ExAliyunOts.Error, "Unsupported column for value: #{inspect(value)}"
    end
  end

  defp process_column_value(value) do
    cond do
      is_boolean(value) ->
        [byte_to_binary(@vt_boolean)] ++ [boolean_to_integer(value)]

      is_integer(value) ->
        [byte_to_binary(@vt_integer)] ++ [<<value::little-integer-size(64)>>]

      is_binary(value) ->
        value_size = byte_size(value)
        [byte_to_binary(@vt_string)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

      is_bitstring(value) ->
        value_size = byte_size(value)
        [byte_to_binary(@vt_blob)] ++ [<<value_size::little-integer-size(32)>>] ++ [value]

      is_float(value) ->
        value_to_binary = <<value::float-little>>
        [byte_to_binary(@vt_double)] ++ [value_to_binary]

      true ->
        raise ExAliyunOts.Error, "Unsupported column for value: #{inspect(value)}"
    end
  end

  defp process_update_column(
         {buffer, row_checksum},
         update_type,
         column_name,
         {column_value, timestamp}
       ) do
    cell_checksum = 0
    updated_buffer = buffer ++ [byte_to_binary(@tag_cell)]

    {updated_buffer, cell_checksum} =
      process_cell_name({updated_buffer, cell_checksum}, column_name)

    {updated_buffer, cell_checksum} =
      if column_value != nil do
        process_column_value_with_checksum({updated_buffer, cell_checksum}, column_value)
      else
        {updated_buffer, cell_checksum}
      end

    updated_buffer =
      cond do
        update_type == OperationType.delete() ->
          updated_buffer ++
            [byte_to_binary(@tag_cell_type)] ++ [byte_to_binary(@op_delete_one_version)]

        update_type == OperationType.delete_all() ->
          updated_buffer ++
            [byte_to_binary(@tag_cell_type)] ++ [byte_to_binary(@op_delete_all_version)]

        true ->
          updated_buffer
      end

    {updated_buffer, cell_checksum} =
      if timestamp != nil do
        updated_buffer =
          updated_buffer ++
            [byte_to_binary(@tag_cell_timestamp)] ++ [<<timestamp::little-integer-size(64)>>]

        cell_checksum = CRC.crc_int64(cell_checksum, timestamp)
        {updated_buffer, cell_checksum}
      else
        {updated_buffer, cell_checksum}
      end

    cell_checksum =
      cond do
        update_type == OperationType.delete() ->
          CRC.crc_int8(cell_checksum, @op_delete_one_version)

        update_type == OperationType.delete_all() ->
          CRC.crc_int8(cell_checksum, @op_delete_all_version)

        true ->
          cell_checksum
      end

    updated_buffer =
      updated_buffer ++ [byte_to_binary(@tag_cell_checksum)] ++ [byte_to_binary(cell_checksum)]

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {updated_buffer, row_checksum}
  end

  defp process_delete_marker({buffer, row_checksum}) do
    updated_buffer = buffer ++ [byte_to_binary(@tag_delete_row_marker)]
    row_checksum = CRC.crc_int8(row_checksum, 1)
    {updated_buffer, row_checksum}
  end

  defp boolean_to_integer(value) do
    if value, do: <<1>>, else: <<0>>
  end

  defp integer_to_boolean(value) do
    if value == 1, do: true, else: false
  end

  defp process_row_checksum(buffer, row_checksum) do
    buffer ++ [byte_to_binary(@tag_row_checksum)] ++ [byte_to_binary(row_checksum)]
  end

  defp byte_to_binary(byte) do
    <<byte::integer>>
  end

  defp buffer_list_to_str(buffer_list) do
    Enum.reduce(buffer_list, fn buffer_item, acc ->
      <<acc::binary, buffer_item::binary>>
    end)
  end

  # deserialize processing

  defp deserialize_filter_header(<<@header::little-integer-size(32), rest_row::binary>>) do
    rest_row
  end

  defp deserialize_filter_header(invalid_row) do
    raise ExAliyunOts.Error, "Invalid row to deserialize, #{inspect(invalid_row)}"
  end

  defp deserialize_process_row(row_values) do
    {primary_keys, attribute_columns} = deserialize_row_data(row_values)
    {primary_keys, attribute_columns}
  end

  defp slice_rows(rows) do
    rows_list = :binary.split(rows, @pk_tag_marker, [:global])

    checked_rows =
      Enum.reduce(Enum.slice(rows_list, 1..-1), %{tidied: [], to_be_merged: <<>>}, fn row, acc ->
        Logger.debug(fn -> "splited data: #{inspect(row, limit: :infinity)}" end)
        Logger.debug(fn -> "to_be_merged: #{inspect(acc.to_be_merged)}" end)

        case :binary.at(row, byte_size(row) - 2) do
          @tag_row_checksum ->
            if acc.to_be_merged == <<>> do
              Map.put(acc, :tidied, acc.tidied ++ [row])
            else
              %{
                acc
                | tidied: acc.tidied ++ [acc.to_be_merged <> @pk_tag_marker <> row],
                  to_be_merged: <<>>
              }
            end

          _ ->
            if acc.to_be_merged == <<>> do
              Map.put(acc, :to_be_merged, acc.to_be_merged <> row)
            else
              Map.put(acc, :to_be_merged, acc.to_be_merged <> @pk_tag_marker <> row)
            end
        end
      end)

    Logger.debug(fn -> "checked_rows: #{inspect(checked_rows, limit: :infinity)}" end)

    # Enum.map(checked_rows.tidied, fn(row_values) -> 
    #  {primary_keys, attribute_columns} = deserialize_row_data(@pk_tag_marker <> row_values)
    #  [{primary_keys, attribute_columns}]
    # end)
    # |> Enum.reduce([], fn(prepared_row, acc) ->
    #  acc ++ prepared_row
    # end)

    checked_rows.tidied
    |> Task.async_stream(
      fn row_values ->
        {primary_keys, attribute_columns} = deserialize_row_data(@pk_tag_marker <> row_values)
        Logger.debug(fn -> "primary_keys: #{inspect(primary_keys)}" end)
        Logger.debug(fn -> "attribute_columns: #{inspect(attribute_columns)}" end)
        [{primary_keys, attribute_columns}]
      end,
      timeout: :infinity
    )
    |> Enum.reduce([], fn {:ok, prepared_row}, acc ->
      acc ++ prepared_row
    end)
  end

  defp deserialize_row_data(values) do
    Logger.debug(fn -> "deserialize_row_data: #{inspect(values, limit: :infinity)}" end)
    row_data_parts = :binary.split(values, @row_data_marker, [:global])

    matched_index =
      Enum.find_index(row_data_parts, fn part_value ->
        :binary.at(part_value, byte_size(part_value) - 2) == @tag_cell_checksum
      end)

    Logger.debug(fn -> "matched_index: #{matched_index}" end)
    Logger.debug(fn -> "#{inspect(row_data_parts, limit: :infinity)}" end)

    if matched_index != nil do
      primary_keys_binary =
        row_data_parts |> Enum.slice(0..matched_index) |> Enum.join(@row_data_marker)

      attribute_columns_binary =
        <<@tag_cell::integer, @tag_cell_name::integer>> <>
          (row_data_parts |> Enum.slice((matched_index + 1)..-1) |> Enum.join(@row_data_marker))

      primary_keys_binary =
        case primary_keys_binary do
          <<(<<@tag_row_pk::integer>>), primary_keys_binary_rest::binary>> ->
            primary_keys_binary_rest

          _ ->
            raise ExAliyunOts.Error,
                  "Unexcepted row data when processing primary_keys: #{
                    inspect(primary_keys_binary, limit: :infinity)
                  }"
        end

      pk_task =
        Task.async(fn ->
          deserialize_process_primary_keys(primary_keys_binary, [])
        end)

      attr_task =
        Task.async(fn ->
          deserialize_process_columns(attribute_columns_binary, [])
        end)

      {Task.await(pk_task, :infinity), Task.await(attr_task, :infinity)}
    else
      primary_keys_binary =
        case values do
          <<(<<@tag_row_pk::integer>>), primary_keys_binary_rest::binary>> ->
            primary_keys_binary_rest

          _ ->
            raise ExAliyunOts.Error,
                  "Unexcepted row data when processing primary_keys: #{
                    inspect(values, limit: :infinity)
                  }"
        end

      {deserialize_process_primary_keys(primary_keys_binary, []), nil}
    end
  end

  defp deserialize_process_primary_keys(primary_keys, result) do
    Logger.debug(fn ->
      "** deserializing primary_keys, prepared result: #{inspect(result)}, the pks data is #{
        inspect(primary_keys, limit: :infinity)
      } **"
    end)

    case primary_keys do
      <<(<<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>), rest::binary>> ->
        <<primary_key_size::little-integer-size(32), rest::binary>> = rest
        primary_key_name = binary_part(rest, 0, primary_key_size)
        Logger.debug(fn -> "get primary_key_name: #{inspect(primary_key_name)}" end)

        rest_primary_key_value_and_other_pk =
          binary_part(rest, primary_key_size, byte_size(rest) - primary_key_size)

        Logger.debug(fn ->
          ">>> rest_primary_key_value_and_other_pk: #{
            inspect(rest_primary_key_value_and_other_pk, limit: :infinity)
          } <<<"
        end)

        case calculate_tag_cell_index(rest_primary_key_value_and_other_pk) do
          next_cell_index when is_integer(next_cell_index) ->
            Logger.debug(fn -> "find next_cell_index: #{next_cell_index}" end)

            value_binary = binary_part(rest_primary_key_value_and_other_pk, 0, next_cell_index)
            primary_key_value = deserialize_process_primary_key_value(value_binary)
            Logger.debug(fn -> "get primary_key_value: #{inspect(primary_key_value)}" end)
            updated_result = result ++ [{primary_key_name, primary_key_value}]

            other_pk =
              binary_part(
                rest_primary_key_value_and_other_pk,
                next_cell_index,
                byte_size(rest_primary_key_value_and_other_pk) - next_cell_index
              )

            Logger.debug(fn ->
              "** the rest to be deserialized data is #{inspect(other_pk, limit: :infinity)} **"
            end)

            if other_pk != "" do
              deserialize_process_primary_keys(other_pk, updated_result)
            else
              updated_result
            end

          :nomatch ->
            Logger.debug("no more cells to deserialize")

            primary_key_value =
              deserialize_process_primary_key_value(rest_primary_key_value_and_other_pk)

            Logger.debug(fn -> "get primary_key_value: #{inspect(primary_key_value)}" end)
            result ++ [{primary_key_name, primary_key_value}]
        end

      _ ->
        result
    end
  end

  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>, <<@vt_integer::integer>>, <<value::little-integer-size(64)>>, (<<_rest::binary>>)>>) do
    value
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>, <<@vt_integer::integer>>, (<<value::little-integer-size(64)>>)>>) do
    value
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>, <<@vt_integer::integer>>, (<<rest::binary>>)>>) do
    raise ExAliyunOts.Error, "Unexcepted integer value as primary value: #{inspect(rest)}"
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<total_size::little-integer-size(32)>>, <<@vt_string::integer>>, <<_value_size::little-integer-size(32)>>, (<<value::binary>>)>>) do
    Logger.debug("process pk value as a string type")
    value_size = total_size - @little_endian_32_size - 1
    binary_part(value, 0, value_size)
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<total_size::little-integer-size(32)>>, <<@vt_blob::integer>>, <<_value_size::little-integer-size(32)>>, (<<value::binary>>)>>) do
    Logger.debug("process pk value as a binary type")
    value_size = total_size - @little_endian_32_size - 1
    binary_part(value, 0, value_size)
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>, (<<rest::binary>>)>>) do
    raise ExAliyunOts.Error, "Unexcepted string value as primary value: #{inspect(rest)}"
  end
  defp deserialize_process_primary_key_value(<<(<<@tag_cell_value::integer>>), (<<rest::binary>>)>>) do
    raise ExAliyunOts.Error, "Unexcepted value as primary value: #{inspect(rest)}"
  end

  defp deserialize_process_columns(attribute_columns, result) do
    Logger.debug(">>>> attribute_columns <<<<")
    Logger.debug(fn -> "attribute_columns: #{inspect(attribute_columns, limit: :infinity)}" end)

    case attribute_columns do
      <<(<<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>), rest::binary>> ->
        <<column_name_size::little-integer-size(32), rest::binary>> = rest
        column_name = binary_part(rest, 0, column_name_size)
        Logger.debug(fn -> "column_name: #{inspect(column_name)}" end)

        rest_value_and_other_columns =
          binary_part(rest, column_name_size, byte_size(rest) - column_name_size)

        case calculate_tag_cell_index(rest_value_and_other_columns) do
          next_cell_index when is_integer(next_cell_index) ->
            Logger.debug(fn -> ">>> find next_cell_index: #{next_cell_index}" end)
            value_binary = binary_part(rest_value_and_other_columns, 0, next_cell_index)
            Logger.debug(fn -> ">>> value_binary: #{inspect(value_binary, limit: :infinity)}" end)

            {column_value, timestamp} =
              deserialize_process_column_value_with_checksum(value_binary)

            updated_result = result ++ [{column_name, column_value, timestamp}]

            other_attribute_columns =
              binary_part(
                rest_value_and_other_columns,
                next_cell_index,
                byte_size(rest_value_and_other_columns) - next_cell_index
              )

            if other_attribute_columns != "" do
              deserialize_process_columns(other_attribute_columns, updated_result)
            else
              updated_result
            end

          :nomatch ->
            Logger.debug(">>> not find next_cell_index")

            {column_value, timestamp} =
              deserialize_process_column_value_with_checksum(rest_value_and_other_columns)

            Logger.debug(fn -> "column_value: #{inspect(column_value)}" end)
            result ++ [{column_name, column_value, timestamp}]
        end

      _ ->
        result
    end
  end

  defp deserialize_process_column_value_timestamp(timestamp_binary) do
    case timestamp_binary do
      <<(<<@tag_cell_timestamp::integer>>), (<<timestamp::little-integer-size(64)>>)>> ->
        timestamp

      <<(<<@tag_cell_timestamp::integer>>), <<timestamp::little-integer-size(64)>>,
        _rest::binary>> ->
        timestamp

      _ ->
        nil
    end
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
          <<@vt_boolean::integer>>, <<value::integer>>, (<<timestamp_rest::binary>>)>>
      ) do
    value_boolean = integer_to_boolean(value)
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value_boolean, timestamp}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
          <<@vt_boolean::integer>>, (<<value::integer>>)>>
      ) do
    value_boolean = integer_to_boolean(value)
    {value_boolean, nil}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
          <<@vt_boolean::integer>>, (<<rest::binary>>)>>
      ) do
    raise ExAliyunOts.Error, "Invalid boolean value as: #{inspect(rest)}"
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_integer::integer>>, <<value::little-integer-size(64)>>,
          (<<timestamp_rest::binary>>)>>
      ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_integer::integer>>, (<<value::little-integer-size(64)>>)>>
      ) do
    {value, nil}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_integer::integer>>, (<<rest::binary>>)>>
      ) do
    raise ExAliyunOts.Error, "Invalid integer value as: #{inspect(rest)}"
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_double::integer>>, <<value::float-little>>, (<<timestamp_rest::binary>>)>>
      ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_double::integer>>, (<<value::float-little>>)>>
      ) do
    {value, nil}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
          <<@vt_double::integer>>, (<<rest::binary>>)>>
      ) do
    raise ExAliyunOts.Error, "Invalid float value as: #{inspect(rest)}"
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_string::integer>>, <<value_size::little-integer-size(32), value::binary-size(value_size)>>,
          (<<timestamp_rest::binary>>)>>
      ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_string::integer>>, (<<value_size::little-integer-size(32), value::binary-size(value_size)>>)>>
      ) do
    {value, nil}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_string::integer>>, rest::binary>>
      ) do
    raise ExAliyunOts.Error, "Unexcepted string value as: #{inspect(rest)}"
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_blob::integer>>, <<value_size::little-integer-size(32), value::binary-size(value_size)>>,
          (<<timestamp_rest::binary>>)>>
      ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_blob::integer>>, (<<value_size::little-integer-size(32), value::binary-size(value_size)>>)>>
      ) do
    {value, nil}
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
          <<@vt_blob::integer>>, rest::binary>>
      ) do
    raise ExAliyunOts.Error, "Unexcepted string value as: #{inspect(rest)}"
  end

  def deserialize_process_column_value_with_checksum(
        <<(<<@tag_cell_value::integer>>), rest::binary>>
      ) do
    raise ExAliyunOts.Error, "Unexcepted value as: #{inspect(rest)}"
  end

  def calculate_tag_cell_index(values) do
    Logger.debug(fn -> "calculate_tag_cell_index: #{inspect(values, limit: :infinity)}" end)

    splited =
      :binary.split(values, <<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>, [:global])

    Logger.debug(fn -> "splited #{inspect(splited, limit: :infinity)}" end)

    index =
      Enum.find_index(splited, fn item ->
        if item == "" do
          false
        else
          last_two_bytes = :binary.part(item, {byte_size(item), -2})

          case last_two_bytes do
            <<(<<@tag_cell_checksum::integer>>), (<<_checksum_value::integer>>)>> ->
              true

            _ ->
              false
          end
        end
      end)

    Logger.debug(fn -> "index #{index}" end)

    if index == nil do
      :nomatch
    else
      calcuated_index =
        splited
        |> Enum.slice(0..index)
        |> Enum.reduce(0, fn item, acc ->
          byte_size(item) + acc
        end)

      calcuated_index + index * 2
    end
  end
end
