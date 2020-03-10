defmodule ExAliyunOts.PlainBuffer do
  @moduledoc false

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
  @op_increment 0x4

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

  @row_data_marker <<@tag_row_data::integer, @tag_cell::integer, @tag_cell_name::integer>>
  @pk_tag_marker <<@tag_row_pk::integer, @tag_cell::integer, @tag_cell_name::integer>>

  @sum_endian_64_size @little_endian_64_size + 1

  alias ExAliyunOts.CRC
  alias ExAliyunOts.Const.{PKType, OperationType}
  require PKType
  require OperationType
  require PKType

  import ExAliyunOts.Logger, only: [debug: 1]

  def serialize_for_put_row(primary_keys, attribute_columns) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys) |> columns(attribute_columns)
    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum)
  end

  def serialize_primary_keys(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys)
    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum)
  end

  def serialize_column_value(value) do
    value |> process_column_value()
  end

  def serialize_for_update_row(primary_keys, attribute_columns) when is_map(attribute_columns) do
    # validation
    for {key, value} <- attribute_columns do
      if key not in OperationType.updates_supported() do
        raise ExAliyunOts.RuntimeError,
              "Unsupported update type: #{inspect(key)}, in attribute_columns: #{
                inspect(attribute_columns)
              }"
      end

      if not is_list(value) do
        raise ExAliyunOts.RuntimeError,
              "Unsupported update value: #{inspect(value)} to key: #{inspect(key)}, expect value as list"
      end
    end

    {buffer, row_checksum} =
      header() |> primary_keys(primary_keys) |> update_grouping_columns(attribute_columns)

    row_checksum = CRC.crc_int8(row_checksum, 0)
    buffer |> process_row_checksum(row_checksum)
  end

  def serialize_for_delete_row(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys) |> process_delete_marker()
    buffer |> process_row_checksum(row_checksum)
  end

  def deserialize_row(<<>>) do
    nil
  end

  def deserialize_row(row) do
    debug(fn ->
      [
        "** deserialize_row:\n",
        inspect(row, limit: :infinity)
      ]
    end)

    row |> deserialize_filter_header() |> deserialize_process_row()
  end

  def deserialize_rows(<<>>) do
    nil
  end

  def deserialize_rows(rows) do
    debug(fn ->
      [
        "** deserialize_rows:\n",
        inspect(rows, limit: :infinity)
      ]
    end)

    rows |> deserialize_filter_header() |> slice_rows()
  end

  defp header() do
    # row_checksum initialized value of header is 0
    {<<@header::little-integer-size(32)>>, 0}
  end

  defp primary_keys({buffer, row_checksum}, primary_keys) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_row_pk)::bitstring>>

    do_primary_keys(primary_keys, buffer, row_checksum)
  end

  defp do_primary_keys([], buffer, row_checksum) do
    {buffer, row_checksum}
  end
  defp do_primary_keys([primary_key | rest], buffer, row_checksum) do
    {buffer, row_checksum} = primary_key_column(primary_key, {buffer, row_checksum})
    do_primary_keys(rest, buffer, row_checksum)
  end

  defp columns({buffer, row_checksum}, columns) when is_list(columns) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_row_data)::bitstring>>
    do_columns(columns, buffer, row_checksum)
  end

  defp do_columns([], buffer, row_checksum) do
    {buffer, row_checksum}
  end
  defp do_columns([column | rest], buffer, row_checksum) do
    {buffer, row_checksum} = process_column(column, {buffer, row_checksum})
    do_columns(rest, buffer, row_checksum)
  end

  defp update_grouping_columns({buffer, row_checksum}, grouping_columns)
       when is_map(grouping_columns) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_row_data)::bitstring>>

    grouping_columns
    |> Map.keys()
    |> Enum.reduce({buffer, row_checksum}, fn update_type, acc ->
      columns = Map.get(grouping_columns, update_type)

      Enum.reduce(columns, acc, fn column, acc_inner ->
        process_update_column(acc_inner, update_type, column)
      end)
    end)
  end

  defp primary_key_column({pk_name, pk_value}, {buffer, row_checksum}) do
    cell_checksum = 0
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell)::bitstring>>

    {buffer, cell_checksum} =
      {buffer, cell_checksum}
      |> process_cell_name(pk_name)
      |> process_primary_key_value(pk_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_checksum)::bitstring,
        byte_to_binary(cell_checksum)::bitstring>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp primary_key_column(primary_keys, {buffer, row_checksum}) when is_list(primary_keys) do
    # nested primary_keys are used for batch operation with multiple pks
    Enum.reduce(primary_keys, {buffer, row_checksum}, fn {pk_name, pk_value}, acc ->
      primary_key_column({pk_name, pk_value}, acc)
    end)
  end

  defp primary_key_column(primary_keys, _) do
    raise ExAliyunOts.RuntimeError, "Invalid primary_keys: #{inspect(primary_keys)}"
  end

  defp process_cell_name({buffer, cell_checksum}, name) do
    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_name)::bitstring,
        <<String.length(name)::little-integer-size(32)>>, name::bitstring>>

    cell_checksum = CRC.crc_string(cell_checksum, name)
    {buffer, cell_checksum}
  end

  defp process_primary_key_value({buffer, cell_checksum}, value) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring>>
    do_process_primary_key_value({buffer, cell_checksum}, value)
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
    when value == PKType.inf_min()
    when value == :inf_min do
    buffer =
      <<buffer::bitstring, <<1::little-integer-size(32)>>,
        byte_to_binary(@vt_inf_min)::bitstring>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_min)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
    when value == PKType.inf_max()
    when value == :inf_max do
    buffer =
      <<buffer::bitstring, <<1::little-integer-size(32)>>,
        byte_to_binary(@vt_inf_max)::bitstring>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_max)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
       when value == PKType.auto_increment() do
    buffer =
      <<buffer::bitstring, <<1::little-integer-size(32)>>,
        byte_to_binary(@vt_auto_increment)::bitstring>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_auto_increment)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_integer(value) do
    buffer =
      <<buffer::bitstring, <<1 + @little_endian_64_size::little-integer-size(32)>>,
        byte_to_binary(@vt_integer)::bitstring, (<<value::little-integer-size(64)>>)>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_binary(value) do
    prefix_length = @little_endian_32_size + 1
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<prefix_length + value_size::little-integer-size(32)>>,
        byte_to_binary(@vt_string)::bitstring, <<value_size::little-integer-size(32)>>,
        value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_string)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_bitstring(value) do
    prefix_length = @little_endian_32_size + 1
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<prefix_length + value_size::little-integer-size(32)>>,
        byte_to_binary(@vt_blob)::bitstring, <<value_size::little-integer-size(32)>>,
        value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_blob)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value(_, value) do
    raise ExAliyunOts.RuntimeError, "Unsupported primary key for value: #{inspect(value)}"
  end

  defp process_column({column_name, column_value}, {buffer, row_checksum}) do
    cell_checksum = 0
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell)::bitstring>>

    {buffer, cell_checksum} =
      {buffer, cell_checksum}
      |> process_cell_name(column_name)
      |> process_column_value_with_checksum(column_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_checksum)::bitstring,
        byte_to_binary(cell_checksum)::bitstring>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_column({column_name, column_value, timestamp}, {buffer, row_checksum})
       when timestamp != nil do
    cell_checksum = 0
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell)::bitstring>>

    {buffer, cell_checksum} =
      {buffer, cell_checksum}
      |> process_cell_name(column_name)
      |> process_column_value_with_checksum(column_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_timestamp)::bitstring,
        (<<timestamp::little-integer-size(64)>>)>>

    cell_checksum = CRC.crc_int64(cell_checksum, timestamp)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_checksum)::bitstring,
        byte_to_binary(cell_checksum)::bitstring>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_column(column, _) do
    raise ExAliyunOts.RuntimeError, "Invalid column: #{inspect(column)} is not a tuple"
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, nil) do
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value)
       when is_boolean(value) do
    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring,
        <<2::little-integer-size(32)>>, byte_to_binary(@vt_boolean)::bitstring,
        boolean_to_integer(value)::bitstring>>

    cell_checksum =
      if value do
        cell_checksum
        |> CRC.crc_int8(@vt_boolean)
        |> CRC.crc_int8(1)
      else
        cell_checksum
        |> CRC.crc_int8(@vt_boolean)
        |> CRC.crc_int8(0)
      end

    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value)
       when is_integer(value) do
    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring,
        <<1 + @little_endian_64_size::little-integer-size(32)>>,
        byte_to_binary(@vt_integer)::bitstring, (<<value::little-integer-size(64)>>)>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value) when is_float(value) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring>>
    value_to_binary = <<value::float-little>>
    <<long::unsigned-little-integer-64>> = value_to_binary

    buffer =
      <<buffer::bitstring, <<1 + @little_endian_64_size::little-integer-size(32)>>,
        byte_to_binary(@vt_double)::bitstring, value_to_binary::bitstring>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_double) |> CRC.crc_int64(long)
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value) when is_binary(value) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring>>
    prefix_length = @little_endian_32_size + 1
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<prefix_length + value_size::little-integer-size(32)>>,
        byte_to_binary(@vt_string)::bitstring, <<value_size::little-integer-size(32)>>,
        value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_string)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value)
       when is_bitstring(value) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell_value)::bitstring>>
    prefix_length = @little_endian_32_size + 1
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<prefix_length + value_size::little-integer-size(32)>>,
        byte_to_binary(@vt_blob)::bitstring, <<value_size::little-integer-size(32)>>,
        value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_blob)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({_buffer, _cell_checksum}, value) do
    raise ExAliyunOts.RuntimeError, "Unsupported column for value: #{inspect(value)}"
  end

  defp process_column_value(value) when is_boolean(value) do
    [byte_to_binary(@vt_boolean), boolean_to_integer(value)]
  end

  defp process_column_value(value) when is_integer(value) do
    [byte_to_binary(@vt_integer), <<value::little-integer-size(64)>>]
  end

  defp process_column_value(value) when is_binary(value) do
    value_size = byte_size(value)
    [byte_to_binary(@vt_string), <<value_size::little-integer-size(32)>>, value]
  end

  defp process_column_value(value) when is_bitstring(value) do
    value_size = byte_size(value)
    [byte_to_binary(@vt_blob), <<value_size::little-integer-size(32)>>, value]
  end

  defp process_column_value(value) when is_float(value) do
    value_to_binary = <<value::float-little>>
    [byte_to_binary(@vt_double), value_to_binary]
  end

  defp process_column_value(value) do
    raise ExAliyunOts.RuntimeError, "Unsupported column for value: #{inspect(value)}"
  end

  defp process_update_column({buffer, row_checksum}, update_type, column)
       when is_bitstring(column) do
    do_process_update_column({buffer, row_checksum}, update_type, column, {nil, nil})
  end

  defp process_update_column({buffer, row_checksum}, update_type, {column_name, column_value}) do
    do_process_update_column(
      {buffer, row_checksum},
      update_type,
      column_name,
      {column_value, nil}
    )
  end

  defp process_update_column(
         {buffer, row_checksum},
         update_type,
         {column_name, column_value, timestamp}
       ) do
    do_process_update_column(
      {buffer, row_checksum},
      update_type,
      column_name,
      {column_value, timestamp}
    )
  end

  defp process_update_column({_buffer, _row_checksum}, _update_type, column) do
    raise ExAliyunOts.RuntimeError,
          "Unsupported column when update grouping columns: #{inspect(column)}"
  end

  defp do_process_update_column(
         {buffer, row_checksum},
         OperationType.delete(),
         column_name,
         {column_value, timestamp}
       ) do
    {buffer, cell_checksum} = process_update_column_with_cell(buffer, column_name, column_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_type)::bitstring,
        byte_to_binary(@op_delete_one_version)::bitstring>>

    {buffer, cell_checksum} =
      process_update_column_with_timestamp(buffer, cell_checksum, timestamp)

    cell_checksum = CRC.crc_int8(cell_checksum, @op_delete_one_version)

    process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum)
  end

  defp do_process_update_column(
         {buffer, row_checksum},
         OperationType.delete_all(),
         column_name,
         {column_value, timestamp}
       ) do
    {buffer, cell_checksum} = process_update_column_with_cell(buffer, column_name, column_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_type)::bitstring,
        byte_to_binary(@op_delete_all_version)::bitstring>>

    {buffer, cell_checksum} =
      process_update_column_with_timestamp(buffer, cell_checksum, timestamp)

    cell_checksum = CRC.crc_int8(cell_checksum, @op_delete_all_version)

    process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum)
  end

  defp do_process_update_column(
         {buffer, row_checksum},
         OperationType.increment(),
         column_name,
         {column_value, timestamp}
       ) do
    {buffer, cell_checksum} = process_update_column_with_cell(buffer, column_name, column_value)

    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_type)::bitstring,
        byte_to_binary(@op_increment)::bitstring>>

    {buffer, cell_checksum} =
      process_update_column_with_timestamp(buffer, cell_checksum, timestamp)

    cell_checksum = CRC.crc_int8(cell_checksum, @op_increment)

    process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum)
  end

  defp do_process_update_column(
         {buffer, row_checksum},
         _update_type,
         column_name,
         {column_value, timestamp}
       ) do
    {buffer, cell_checksum} = process_update_column_with_cell(buffer, column_name, column_value)

    {buffer, cell_checksum} =
      process_update_column_with_timestamp(buffer, cell_checksum, timestamp)

    process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum)
  end

  defp process_update_column_with_cell(buffer, column_name, column_value) do
    cell_checksum = 0
    buffer = <<buffer::bitstring, byte_to_binary(@tag_cell)::bitstring>>

    {buffer, cell_checksum}
    |> process_cell_name(column_name)
    |> process_column_value_with_checksum(column_value)
  end

  defp process_update_column_with_timestamp(buffer, cell_checksum, nil) do
    {buffer, cell_checksum}
  end

  defp process_update_column_with_timestamp(buffer, cell_checksum, timestamp) do
    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_timestamp)::bitstring,
        (<<timestamp::little-integer-size(64)>>)>>

    cell_checksum = CRC.crc_int64(cell_checksum, timestamp)
    {buffer, cell_checksum}
  end

  defp process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum) do
    buffer =
      <<buffer::bitstring, byte_to_binary(@tag_cell_checksum)::bitstring,
        byte_to_binary(cell_checksum)::bitstring>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_delete_marker({buffer, row_checksum}) do
    buffer = <<buffer::bitstring, byte_to_binary(@tag_delete_row_marker)::bitstring>>
    row_checksum = CRC.crc_int8(row_checksum, 1)
    {buffer, row_checksum}
  end

  defp boolean_to_integer(true) do
    <<1>>
  end

  defp boolean_to_integer(_) do
    <<0>>
  end

  defp integer_to_boolean(1) do
    true
  end

  defp integer_to_boolean(_) do
    false
  end

  defp process_row_checksum(buffer, row_checksum) do
    <<buffer::bitstring, byte_to_binary(@tag_row_checksum)::bitstring,
      byte_to_binary(row_checksum)::bitstring>>
  end

  defp byte_to_binary(byte) do
    <<byte::integer>>
  end

  # deserialize processing

  defp deserialize_filter_header(<<@header::little-integer-size(32), rest_row::binary>>) do
    rest_row
  end

  defp deserialize_filter_header(invalid_row) do
    raise ExAliyunOts.RuntimeError, "Invalid row to deserialize, #{inspect(invalid_row)}"
  end

  defp deserialize_process_row(row_values) do
    case deserialize_row_data(row_values) do
      {primary_keys, attribute_columns} ->
        {primary_keys, attribute_columns}

      nil ->
        nil
    end
  end

  defp slice_rows(rows) do
    result =
      rows
      |> :binary.split(@pk_tag_marker, [:global])
      |> do_slice_rows()

    debug(fn ->
      [
        "\nchecked_rows result:\s",
        inspect(result, limit: :infinity)
      ]
    end)

    Enum.reverse(result.rows)
  end

  defp do_slice_rows(bytes_rows_list) do
    do_slice_rows(bytes_rows_list, %{rows: [], to_be_merged: <<>>})
  end

  defp do_slice_rows([], prepared) do
    prepared
  end

  defp do_slice_rows([<<>> | rest], prepared) do
    do_slice_rows(rest, prepared)
  end
  defp do_slice_rows([row | rest], prepared) do
    second_last_byte = binary_part(row, byte_size(row) - 2, 1)
    prepared = do_slice_row_binary(second_last_byte, row, prepared)
    do_slice_rows(rest, prepared)
  end

  defp do_slice_row_binary(<<@tag_row_checksum::integer>>, row, %{to_be_merged: <<>>, rows: rows} = result) do
    row = deserialize_raw_rows(row)
    Map.put(result, :rows, [row | rows])
  end
  defp do_slice_row_binary(<<@tag_row_checksum::integer>>, row, %{to_be_merged: to_be_merged, rows: rows} = result) do
    row = deserialize_raw_rows(<<to_be_merged::bitstring, @pk_tag_marker::bitstring, row::bitstring>>)
    result
    |> Map.put(:rows, [row | rows])
    |> Map.put(:to_be_merged, <<>>)
  end
  defp do_slice_row_binary(_, row, %{to_be_merged: <<>>} = result) do
    Map.put(result, :to_be_merged, row)
  end
  defp do_slice_row_binary(_, row, %{to_be_merged: to_be_merged} = result) do
    Map.put(result, :to_be_merged, <<to_be_merged::bitstring, @pk_tag_marker::bitstring, row::bitstring>>)
  end

  defp deserialize_raw_rows(row_values) do
    {primary_keys, attribute_columns} = deserialize_row_data(<<@pk_tag_marker, row_values::bitstring>>)
    debug(fn ->
      [
        "\nprimary_keys:\s",
        inspect(primary_keys),
        ?\n,
        "attribute_columns:\s",
        inspect(attribute_columns)
      ]
    end)
    {primary_keys, attribute_columns}
  end

  defp deserialize_row_data(values) do
    row_data_parts = :binary.split(values, @row_data_marker, [:global])

    matched_index =
      Enum.find_index(row_data_parts, fn(part_value) ->
        size = byte_size(part_value)
        if size >= 2 do
          binary_part(part_value, size - 2, 1) == <<@tag_cell_checksum::integer>>
        else
          false
        end
      end)

    debug(fn ->
      [
        "\ndeserialize_row_data:\n",
        inspect(values, limit: :infinity),
        ?\n,
        "matched_index:\n",
        ?\n,
        inspect(matched_index),
        "row_data_parts:\n"
        | inspect(row_data_parts, limit: :infinity)
      ]
    end)

    deserialize_row_data_with_match_index(matched_index, values, row_data_parts)
  end

  defp deserialize_row_data_with_match_index(nil, <<(<<@tag_row_pk::integer>>), primary_keys_binary_rest::binary>>, _) do
    {deserialize_process_primary_keys(primary_keys_binary_rest), nil}
  end
  defp deserialize_row_data_with_match_index(nil, <<(<<@tag_row_data::integer>>), attribute_columns_binary_rest::binary>>, _) do
    {nil, deserialize_process_columns(attribute_columns_binary_rest)}
  end
  defp deserialize_row_data_with_match_index(nil, values, _) do
    debug(fn ->
      [
        "\n** unexcepted row data when deserialize_row_data:\s",
        inspect(values, limit: :infinity)
      ]
    end)

    nil
  end
  defp deserialize_row_data_with_match_index(matched_index, _values, row_data_parts) do
    primary_keys_binary =
      row_data_parts |> Enum.slice(0..matched_index) |> Enum.join(@row_data_marker)

    attribute_columns_binary =
      <<@tag_cell::integer, @tag_cell_name::integer,
        row_data_parts
        |> Enum.slice((matched_index + 1)..-1)
        |> Enum.join(@row_data_marker)::bitstring>>

    primary_keys_binary =
      case primary_keys_binary do
        <<(<<@tag_row_pk::integer>>), primary_keys_binary_rest::binary>> ->
          primary_keys_binary_rest

        _ ->
          raise ExAliyunOts.RuntimeError,
                "Unexcepted row data when processing primary_keys: #{
                  inspect(primary_keys_binary, limit: :infinity)
                }"
      end

    {
      deserialize_process_primary_keys(primary_keys_binary),
      deserialize_process_columns(attribute_columns_binary),
    }
  end

  defp deserialize_process_primary_keys(primary_keys_binary) do
    primary_keys_binary |> do_deserialize_process_primary_keys([]) |> Enum.reverse()
  end

  defp do_deserialize_process_primary_keys("", result) do
    result
  end

  defp do_deserialize_process_primary_keys(
         <<(<<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>), primary_key_size::little-integer-size(32), rest::binary>> =
           primary_keys,
         result
       ) do
    debug(fn ->
      [
        "\n** deserializing primary_keys, prepared result:\n",
        inspect(result),
        ?\n,
        "pk data:\s"
        | inspect(primary_keys, limit: :infinity)
      ]
    end)

    primary_key_name = binary_part(rest, 0, primary_key_size)

    rest_primary_key_value_and_other_pk =
      binary_part(rest, primary_key_size, byte_size(rest) - primary_key_size)

    debug(fn ->
      [
        "\nget primary_key_name:\s",
        inspect(primary_key_name),
        ?\n,
        "rest_primary_key_value_and_other_pk:\s"
        | inspect(rest_primary_key_value_and_other_pk, limit: :infinity)
      ]
    end)

    case calculate_tag_cell_index(rest_primary_key_value_and_other_pk) do
      next_cell_index when is_integer(next_cell_index) ->
        value_binary = binary_part(rest_primary_key_value_and_other_pk, 0, next_cell_index)
        primary_key_value = deserialize_process_primary_key_value(value_binary)

        result = [{primary_key_name, primary_key_value} | result]

        other_pk =
          binary_part(
            rest_primary_key_value_and_other_pk,
            next_cell_index,
            byte_size(rest_primary_key_value_and_other_pk) - next_cell_index
          )

        debug(fn ->
          [
            "\nfind next_cell_index:\s",
            next_cell_index,
            ?\n,
            "get primary_key_value:\s",
            inspect(primary_key_value),
            ?\n,
            "rest to be deserialized data:\s"
            | inspect(other_pk, limit: :infinity)
          ]
        end)

        do_deserialize_process_primary_keys(other_pk, result)

      nil ->
        primary_key_value =
          deserialize_process_primary_key_value(rest_primary_key_value_and_other_pk)

        debug(fn ->
          [
            "\nno more cells to deserialized, primary_key_value:\n",
            inspect(primary_key_value)
          ]
        end)

        [{primary_key_name, primary_key_value} | result]
    end
  end

  defp do_deserialize_process_primary_keys(primary_keys, result) do
    debug(fn ->
      [
        "\n** deserializing primary_keys, prepared result:\n",
        inspect(result),
        ?\n,
        "pk data:\s"
        | inspect(primary_keys, limit: :infinity)
      ]
    end)

    result
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, <<value::little-integer-size(64)>>, (<<_rest::binary>>)>>
       ) do
    value
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, (<<value::little-integer-size(64)>>)>>
       ) do
    value
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted integer value as primary value: #{inspect(rest)}"
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<total_size::little-integer-size(32)>>,
           <<@vt_string::integer>>, <<_value_size::little-integer-size(32)>>,
           (<<value::binary>>)>>
       ) do
    value_size = total_size - @little_endian_32_size - 1
    binary_part(value, 0, value_size)
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<total_size::little-integer-size(32)>>,
           <<@vt_blob::integer>>, <<_value_size::little-integer-size(32)>>, (<<value::binary>>)>>
       ) do
    value_size = total_size - @little_endian_32_size - 1
    binary_part(value, 0, value_size)
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted string value as primary value: #{inspect(rest)}"
  end

  defp deserialize_process_primary_key_value(
         <<(<<@tag_cell_value::integer>>), (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted value as primary value: #{inspect(rest)}"
  end

  defp deserialize_process_columns(attribute_columns) do
    debug(fn ->
      [
        "\n>>>> attribute_columns <<<<\n",
        inspect(attribute_columns, limit: :infinity)
      ]
    end)

    attribute_columns |> do_deserialize_process_columns([]) |> Enum.reverse()
  end

  defp do_deserialize_process_columns(<<(<<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>), column_name_size::little-integer-size(32), rest::binary>>, result) do
    column_name = binary_part(rest, 0, column_name_size)

    rest_value_and_other_columns =
      binary_part(rest, column_name_size, byte_size(rest) - column_name_size)

    case calculate_tag_cell_index(rest_value_and_other_columns) do
      next_cell_index when is_integer(next_cell_index) ->
        value_binary = binary_part(rest_value_and_other_columns, 0, next_cell_index)

        debug(fn ->
          [
            "\ncolumn_name:\s",
            inspect(column_name),
            ?\n,
            "value_binary:\s",
            inspect(value_binary, limit: :infinity),
            "\nfind next_cell_index:\s",
            next_cell_index
          ]
        end)

        {column_value, timestamp} =
          deserialize_process_column_value_with_checksum(value_binary)

        result = [{column_name, column_value, timestamp} | result]

        other_attribute_columns =
          binary_part(
            rest_value_and_other_columns,
            next_cell_index,
            byte_size(rest_value_and_other_columns) - next_cell_index
          )

        do_deserialize_process_columns(other_attribute_columns, result)

      nil ->
        {column_value, timestamp} = deserialize_process_column_value_with_checksum(rest_value_and_other_columns)

        debug(fn ->
          [
            "\ncolumn_name:\s",
            inspect(column_name),
            "\ncolumn_value:\s",
            inspect(column_value),
            "\n=== not find next_cell_index ===\n"
          ]
        end)

        [{column_name, column_value, timestamp} | result]
    end
  end
  defp do_deserialize_process_columns(_, result) do
    result
  end

  defp deserialize_process_column_value_timestamp(
         <<(<<@tag_cell_timestamp::integer>>), (<<timestamp::little-integer-size(64)>>)>>
       ) do
    timestamp
  end

  defp deserialize_process_column_value_timestamp(
         <<(<<@tag_cell_timestamp::integer>>), <<timestamp::little-integer-size(64)>>,
           _rest::binary>>
       ) do
    timestamp
  end

  defp deserialize_process_column_value_timestamp(_) do
    nil
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
           <<@vt_boolean::integer>>, <<value::integer>>, (<<timestamp_rest::binary>>)>>
       ) do
    value_boolean = integer_to_boolean(value)
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value_boolean, timestamp}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
           <<@vt_boolean::integer>>, (<<value::integer>>)>>
       ) do
    value_boolean = integer_to_boolean(value)
    {value_boolean, nil}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<2::little-integer-size(32)>>,
           <<@vt_boolean::integer>>, (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Invalid boolean value as: #{inspect(rest)}"
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, <<value::little-integer-size(64)>>,
           (<<timestamp_rest::binary>>)>>
       ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, (<<value::little-integer-size(64)>>)>>
       ) do
    {value, nil}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_integer::integer>>, (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Invalid integer value as: #{inspect(rest)}"
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_double::integer>>, <<value::float-little>>, (<<timestamp_rest::binary>>)>>
       ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_double::integer>>, (<<value::float-little>>)>>
       ) do
    {value, nil}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<@sum_endian_64_size::little-integer-size(32)>>,
           <<@vt_double::integer>>, (<<rest::binary>>)>>
       ) do
    raise ExAliyunOts.RuntimeError, "Invalid float value as: #{inspect(rest)}"
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_string::integer>>,
           <<value_size::little-integer-size(32), value::binary-size(value_size)>>,
           (<<timestamp_rest::binary>>)>>
       ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_string::integer>>,
           (<<value_size::little-integer-size(32), value::binary-size(value_size)>>)>>
       ) do
    {value, nil}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_string::integer>>, rest::binary>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted string value as: #{inspect(rest)}"
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_blob::integer>>,
           <<value_size::little-integer-size(32), value::binary-size(value_size)>>,
           (<<timestamp_rest::binary>>)>>
       ) do
    timestamp = deserialize_process_column_value_timestamp(timestamp_rest)
    {value, timestamp}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_blob::integer>>,
           (<<value_size::little-integer-size(32), value::binary-size(value_size)>>)>>
       ) do
    {value, nil}
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), <<_total_size::little-integer-size(32)>>,
           <<@vt_blob::integer>>, rest::binary>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted string value as: #{inspect(rest)}"
  end

  defp deserialize_process_column_value_with_checksum(
         <<(<<@tag_cell_value::integer>>), rest::binary>>
       ) do
    raise ExAliyunOts.RuntimeError, "Unexcepted value as: #{inspect(rest)}"
  end

  def calculate_tag_cell_index(values) do
    splited =
      :binary.split(values, <<(<<@tag_cell::integer>>), (<<@tag_cell_name::integer>>)>>, [:global])

    index =
      Enum.find_index(splited, fn(item) ->
        size = byte_size(item)
        if size >= 2 do
          binary_part(item, size - 2, 1) == <<@tag_cell_checksum::integer>>
        else
          false
        end
      end)

    debug(fn ->
      [
        "\ncalculate_tag_cell_index:\s",
        inspect(values, limit: :infinity),
        "\nsplited:\s",
        inspect(splited, limit: :infinity),
        "\nindex:\s",
        inspect(index)
      ]
    end)

    if index == nil do
      nil
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
