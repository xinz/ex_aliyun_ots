defmodule ExAliyunOts.PlainBuffer.Commit_3a4a442 do
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

  @sum_endian_32_size @little_endian_32_size + 1
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
    process_row_checksum(buffer, row_checksum)
  end

  def serialize_primary_keys(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys)
    row_checksum = CRC.crc_int8(row_checksum, 0)
    process_row_checksum(buffer, row_checksum)
  end

  def serialize_column_value(value) when is_boolean(value) do
    <<@vt_boolean::integer, boolean_to_integer(value)::bitstring>>
  end

  def serialize_column_value(value) when is_integer(value) do
    <<@vt_integer::integer, value::little-integer-size(64)>>
  end

  def serialize_column_value(value) when is_binary(value) do
    value_size = byte_size(value)
    <<@vt_string::integer, value_size::little-integer-size(32), value::binary>>
  end

  def serialize_column_value(value) when is_bitstring(value) do
    value_size = byte_size(value)
    <<@vt_blob::integer, value_size::little-integer-size(32), value::binary>>
  end

  def serialize_column_value(value) when is_float(value) do
    <<@vt_double::integer, value::little-float>>
  end

  def serialize_column_value(value) do
    raise ExAliyunOts.RuntimeError, "Unsupported column for value: #{inspect(value)}"
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
    process_row_checksum(buffer, row_checksum)
  end

  def serialize_for_delete_row(primary_keys) do
    {buffer, row_checksum} = header() |> primary_keys(primary_keys) |> process_delete_marker()
    process_row_checksum(buffer, row_checksum)
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

    decode_row(row)
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

    decode_rows(rows)
  end

  defp header() do
    # row_checksum initialized value of header is 0
    {<<@header::little-integer-size(32)>>, 0}
  end

  defp primary_keys({buffer, row_checksum}, primary_keys) do
    buffer = <<buffer::bitstring, @tag_row_pk::integer>>
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
    buffer = <<buffer::bitstring, @tag_row_data::integer>>
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
    buffer = <<buffer::bitstring, @tag_row_data::integer>>

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
    buffer = <<buffer::bitstring, @tag_cell::integer>>

    {buffer, cell_checksum} =
      {buffer, 0}
      |> process_cell_name(pk_name)
      |> process_primary_key_value(pk_value)

    buffer = <<buffer::bitstring, @tag_cell_checksum::integer, cell_checksum::integer>>

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
      <<buffer::bitstring, @tag_cell_name::integer,
        <<String.length(name)::little-integer-size(32)>>, name::bitstring>>

    cell_checksum = CRC.crc_string(cell_checksum, name)
    {buffer, cell_checksum}
  end

  defp process_primary_key_value({buffer, cell_checksum}, value) do
    do_process_primary_key_value(
      {<<buffer::bitstring, @tag_cell_value::integer>>, cell_checksum},
      value
    )
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
       when value == PKType.inf_min()
       when value == :inf_min do
    buffer = <<buffer::bitstring, <<1::little-integer-size(32)>>, @vt_inf_min::integer>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_min)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
       when value == PKType.inf_max()
       when value == :inf_max do
    buffer = <<buffer::bitstring, <<1::little-integer-size(32)>>, @vt_inf_max::integer>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_inf_max)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value)
       when value == PKType.auto_increment() do
    buffer = <<buffer::bitstring, <<1::little-integer-size(32)>>, @vt_auto_increment::integer>>

    cell_checksum = CRC.crc_int8(cell_checksum, @vt_auto_increment)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_integer(value) do
    buffer =
      <<buffer::bitstring, <<@sum_endian_64_size::little-integer-size(32)>>, @vt_integer::integer,
        (<<value::little-integer-size(64)>>)>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_binary(value) do
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<@sum_endian_32_size + value_size::little-integer-size(32)>>,
        @vt_string::integer, <<value_size::little-integer-size(32)>>, value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_string)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp do_process_primary_key_value({buffer, cell_checksum}, value) when is_bitstring(value) do
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<@sum_endian_32_size + value_size::little-integer-size(32)>>,
        @vt_blob::integer, <<value_size::little-integer-size(32)>>, value::bitstring>>

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
    buffer = <<buffer::bitstring, @tag_cell::integer>>

    {buffer, cell_checksum} =
      {buffer, 0}
      |> process_cell_name(column_name)
      |> process_column_value_with_checksum(column_value)

    buffer = <<buffer::bitstring, @tag_cell_checksum::integer, cell_checksum::integer>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_column({column_name, column_value, timestamp}, {buffer, row_checksum})
       when timestamp != nil do
    buffer = <<buffer::bitstring, @tag_cell::integer>>

    {buffer, cell_checksum} =
      {buffer, 0}
      |> process_cell_name(column_name)
      |> process_column_value_with_checksum(column_value)

    buffer =
      <<buffer::bitstring, @tag_cell_timestamp::integer,
        (<<timestamp::little-integer-size(64)>>)>>

    cell_checksum = CRC.crc_int64(cell_checksum, timestamp)

    buffer = <<buffer::bitstring, @tag_cell_checksum::integer, cell_checksum::integer>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_column(column, _) do
    raise ExAliyunOts.RuntimeError, "Invalid column: #{inspect(column)} is not a tuple"
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, nil) do
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, true) do
    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_boolean)
      |> CRC.crc_int8(1)

    {
      boolean_value_to_buffer(buffer, true),
      cell_checksum
    }
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, false) do
    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_boolean)
      |> CRC.crc_int8(0)

    {
      boolean_value_to_buffer(buffer, false),
      cell_checksum
    }
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value)
       when is_integer(value) do
    buffer =
      <<buffer::bitstring, @tag_cell_value::integer,
        <<@sum_endian_64_size::little-integer-size(32)>>, <<@vt_integer::integer>>,
        (<<value::little-integer-size(64)>>)>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_integer) |> CRC.crc_int64(value)
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value) when is_float(value) do
    buffer = <<buffer::bitstring, @tag_cell_value::integer>>
    value_to_binary = <<value::little-float>>
    <<long::unsigned-little-integer-64>> = value_to_binary

    buffer =
      <<buffer::bitstring, <<@sum_endian_64_size::little-integer-size(32)>>, @vt_double::integer,
        value_to_binary::bitstring>>

    cell_checksum = cell_checksum |> CRC.crc_int8(@vt_double) |> CRC.crc_int64(long)
    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value) when is_binary(value) do
    buffer = <<buffer::bitstring, @tag_cell_value::integer>>
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<@sum_endian_32_size + value_size::little-integer-size(32)>>,
        @vt_string::integer, <<value_size::little-integer-size(32)>>, value::bitstring>>

    cell_checksum =
      cell_checksum
      |> CRC.crc_int8(@vt_string)
      |> CRC.crc_int32(value_size)
      |> CRC.crc_string(value)

    {buffer, cell_checksum}
  end

  defp process_column_value_with_checksum({buffer, cell_checksum}, value)
       when is_bitstring(value) do
    buffer = <<buffer::bitstring, @tag_cell_value::integer>>
    value_size = byte_size(value)

    buffer =
      <<buffer::bitstring, <<@sum_endian_32_size + value_size::little-integer-size(32)>>,
        @vt_blob::integer, <<value_size::little-integer-size(32)>>, value::bitstring>>

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

  defp boolean_value_to_buffer(buffer, value) when is_boolean(value) do
    <<buffer::bitstring, @tag_cell_value::integer, <<2::little-integer-size(32)>>,
      @vt_boolean::integer, boolean_to_integer(value)::bitstring>>
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

    buffer = <<buffer::bitstring, @tag_cell_type::integer, @op_delete_one_version::integer>>

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

    buffer = <<buffer::bitstring, @tag_cell_type::integer, @op_delete_all_version::integer>>

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

    buffer = <<buffer::bitstring, @tag_cell_type::integer, @op_increment::integer>>

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
    buffer = <<buffer::bitstring, @tag_cell::integer>>

    {buffer, 0}
    |> process_cell_name(column_name)
    |> process_column_value_with_checksum(column_value)
  end

  defp process_update_column_with_timestamp(buffer, cell_checksum, nil) do
    {buffer, cell_checksum}
  end

  defp process_update_column_with_timestamp(buffer, cell_checksum, timestamp) do
    buffer =
      <<buffer::bitstring, @tag_cell_timestamp::integer,
        (<<timestamp::little-integer-size(64)>>)>>

    cell_checksum = CRC.crc_int64(cell_checksum, timestamp)
    {buffer, cell_checksum}
  end

  defp process_update_column_with_row_checksum(buffer, cell_checksum, row_checksum) do
    buffer = <<buffer::bitstring, @tag_cell_checksum::integer, cell_checksum::integer>>

    row_checksum = CRC.crc_int8(row_checksum, cell_checksum)
    {buffer, row_checksum}
  end

  defp process_delete_marker({buffer, row_checksum}) do
    {
      <<buffer::bitstring, @tag_delete_row_marker::integer>>,
      CRC.crc_int8(row_checksum, 1)
    }
  end

  defp boolean_to_integer(true), do: <<1>>
  defp boolean_to_integer(_), do: <<0>>

  defp process_row_checksum(buffer, row_checksum) do
    <<buffer::bitstring, @tag_row_checksum::integer, row_checksum::integer>>
  end

  # deserialize processing

  defp decode_row(<<@header::little-integer-size(32), rest::binary>>) do
    start_decoding(rest)
  end

  defp decode_rows(<<@header::little-integer-size(32), rest::binary>>) do
    decode_rows(rest, [])
  end

  defp decode_rows(<<>>, acc) do
    Enum.reverse(acc)
  end

  defp decode_rows(rest, acc) do
    case start_decoding(rest) do
      {:cont, rest, row} ->
        decode_rows(rest, [row | acc])

      row ->
        decode_rows(<<>>, [row | acc])
    end
  end

  defp start_decoding(<<@tag_row_pk::integer, rest::binary>>) do
    # start decoding from primary key(s)
    decode_pk(rest, [])
  end

  defp start_decoding(<<@tag_row_data::integer, rest::binary>>) do
    # no primary key(s) decoding, start decoding from attribute column(s)
    {nil, decode_attr(rest, [])}
  end

  defp decode_pk(
         <<@tag_cell::integer, @tag_cell_name::integer, pk_field_size::little-integer-size(32),
           pk_field::binary-size(pk_field_size), rest::binary>>,
         acc
       ) do
    # primary key(s) decoding
    {pk_value, rest} = calculate_pk_value(rest)
    acc = [{pk_field, pk_value} | acc]
    decode_pk(rest, acc)
  end

  defp decode_pk(<<@tag_row_data::integer, rest::binary>>, acc) do
    # finish primary key(s) decoding and start this row's attribute column(s) decoding
    case decode_attr(rest, []) do
      {rest, attrs} ->
        {:cont, rest, {Enum.reverse(acc), attrs}}

      attrs ->
        {Enum.reverse(acc), attrs}
    end
  end

  defp decode_pk(<<@tag_row_checksum::integer, _::integer>>, acc) do
    # finish primary key(s) decoding and no attribute column(s) decoding
    {
      Enum.reverse(acc),
      nil
    }
  end

  defp decode_pk(<<@tag_row_checksum::integer, _::integer, rest::binary>>, acc) do
    # finish primary keys(s) and no attribute column(s) decoding, but still be with other row(s) decoding
    {
      :cont,
      rest,
      {
        Enum.reverse(acc),
        nil
      }
    }
  end

  defp decode_pk(_, acc) do
    # still some ignorable bytes but can finish row data decoding
    {
      Enum.reverse(acc),
      nil
    }
  end

  defp decode_attr(
         <<@tag_cell::integer, @tag_cell_name::integer, attr_field_size::little-integer-size(32),
           attr_field::binary-size(attr_field_size), rest::binary>>,
         acc
       ) do
    # attribute columns decoding
    {attr_value, timestamp, rest} = calculate_attr_value(rest)
    acc = [{attr_field, attr_value, timestamp} | acc]
    decode_attr(rest, acc)
  end

  defp decode_attr(<<@tag_row_checksum::integer, _::integer>>, acc) do
    # be with an ending flag to finish row data decoding
    Enum.reverse(acc)
  end

  defp decode_attr(_, []) do
    nil
  end

  defp decode_attr(<<@tag_row_checksum::integer, _::integer, rest::binary>>, acc) do
    # current row data is decoded but still need to process other row(s) data
    {rest, Enum.reverse(acc)}
  end

  defp decode_attr(_, acc) do
    # still some ignorable bytes but can finish row data decoding
    Enum.reverse(acc)
  end

  defp decode_attr_timestamp(
         <<@tag_cell_timestamp::integer, timestamp::little-integer-size(64),
           @tag_cell_checksum::integer, _row_crc8::integer, rest::binary>>
       ) do
    {timestamp, rest}
  end

  defp decode_attr_timestamp(<<@tag_cell_checksum::integer, _row_crc8::integer, rest::binary>>) do
    {nil, rest}
  end

  defp calculate_pk_value(
         <<@tag_cell_value::integer, _total_bytes_size::little-integer-size(32),
           @vt_integer::integer, pk_value::binary-size(8), @tag_cell_checksum::integer,
           _row_crc8::integer, rest::binary>>
       ) do
    <<value::signed-little-integer-size(64)>> = pk_value
    {value, rest}
  end

  defp calculate_pk_value(
         <<@tag_cell_value::integer, _total_bytes_size::little-integer-size(32), type::integer,
           pk_value_size::little-integer-size(32), value::binary-size(pk_value_size),
           @tag_cell_checksum::integer, _row_crc8::integer, rest::binary>>
       )
       when type == @vt_string or type == @vt_blob do
    {value, rest}
  end

  defp calculate_pk_value(
         <<@tag_cell_value::integer, _total_bytes_size::little-integer-size(32), type::integer,
           _rest::binary>> = input
       ) do
    raise ExAliyunOts.RuntimeError,
          "Unexcepted primary type as: `#{inspect(type)}` and its binary input: `#{inspect(input)}`"
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), @vt_boolean::integer,
           1::integer, rest::binary>>
       ) do
    {timestamp, rest} = decode_attr_timestamp(rest)
    {true, timestamp, rest}
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), @vt_boolean::integer,
           _::integer, rest::binary>>
       ) do
    {timestamp, rest} = decode_attr_timestamp(rest)
    {false, timestamp, rest}
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), @vt_integer::integer,
           value::signed-little-integer-size(64), rest::binary>>
       ) do
    {timestamp, rest} = decode_attr_timestamp(rest)
    {value, timestamp, rest}
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), @vt_double::integer,
           value::signed-little-float-size(64), rest::binary>>
       ) do
    {timestamp, rest} = decode_attr_timestamp(rest)
    {value, timestamp, rest}
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), type::integer,
           value_size::little-integer-size(32), value::binary-size(value_size), rest::binary>>
       )
       when type == @vt_string or type == @vt_blob do
    {timestamp, rest} = decode_attr_timestamp(rest)
    {value, timestamp, rest}
  end

  defp calculate_attr_value(
         <<@tag_cell_value::integer, _total_size::little-integer-size(32), type::integer>> = input
       ) do
    raise ExAliyunOts.RuntimeError,
          "Unexcepted attribute column type as: `#{inspect(type)}` and its binary input: `#{
            inspect(input)
          }`"
  end
end
