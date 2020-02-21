defmodule ExAliyunOts.Utils do
  @moduledoc """
  Common tools.
  """

  @geo_point_reg ~r/\-?\d+\.?\d*,\-?\d+\.?\d*/

  defmodule Guards do
    @moduledoc """
    Define some custom guard expressions.
    """

    defguard is_valid_primary_key_type(type) when type in [:integer, :binary, :string]

    defguard is_valid_string(value) when value != nil and value != "" and is_bitstring(value)

    defguard is_valid_table_ttl(value) when is_integer(value) and (value == -1 or value >= 86_400)

    defguard is_valid_input_columns(columns) when is_list(columns) or is_map(columns)

  end
  
  def valid_geo_point?(point) when is_bitstring(point) do
    Regex.match?(@geo_point_reg, point)
  end

  def row_to_map({pks, attrs}) when is_list(pks) and is_list(attrs) do
    %{}
    |> do_reduce_pks(pks)
    |> do_reduce_attrs(attrs)
  end

  def row_to_map({pks, nil}) when is_list(pks) do
    do_reduce_pks(%{}, pks)
  end

  def row_to_map({nil, attrs}) when is_list(attrs) do
    do_reduce_attrs(%{}, attrs)
  end

  def row_to_map(nil) do
    %{}
  end

  def row_to_map(row) do
    raise ExAliyunOts.RuntimeError, "Fail to transfer invalid row: #{inspect row} to map."
  end

  def attrs_to_row(attrs) when is_list(attrs) do
    Enum.reduce(attrs, [], &assemble_attribute_column/2)
  end

  def attrs_to_row(attrs) when is_map(attrs) do
    Enum.reduce(attrs, [], &assemble_attribute_column/2)
  end

  def attrs_to_row(attrs) do
    raise ExAliyunOts.RuntimeError, "Fail to transfer invalid attributes: #{inspect attrs} to row attributes column(s), expect it is a Map or Keyword."
  end

  defp do_reduce_pks(acc, items) do
    Enum.reduce(items, acc, fn({k, v}, acc) ->
      Map.put(acc, String.to_atom(k), v)
    end)
  end

  defp do_reduce_attrs(acc, items) do
    Enum.reduce(items, acc, fn({k, v, _ts}, acc) ->
      Map.put(acc, String.to_atom(k), v)
    end)
  end

  defp value_to_attribute_column(value) when is_map(value) or is_list(value) do
    Jason.encode!(value)
  end
  defp value_to_attribute_column(value) do
    value
  end

  defp assemble_attribute_column({key, value}, acc) when is_atom(key) do
    value = value_to_attribute_column(value)
    if value == nil, do: acc, else: acc ++ [{Atom.to_string(key), value}]
  end
  defp assemble_attribute_column({key, value}, acc) when is_bitstring(key) do
    value = value_to_attribute_column(value)
    if value == nil, do: acc, else: acc ++ [{key, value}]
  end
  defp assemble_attribute_column(_, acc) do
    acc
  end

end
