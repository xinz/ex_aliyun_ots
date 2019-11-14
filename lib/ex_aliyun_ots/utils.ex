defmodule ExAliyunOts.Utils do
  @moduledoc """
  Common tools.
  """

  defmodule Guards do
    @moduledoc """
    Define some custom guard expressions.
    """

    defguard is_valid_primary_key_type(type) when type in [:integer, :binary, :string]

    defguard is_valid_string(value) when value != nil and value != "" and is_bitstring(value)

    defguard is_valid_table_ttl(value) when is_integer(value) and (value == -1 or value >= 86_400)

    defguard is_valid_input_columns(columns) when is_list(columns) or is_map(columns)

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
    Enum.reduce(attrs, [], fn
      {key, value}, acc when is_atom(key) ->
        acc ++ [{Atom.to_string(key), value_to_attribute_column(value)}]

      {key, value}, acc when is_bitstring(key) ->
        acc ++ [{key, value_to_attribute_column(value)}]

      _, acc ->
        acc
    end)
  end

  def attrs_to_row(attrs) when is_map(attrs) do
    Map.keys(attrs)
    |> Enum.sort()
    |> Enum.reduce([], fn
      key, acc when is_atom(key) ->
        value = attrs |> Map.get(key) |> value_to_attribute_column()
        acc ++ [{Atom.to_string(key), value}]

      key, acc when is_bitstring(key) ->
        value = attrs |> Map.get(key) |> value_to_attribute_column()
        acc ++ [{key, value}]

      _, acc ->
        acc
    end)
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

  defp value_to_attribute_column(nil) do
    ""
  end
  defp value_to_attribute_column(value) when is_map(value) or is_list(value) do
    Jason.encode!(value)
  end
  defp value_to_attribute_column(value) do
    value
  end

end
