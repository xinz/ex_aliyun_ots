defmodule ExAliyunOts.Client.SQL do
  @moduledoc false

  alias ExAliyunOts.{Http, PlainBuffer}
  alias ExAliyunOts.TableStore.{SQLQueryRequest, SQLQueryResponse}

  def remote_sql_query(instance, query) do
    request_body =
      %SQLQueryRequest{query: query} |> SQLQueryRequest.encode!() |> IO.iodata_to_binary()

    instance
    |> Http.client("/SQLQuery", request_body, &SQLQueryResponse.decode!/1)
    |> Http.post()
    |> case do
      {:ok, response} ->
        {:ok, %{response | rows: PlainBuffer.deserialize_rows(response.rows)}}

      error ->
        error
    end
  end

  def create_mapping_table(instance, query) do
    case remote_sql_query(instance, query) do
      {:ok, %SQLQueryResponse{rows: []}} -> :ok
      error -> error
    end
  end

  def drop_mapping_table(instance, table) do
    case remote_sql_query(instance, "DROP MAPPING TABLE IF EXISTS #{table}") do
      {:ok, %SQLQueryResponse{rows: []}} -> :ok
      error -> error
    end
  end

  def describe_mapping_table(instance, table) do
    case remote_sql_query(instance, "DESCRIBE #{table}") do
      {:ok, %SQLQueryResponse{rows: rows}} ->
        map =
          for row <- rows, into: %{} do
            transfer_row(row) |> Map.pop!("Field")
          end

        {:ok, map}

      error ->
        error
    end
  end

  def alter_table_drop_column(instance, table, column) do
    case remote_sql_query(instance, "ALTER TABLE #{table} DROP COLUMN #{column}") do
      {:ok, %SQLQueryResponse{rows: []}} -> :ok
      error -> error
    end
  end

  def alter_table_add_column(instance, table, column, type) do
    case remote_sql_query(instance, "ALTER TABLE #{table} ADD COLUMN #{column} #{type}") do
      {:ok, %SQLQueryResponse{rows: []}} -> :ok
      error -> error
    end
  end

  def query(instance, query) do
    case remote_sql_query(instance, query) do
      {:ok, %SQLQueryResponse{rows: rows}} ->
        {:ok, Enum.map(rows, &transfer_row/1)}

      error ->
        error
    end
  end

  defp transfer_row({[], attrs}) do
    for {attr_key, attr_value, _ts} <- attrs, into: %{} do
      {attr_key, attr_value}
    end
  end
end
