defmodule ExAliyunOts.Client.Table do
  @moduledoc false

  alias ExAliyunOts.TableStore.{
    CreateTableRequest,
    PrimaryKeySchema,
    PrimaryKeyType,
    DefinedColumnSchema,
    DefinedColumnType,
    TableMeta,
    CreateIndexRequest,
    DropIndexRequest,
    ReservedThroughput,
    CapacityUnit,
    TableOptions,
    ListTableRequest,
    ListTableResponse,
    DeleteTableRequest,
    UpdateTableRequest,
    UpdateTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    StreamSpecification
  }

  alias ExAliyunOts.{Var, Http, Utils}
  import ExAliyunOts.Logger, only: [debug: 1]

  defp request_to_create_table(var_create_table) do
    primary_key_list = Enum.map(var_create_table.primary_keys, &map_primary_key_schema/1)
    defined_column_list = Enum.map(var_create_table.defined_columns, &map_defined_column_schema/1)

    table_meta =
      TableMeta.new(
        table_name: var_create_table.table_name,
        primary_key: primary_key_list,
        defined_column: defined_column_list
      )

    cu =
      throughput_to_cu(
        var_create_table.reserved_throughput_read,
        var_create_table.reserved_throughput_write
      )

    reserved_throughput = ReservedThroughput.new(capacity_unit: cu)

    table_options =
      TableOptions.new(
        time_to_live: var_create_table.time_to_live,
        max_versions: var_create_table.max_versions,
        deviation_cell_version_in_sec: var_create_table.deviation_cell_version_in_sec
      )

    CreateTableRequest.new(
      table_meta: table_meta,
      reserved_throughput: reserved_throughput,
      table_options: table_options,
      index_metas: var_create_table.index_metas
    )
    |> put_stream_spec(var_create_table.stream_spec)
    |> CreateTableRequest.encode()
  end

  def remote_create_table(instance, var_create_table) do
    request_body = request_to_create_table(var_create_table)

    result =
      instance
      |> Http.client("/CreateTable", request_body, nil)
      |> Http.post()

    debug(["create_table result: ", inspect(result)])

    result
  end

  def remote_create_index(instance, create_index_request) do
    request_body = CreateIndexRequest.encode(create_index_request)

    result =
      instance
      |> Http.client("/CreateIndex", request_body, nil)
      |> Http.post()

    debug(["create_index result: ", inspect(result)])

    result
  end

  def remote_delete_index(instance, table_name, index_name) do
    request_body =
      %DropIndexRequest{
        main_table_name: table_name,
        index_name: index_name
      }
      |> DropIndexRequest.encode()

    result =
      instance
      |> Http.client("/DropIndex", request_body, nil)
      |> Http.post()

    debug(["drop_index result: ", inspect(result)])

    result
  end

  def remote_list_table(instance) do
    request_body = ListTableRequest.new() |> ListTableRequest.encode()

    result =
      instance
      |> Http.client("/ListTable", request_body, &ListTableResponse.decode/1)
      |> Http.post()

    debug(["list_table result: ", inspect(result)])

    result
  end

  def remote_delete_table(instance, table_name) do
    request_body = DeleteTableRequest.new(table_name: table_name) |> DeleteTableRequest.encode()

    result =
      instance
      |> Http.client("/DeleteTable", request_body, nil)
      |> Http.post()

    debug(["delete_table result: ", inspect(result)])

    result
  end

  defp request_to_update_table(var_update_table) do
    reserved_throughput_read = var_update_table.reserved_throughput_read
    reserved_throughput_write = var_update_table.reserved_throughput_write
    cu = throughput_to_cu(reserved_throughput_read, reserved_throughput_write)
    reserved_throughput = ReservedThroughput.new(capacity_unit: cu)

    table_options =
      TableOptions.new(
        time_to_live: var_update_table.time_to_live,
        max_versions: var_update_table.max_versions,
        deviation_cell_version_in_sec: var_update_table.deviation_cell_version_in_sec
      )

    UpdateTableRequest.new(
      table_name: var_update_table.table_name,
      reserved_throughput: reserved_throughput,
      table_options: table_options
    )
    |> put_stream_spec(var_update_table.stream_spec)
    |> UpdateTableRequest.encode()
  end

  def remote_update_table(instance, var_update_table) do
    request_body = request_to_update_table(var_update_table)

    result =
      instance
      |> Http.client("/UpdateTable", request_body, &UpdateTableResponse.decode/1)
      |> Http.post()

    debug(["update_table result: ", inspect(result)])

    result
  end

  def remote_describe_table(instance, table_name) do
    request_body =
      DescribeTableRequest.new(table_name: table_name) |> DescribeTableRequest.encode()

    result =
      instance
      |> Http.client("/DescribeTable", request_body, &DescribeTableResponse.decode/1)
      |> Http.post()

    debug(["describe_table result: ", inspect(result)])

    result
  end

  defp throughput_to_cu(read, write) when is_integer(read) and is_integer(write) do
    CapacityUnit.new(read: read, write: write)
  end

  defp throughput_to_cu(read, write) when read == nil and is_integer(write) do
    CapacityUnit.new(write: write)
  end

  defp throughput_to_cu(read, write) when is_integer(read) and write == nil do
    CapacityUnit.new(read: read)
  end

  defp throughput_to_cu(read, write) do
    raise ExAliyunOts.RuntimeError,
          "Invalid throughput setting, at least set an integer for read or write, but setting read: #{
            inspect(read)
          }, write: #{inspect(write)}"
  end

  defp put_stream_spec(
         request,
         %Var.StreamSpec{is_enabled: true, expiration_time: stream_expiration_time}
       )
       when is_integer(stream_expiration_time) and
              (stream_expiration_time >= 1 and stream_expiration_time <= 24) do
    stream_spec =
      StreamSpecification.new(
        enable_stream: true,
        expiration_time: stream_expiration_time
      )

    %{request | stream_spec: stream_spec}
  end

  defp put_stream_spec(request, %Var.StreamSpec{is_enabled: false}) do
    stream_spec = StreamSpecification.new(enable_stream: false)
    %{request | stream_spec: stream_spec}
  end

  defp put_stream_spec(request, %Var.StreamSpec{is_enabled: nil}) do
    request
  end

  defp put_stream_spec(_request, spec) do
    raise ExAliyunOts.RuntimeError,
          "Invalid stream_spec #{inspect(spec)}, is_enabled should be boolean and expiration_time should be an integer and in (1, 24)"
  end

  defp map_defined_column_schema({key_name, :binary}),
    do: DefinedColumnSchema.new(name: key_name, type: :DCT_BLOB)

  DefinedColumnType.mapping()
  |> Map.keys()
  |> Enum.map(fn type ->
    downcase_type =
      type |> to_string() |> String.slice(4..-1) |> String.downcase() |> String.to_atom()

    defp map_defined_column_schema({key_name, unquote(downcase_type)}),
      do: DefinedColumnSchema.new(name: key_name, type: unquote(type))

    defp map_defined_column_schema({key_name, unquote(type)}),
      do: DefinedColumnSchema.new(name: key_name, type: unquote(type))
  end)

  defp map_defined_column_schema(defined_column) do
    raise ExAliyunOts.RuntimeError, "Invalid defined_column #{inspect(defined_column)}"
  end

  PrimaryKeyType.mapping()
  |> Map.keys()
  |> Enum.map(fn type ->
    defp map_primary_key_schema({key_name, unquote(Utils.downcase_atom(type))}),
      do: PrimaryKeySchema.new(name: key_name, type: unquote(type))

    defp map_primary_key_schema({key_name, unquote(type)}),
      do: PrimaryKeySchema.new(name: key_name, type: unquote(type))
  end)

  defp map_primary_key_schema({key_name, type}) when type in [:auto_increment, :AUTO_INCREMENT] do
    PrimaryKeySchema.new(name: key_name, type: :INTEGER, option: :AUTO_INCREMENT)
  end

  defp map_primary_key_schema({key_name, _type, option})
       when option in [:auto_increment, :AUTO_INCREMENT] do
    PrimaryKeySchema.new(name: key_name, type: :INTEGER, option: :AUTO_INCREMENT)
  end

  defp map_primary_key_schema(primary_key) do
    raise ExAliyunOts.RuntimeError, "Invalid primary_key #{inspect(primary_key)}"
  end
end
