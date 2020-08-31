defmodule ExAliyunOts.Client.Table do
  @moduledoc false

  alias ExAliyunOts.TableStore.{
    CreateTableRequest,
    PrimaryKeySchema,
    TableMeta,
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

  alias ExAliyunOts.{Var, Http}
  alias ExAliyunOts.Const.PKType
  import ExAliyunOts.Logger, only: [debug: 1]

  require PKType

  def request_to_create_table(var_create_table) do
    primary_key_list = Enum.map(var_create_table.primary_keys, &map_primary_key_schema/1)

    table_meta =
      TableMeta.new(table_name: var_create_table.table_name, primary_key: primary_key_list)

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
      table_options: table_options
    )
    |> put_stream_spec(var_create_table.stream_spec)
    |> CreateTableRequest.encode()
  end

  def remote_create_table(instance, request_body) do
    result =
      instance
      |> Http.client("/CreateTable", request_body, nil)
      |> Http.post()

    debug([
      "create_table result: ",
      inspect(result)
    ])

    result
  end

  def request_to_list_table() do
    ListTableRequest.encode(ListTableRequest.new())
  end

  def remote_list_table(instance, request_body) do
    result =
      instance
      |> Http.client("/ListTable", request_body, &ListTableResponse.decode/1)
      |> Http.post()

    debug([
      "list_table result: ",
      inspect(result)
    ])

    result
  end

  def request_to_delete_table(table_name) do
    DeleteTableRequest.encode(DeleteTableRequest.new(table_name: table_name))
  end

  def remote_delete_table(instance, request_body) do
    result =
      instance
      |> Http.client("/DeleteTable", request_body, nil)
      |> Http.post()

    debug([
      "delete_table result: ",
      inspect(result)
    ])

    result
  end

  def request_to_update_table(var_update_table) do
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

  def remote_update_table(instance, request_body) do
    result =
      instance
      |> Http.client("/UpdateTable", request_body, &UpdateTableResponse.decode/1)
      |> Http.post()

    debug([
      "update_table result: ",
      inspect(result)
    ])

    result
  end

  def request_to_describe_table(table_name) do
    DescribeTableRequest.encode(DescribeTableRequest.new(table_name: table_name))
  end

  def remote_describe_table(instance, request_body) do
    result =
      instance
      |> Http.client("/DescribeTable", request_body, &DescribeTableResponse.decode/1)
      |> Http.post()

    debug([
      "describe_table result: ",
      inspect(result)
    ])

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

  defp map_primary_key_schema({key_name, :integer}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer())
  end

  defp map_primary_key_schema({key_name, :string}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.string())
  end

  defp map_primary_key_schema({key_name, :binary}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.binary())
  end

  defp map_primary_key_schema({key_name, :auto_increment}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer(), option: PKType.auto_increment())
  end

  defp map_primary_key_schema({key_name, _, :auto_increment}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer(), option: PKType.auto_increment())
  end

  defp map_primary_key_schema({key_name, PKType.integer()}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer())
  end

  defp map_primary_key_schema({key_name, PKType.string()}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.string())
  end

  defp map_primary_key_schema({key_name, PKType.binary()}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.binary())
  end

  defp map_primary_key_schema({key_name, PKType.auto_increment()}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer(), option: PKType.auto_increment())
  end

  defp map_primary_key_schema({key_name, _, PKType.auto_increment()}) do
    PrimaryKeySchema.new(name: key_name, type: PKType.integer(), option: PKType.auto_increment())
  end

  defp map_primary_key_schema(primary_key) do
    raise ExAliyunOts.RuntimeError, "Invalid primary_key #{inspect(primary_key)}"
  end
end
