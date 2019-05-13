defmodule ExAliyunOts.Client.Tunnel do

  alias ExAliyunOts.Http

  alias ExAliyunOts.TableStoreTunnel.{
    CreateTunnelRequest,
    CreateTunnelResponse,
    Tunnel,
    DeleteTunnelRequest,
    DeleteTunnelResponse,
    ListTunnelRequest,
    ListTunnelResponse,
    DescribeTunnelRequest,
    DescribeTunnelResponse,
    ConnectRequest,
    ClientConfig,
    ConnectResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    Channel,
    ShutdownRequest,
    ShutdownResponse,
    GetCheckpointRequest,
    GetCheckpointResponse,
    CheckpointRequest,
    CheckpointResponse,
    ReadRecordsRequest,
    ReadRecordsResponse
  }

  alias ExAliyunOts.PlainBuffer

  import ExAliyunOts.Logger, only: [debug: 1]

  def request_to_create_tunnel(var_create_tunnel) do
    tunnel = Tunnel.new(
      table_name: var_create_tunnel.table_name,
      tunnel_name: var_create_tunnel.tunnel_name,
      tunnel_type: var_create_tunnel.type
    )
    CreateTunnelRequest.new(tunnel: tunnel) |> CreateTunnelRequest.encode()
  end

  def remote_create_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/create", request_body, &CreateTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "create_tunnel result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_delete_tunnel(var_delete_tunnel) do
    [
      table_name: var_delete_tunnel.table_name,
      tunnel_name: var_delete_tunnel.tunnel_name,
      tunnel_id: var_delete_tunnel.tunnel_id
    ]
    |> DeleteTunnelRequest.new()
    |> DeleteTunnelRequest.encode()
  end

  def remote_delete_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/delete", request_body, &DeleteTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "delete_tunnel result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_list_tunnel(table_name) do
    ListTunnelRequest.new(table_name: table_name) |> ListTunnelRequest.encode()
  end

  def remote_list_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/list", request_body, &ListTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "list_tunnel result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_describe_tunnel(var_describe_tunnel) do
    [
      table_name: var_describe_tunnel.table_name,
      tunnel_name: var_describe_tunnel.tunnel_name,
      tunnel_id: var_describe_tunnel.tunnel_id
    ]
    |> DescribeTunnelRequest.new()
    |> DescribeTunnelRequest.encode()
  end

  def remote_describe_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/describe", request_body, &DescribeTunnelResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "describe_tunnel result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_connect_tunnel(var_connect_tunnel) do
    config = ClientConfig.new(timeout: var_connect_tunnel.timeout, client_tag: var_connect_tunnel.client_tag)
    [
      tunnel_id: var_connect_tunnel.tunnel_id,
      client_config: config
    ]
    |> ConnectRequest.new()
    |> ConnectRequest.encode()
  end

  def remote_connect_tunnel(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/connect", request_body, &ConnectResponse.decode/1)
      |> Http.post()

    debug(fn ->
      [
        "connect_tunnel result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_heartbeat(var_heartbeat) do
    channels =
      Enum.map(var_heartbeat.channels, fn(channel) ->
        Channel.new(
          channel_id: channel.channel_id,
          version: channel.version,
          status: channel.status
        )
      end)
    [
      tunnel_id: var_heartbeat.tunnel_id,
      client_id: var_heartbeat.client_id,
      channels: channels
    ]
    |> HeartbeatRequest.new()
    |> HeartbeatRequest.encode()
  end

  def remote_heartbeat(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/heartbeat", request_body, &HeartbeatResponse.decode/1)
      |> Http.post()
    debug(fn ->
      [
        "heartbeat result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_shutdown(var_shutdown) do
    [
      tunnel_id: var_shutdown.tunnel_id,
      client_id: var_shutdown.client_id
    ]
    |> ShutdownRequest.new()
    |> ShutdownRequest.encode()
  end

  def remote_shutdown(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/shutdown", request_body, &ShutdownResponse.decode/1)
      |> Http.post()
    debug(fn ->
      [
        "shutdown result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_get_checkpoint(var_getcheckpoint) do
    [
      tunnel_id: var_getcheckpoint.tunnel_id,
      client_id: var_getcheckpoint.client_id,
      channel_id: var_getcheckpoint.channel_id
    ]
    |> GetCheckpointRequest.new()
    |> GetCheckpointRequest.encode()
  end

  def remote_get_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/getcheckpoint", request_body, &GetCheckpointResponse.decode/1)
      |> Http.post()
    debug(fn ->
      [
        "get checkpoint result: ",
        inspect(result)
      ]
    end)
    result
  end

  def request_to_readrecords(var_read_records) do
    [
      tunnel_id: var_read_records.tunnel_id,
      client_id: var_read_records.client_id,
      channel_id: var_read_records.channel_id,
      token: var_read_records.token
    ]
    |> ReadRecordsRequest.new()
    |> ReadRecordsRequest.encode()
  end

  def remote_readrecords(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/readrecords", request_body, &decode_readrecords_response/1)
      |> Http.post()
    debug(fn ->
      [
        "readrecords result: ",
        inspect(result)
      ]
    end)
    result
  end

  defp decode_readrecords_response(response_body) do
    response = ReadRecordsResponse.decode(response_body)
    readable_records =
      response.records
      |> Task.async_stream(fn(record) ->
        readable_record = PlainBuffer.deserialize_row(record.record)
        Map.put(record, :record, readable_record)
      end, timeout: :infinity)
      |> Enum.map(fn({:ok, record}) -> record end)

    Map.put(response, :records, readable_records)
  end

  def request_to_checkpoint(var_checkpoint) do
    [
      tunnel_id: var_checkpoint.tunnel_id,
      client_id: var_checkpoint.client_id,
      channel_id: var_checkpoint.channel_id,
      checkpoint: var_checkpoint.checkpoint,
      sequence_number: var_checkpoint.sequence_number
    ]
    |> CheckpointRequest.new()
    |> CheckpointRequest.encode()
  end

  def remote_checkpoint(instance, request_body) do
    result =
      instance
      |> Http.client("/tunnel/checkpoint", request_body, &CheckpointResponse.decode/1)
      |> Http.post()
    debug(fn ->
      [
        "checkpoint result: ",
        inspect(result)
      ]
    end)
    result
  end

end
