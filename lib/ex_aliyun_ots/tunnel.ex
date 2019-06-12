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

  def request_to_create_tunnel(opts) do
    CreateTunnelRequest.new(tunnel: Tunnel.new(opts)) |> CreateTunnelRequest.encode()
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

  def request_to_delete_tunnel(opts) do
    opts
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

  def request_to_describe_tunnel(opts) do
    opts
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

  def request_to_connect_tunnel(opts) do
    config = ClientConfig.new(timeout: opts[:timeout], client_tag: opts[:client_tag])
    ConnectRequest.new(
      tunnel_id: opts[:tunnel_id],
      client_config: config
    )
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

  def request_to_heartbeat(opts) do
    channels =
      Enum.map(Keyword.get(opts, :channels, []), fn(channel) ->
        Channel.new(channel)
      end)
    HeartbeatRequest.new(
      tunnel_id: opts[:tunnel_id],
      client_id: opts[:client_id],
      channels: channels
    )
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

  def request_to_shutdown(opts) do
    opts
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

  def request_to_get_checkpoint(opts) do
    opts
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

  def request_to_readrecords(opts) do
    opts
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

  def request_to_checkpoint(opts) do
    opts
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
