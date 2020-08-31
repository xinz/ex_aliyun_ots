defmodule ExAliyunOtsTest.Tunnel do
  use ExUnit.Case

  require Logger

  alias ExAliyunOts.Client

  alias ExAliyunOts.Const.TunnelType
  require TunnelType

  @instance_key EDCEXTestInstance
  @table_name "test_tunnel_client"
  @tunnel_name "tunnelid1"
  @test_records_size 10

  defmodule TunnelData do
    use ExAliyunOts,
      instance: EDCEXTestInstance

    def create_table(table_name) do
      create_table(table_name, [{"id", PKType.integer()}])
    end

    def write(table_name, pk, attrs) do
      put_row(table_name, pk, attrs, condition: condition(:ignore))
    end
  end

  setup_all do
    TunnelData.create_table(@table_name)

    {:ok, _response1} =
      Client.create_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: @tunnel_name,
        tunnel_type: TunnelType.base()
      )

    on_exit(fn ->
      {:ok, _response} =
        Client.delete_tunnel(@instance_key, table_name: @table_name, tunnel_name: @tunnel_name)

      TunnelData.delete_table(@table_name)
    end)

    Process.sleep(6_000)

    for i <- 1..@test_records_size do
      TunnelData.write(
        @table_name,
        [{"id", i}],
        [
          {"attr", "attr#{i}"},
          {"attr2", "somevalue#{i}"},
          {"attr3", i},
          {"attr4", 89.1},
          {"attr5", false},
          {"attr6", "#{i}sometestvalue"}
        ]
      )
    end

    Process.sleep(1_000)

    {:ok, response} = Client.list_tunnel(@instance_key, @table_name)
    Logger.info("list tunnel response: #{inspect(response)}")
    assert length(response.tunnels) == 1
    [tunnel] = response.tunnels
    assert tunnel.tunnel_type == Atom.to_string(TunnelType.base())

    {:ok, tunnel: tunnel}
  end

  test "describe tunnel", context do
    tunnel = context[:tunnel]

    {:ok, response} =
      Client.describe_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: tunnel.tunnel_name,
        tunnel_id: tunnel.tunnel_id
      )

    Logger.info("describe tunnel response: #{inspect(response)}")
    [channel] = response.channels
    assert channel.channel_rpo == 0
  end

  test "connect and heartbeat tunnel", context do
    tunnel = context[:tunnel]
    tunnel_id = tunnel.tunnel_id
    {:ok, response} = Client.connect_tunnel(@instance_key, tunnel_id: tunnel_id)
    client_id = response.client_id
    assert client_id != nil

    {:ok, heartbeat_response} =
      Client.heartbeat(@instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id
      )

    Logger.info(">>> heartbeat_response: #{inspect(heartbeat_response)}")

    assert length(heartbeat_response.channels) >= 0

    {:ok, response} =
      Client.describe_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: tunnel.tunnel_name,
        tunnel_id: tunnel_id
      )

    Logger.info("describe tunnel response: #{inspect(response)}")
    [channel] = response.channels

    channel_id = channel.channel_id

    {:ok, response} =
      Client.get_checkpoint(@instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id,
        channel_id: channel_id
      )

    Logger.info("get_checkpoint response: #{inspect(response)}")

    checkpoint = response.checkpoint

    all_records = read_all_records(checkpoint, [], tunnel_id, client_id, channel_id)

    assert length(all_records) == @test_records_size

    local_seq_num = response.sequence_number + 1

    {:ok, checkpoint_response} =
      Client.checkpoint(@instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id,
        channel_id: channel_id,
        checkpoint: "finished",
        sequence_number: local_seq_num
      )

    assert checkpoint_response == %ExAliyunOts.TableStoreTunnel.CheckpointResponse{}

    {:ok, last_get_checkpoint_response} =
      Client.get_checkpoint(@instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id,
        channel_id: channel_id
      )

    assert last_get_checkpoint_response.checkpoint == "finished"
    assert last_get_checkpoint_response.sequence_number == local_seq_num

    {shutdown_result, response} =
      Client.shutdown_tunnel(@instance_key, tunnel_id: tunnel.tunnel_id, client_id: client_id)

    assert shutdown_result == :ok
    assert response == %ExAliyunOts.TableStoreTunnel.ShutdownResponse{}
  end

  defp read_all_records("finished", records, _tunnel_id, _client_id, _channel_id) do
    records
  end

  defp read_all_records(checkpoint, records, tunnel_id, client_id, channel_id) do
    {:ok, response, _body_size} =
      Client.read_records(@instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id,
        channel_id: channel_id,
        token: checkpoint
      )

    updated_records = response.records ++ records
    read_all_records(response.next_token, updated_records, tunnel_id, client_id, channel_id)
  end
end
