defmodule ExAliyunOtsTest.Tunnel.Integration do
  use ExUnit.Case

  require Logger

  alias ExAliyunOts.Client
  alias ExAliyunOts.Tunnel.Worker

  alias ExAliyunOts.Const.TunnelType
  require TunnelType

  @instance_key EDCEXTestInstance

  @table_name "test_tunnel"

  @tunnel_name1 "tunnelid1"
  @tunnel_name2 "tunnelid2"
  @tunnel_name3 "tunnelid3"

  @test_rows 10

  def table_name, do: @table_name
  def test_rows, do: @test_rows

  defmodule TunnelData do
  
    alias ExAliyunOtsTest.Tunnel.Integration

    use ExAliyunOts,
      instance: EDCEXTestInstance
  
    def create_table(table_name) do
      create_table table_name, [{"id", PKType.integer}]
      Process.sleep(3_000)
    end

    def write(number \\ Integration.test_rows) do
      for i <- 1..number do
        put_row Integration.table_name, [{"id", i}],
          [{"attr1", "attr1_#{i}"}, {"data1", "data1_#{i}"}, {"index", i}],
          condition: condition(:ignore)
      end
    end

    def clean() do
      for i <- 1..Integration.test_rows do
        delete_row Integration.table_name, [{"id", i}],
          condition: condition(:ignore)
      end
    end

    def match?(index, {keys, attrs}) do
      [{"id", id}] = keys
      [{"attr1", value1, _ts1}, {"data1", value2, _ts2}, {"index", value3, _ts3}] = attrs
      (index == id) and (value1 == "attr1_#{index}") and (value2 == "data1_#{index
      }") and (value3 == index)
    end
  
  end

  setup_all do

    TunnelData.create_table(@table_name)
    
    {:ok, response1} =
      Client.create_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: @tunnel_name1,
        tunnel_type: TunnelType.base
      )

    {:ok, response2} =
      Client.create_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: @tunnel_name2,
        tunnel_type: TunnelType.stream
      )

    {:ok, response3} =
      Client.create_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: @tunnel_name3,
        tunnel_type: TunnelType.base_and_stream
      )
    
    tunnel_id1 = response1.tunnel_id
    tunnel_id2 = response2.tunnel_id
    tunnel_id3 = response3.tunnel_id

    on_exit(fn ->

      Worker.stop(tunnel_id1)
      Worker.stop(tunnel_id2)
      Worker.stop(tunnel_id3)

      {del_result, _response} = Client.delete_tunnel(@instance_key, table_name: @table_name, tunnel_name: @tunnel_name1)
      assert :ok == del_result
      {del_result2, _response} = Client.delete_tunnel(@instance_key, table_name: @table_name, tunnel_name: @tunnel_name2)
      assert :ok == del_result2
      {del_result3, _response} = Client.delete_tunnel(@instance_key, table_name: @table_name, tunnel_name: @tunnel_name3)
      assert :ok == del_result3

      TunnelData.clean()

      TunnelData.delete_table(@table_name)
    end)

    Logger.info ">>>> self(): #{inspect self()}"

    Process.sleep(6_000)

    tunnels = %{
      @tunnel_name1 => [tunnel_id: tunnel_id1],
      @tunnel_name2 => [tunnel_id: tunnel_id2],
      @tunnel_name3 => [tunnel_id: tunnel_id3]
    }
    {:ok, tunnels: tunnels}
  end

  test "describe and list tunnel", context do
    Logger.info ">>>> self1(): #{inspect self()}"

    {:ok, describe_response} =
      Client.describe_tunnel(@instance_key,
        table_name: @table_name,
        tunnel_name: @tunnel_name1
      )
    
    Logger.info("describe_response: #{inspect describe_response, pretty: true}")

    tunnel1_stage = describe_response.tunnel.stage
    cond do
      tunnel1_stage == "ProcessBaseData" ->
        assert length(describe_response.channels) > 0
      tunnel1_stage == "InitBaseDataAndStreamShard" ->
        assert length(describe_response.channels) == 0
    end

    {:ok, response} = Client.list_tunnel(@instance_key, @table_name)

    assert length(response.tunnels) == 3

    Enum.map(response.tunnels, fn(tunnel_info) ->
      tunnel_name = tunnel_info.tunnel_name
      tunnel = Map.get(context[:tunnels], tunnel_name)

      assert tunnel != nil

      cond do
        tunnel_name == "tunnelid1" ->
          assert tunnel_info.tunnel_type == "BaseData"
        tunnel_name == "tunnelid2" ->
          assert tunnel_info.tunnel_type == "Stream"
        tunnel_name == "tunnelid3" ->
          assert tunnel_info.tunnel_type == "BaseAndStream"
      end

    end)

  end

  test "stream tunnel read data", context do
    Logger.info "start stream tunnel testing...#{inspect self()}"

    tunnels = context[:tunnels]
    tunnel_id2 = tunnels[@tunnel_name2][:tunnel_id] 

    worker_pid2 = Worker.start(@instance_key, [tunnel_id: tunnel_id2])

    Logger.info "worker_pid2: #{inspect worker_pid2}"

    Process.sleep(5_000)

    TunnelData.write()

    assert_receive {:"$gen_call", from, {:record_event, {records, _next_token}}}, 40_000
    GenServer.reply(from, :ok)

    assert length(records) == @test_rows

    records
    |> Enum.with_index()
    |> Enum.map(fn({record, index}) ->
      assert record.action_type == :PUT_ROW
      assert TunnelData.match?(index + 1, record.record) == true
    end)

  end

  test "base tunnel read data", context do
    Logger.info "start base tunnel testing..."

    tunnels = context[:tunnels]
    tunnel_id1 = tunnels[@tunnel_name1][:tunnel_id]

    _worker_pid1 = Worker.start(@instance_key, [tunnel_id: tunnel_id1])

    Process.sleep(5_000)

    assert_receive {:"$gen_call", from, {:record_event, {records, _next_token}}}, 40_000
    GenServer.reply(from, :ok)

    assert length(records) == @test_rows

    records
    |> Enum.with_index()
    |> Enum.map(fn({record, index}) ->
      assert record.action_type == :PUT_ROW
      assert TunnelData.match?(index + 1, record.record) == true
    end)

  end

  test "base_and_stream read data", context do
    Logger.info "start base_and_stream tunnel testing...#{inspect self()}"

    tunnels = context[:tunnels]
    tunnel_id3 = tunnels[@tunnel_name3][:tunnel_id]

    worker_pid3 = Worker.start(@instance_key, [tunnel_id: tunnel_id3])

    Logger.info "worker_pid3: #{inspect worker_pid3}"

    Process.sleep(5_000)

    TunnelData.write(20)
    
    assert_receive {:"$gen_call", from, {:record_event, {records, _next_token}}}, 40_000
    GenServer.reply(from, :ok)

    Logger.info "base_and_stream records: #{inspect records}"
    assert length(records) == @test_rows + 10

    TunnelData.clean()

    assert_receive {:"$gen_call", from, {:record_event, {records, _next_token}}}, 60_000
    GenServer.reply(from, :ok)
    Logger.info "base_and_stream records after clean all records: #{inspect records}"

    action_types = 
      records
      |> Enum.map(fn(record) ->
        record.action_type
      end)
      |> MapSet.new()

    assert MapSet.equal?(action_types, MapSet.new([:PUT_ROW, :DELETE_ROW])) == true

  end

end
