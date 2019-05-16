defmodule ExAliyunOtsTest.Tunnel.Registry do
  use ExUnit.Case
  alias ExAliyunOts.Tunnel.{Registry, Registry}

  alias ExAliyunOts.Tunnel.{EntryWorker, EntryChannel}
  import EntryWorker
  import EntryChannel

  test "registry worker operation" do

    client_pid = self()

    new_result1 = Registry.new_worker(entry_worker(tunnel_id: "tunnel_id0"))
    assert new_result1 == true
    new_result2 = Registry.new_worker(entry_worker(tunnel_id: "tunnel_id0"))
    assert new_result2 == false

    Registry.update_worker("tunnel_id0", [{:client_id, "client_id0"}])

    [{"tunnel_id0", "client_id0", nil_client_pid, %{}}] = Registry.workers()

    assert nil_client_pid == nil

    Registry.remove_worker("tunnel_id0")

    Registry.new_worker(entry_worker(
      tunnel_id: "tunnel_id1",
      client_id: "client_id1",
      pid: client_pid
    ))

    index_tunnel_id = EntryWorker.index(:tunnel_id)
    index_client_id = EntryWorker.index(:client_id)
    assert index_tunnel_id == 1
    assert index_client_id == 2

    Registry.new_worker(entry_worker(
      tunnel_id: "tunnel_id2",
      client_id: "client_id2",
      pid: client_pid,
      meta: %{
        client_tag: "client_tag2"
      }
    ))

    updated_meta = %{
      client_tag: "updated_tag2",
      key: 1
    }
    update_result = Registry.update_worker("tunnel_id1", [{:meta, updated_meta}])
    assert update_result == true

    [_tunnel_id, _client_id, _worker_pid, meta] = Registry.worker("tunnel_id1")
    assert meta == updated_meta

    Registry.remove_worker("tunnel_id1")

    result = Registry.workers()
    assert length(result) == 1
    {"tunnel_id2", "client_id2", ^client_pid, _mate} = List.first(result)

    worker1 = Registry.worker("tunnel_id1")
    assert worker1 == nil
    ["tunnel_id2", "client_id2", ^client_pid, meta1] = Registry.worker("tunnel_id2")
    ["tunnel_id2", "client_id2", ^client_pid, meta2] = Registry.worker(client_pid)
    assert meta1 == meta2
  end


  test "registry channel operation" do

    assert EntryChannel.index(:channel_id) == 1
    assert EntryChannel.index(:tunnel_id) == 2
    assert EntryChannel.index(:client_id) == 3
    tunnel_id = "tunnel_id1"
    channel_pid = Process.spawn(fn -> :ok end, [:link])
    Registry.new_channel(entry_channel(
      channel_id: "channel_id1",
      tunnel_id: tunnel_id,
      client_id: "client_id1",
      pid: channel_pid,
      status: "OPEN",
      version: 1
    ))

    channel_pid2 = Process.spawn(fn -> :ok end, [:link])
    Registry.new_channel(entry_channel(
      channel_id: "channel_id2",
      tunnel_id: tunnel_id,
      client_id: "client_id1",
      pid: channel_pid2,
      status: "OPEN",
      version: 2
    ))

    fake_channel_pid = Process.spawn(fn -> :ok end, [:link])

    channel1 = Registry.channel(channel_pid)
    ["channel_id1", "tunnel_id1", "client_id1", ^channel_pid, "OPEN", 1] = channel1

    inc_result = Registry.inc_channel_version(channel_pid)
    assert inc_result == 2

    res = Registry.channel(fake_channel_pid)
    assert res == nil

    channels = Registry.channels(tunnel_id)
    assert length(channels) == 2
    Enum.map(channels, fn(c) ->
      [_channel_id, ^tunnel_id, "client_id1", _pid, "OPEN", 2] = c
    end)

    remove_result = Registry.remove_channel(channel_pid)
    assert remove_result == true

    remove_result = Registry.remove_channel(fake_channel_pid)
    assert remove_result == true

  end

end
