defmodule ExAliyunOts.Tunnel.Channel do
  use GenStateMachine

  alias ExAliyunOts.Logger
  alias ExAliyunOts.Const.Tunnel.ChannelStatus
  alias ExAliyunOts.Tunnel.{Registry, EntryChannel}
  require ChannelStatus

  import EntryChannel

  alias __MODULE__.Agent

  def start_link(channel_id, tunnel_id, client_id, status, version, agent) do
    GenStateMachine.start_link(
      __MODULE__,
      {channel_id, tunnel_id, client_id, status, version, agent}
    )
  end

  def init({channel_id, tunnel_id, client_id, status, version, agent}) do
    channel_pid = self()

    Process.flag(:trap_exit, true)

    channel = %{
      pid: channel_pid,
      agent: agent
    }

    Registry.new_channel(
      entry_channel(
        channel_id: channel_id,
        tunnel_id: tunnel_id,
        client_id: client_id,
        pid: channel_pid,
        status: status,
        version: version
      )
    )

    {:ok, status, channel}
  end


  @spec update(
          channel :: pid(),
          remote_channel :: %ExAliyunOts.TableStoreTunnel.Channel{}
        ) :: :ok
  def update(channel, remote_channel) do
    GenStateMachine.call(channel, {:update, remote_channel})
  end

  def stop(channel) do
    Logger.info(">>>>>> Stop finished channel <<<<<<")
    GenStateMachine.stop(channel, {:shutdown, :channel_finished})
  end

  def handle_event(
        {:call, from},
        {:update, remote_channel},
        ChannelStatus.open(),
        channel
      ) do

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status open, remote_channel: #{
        inspect(remote_channel)
      }"
    )

    remote_channel_status = remote_channel.status

    process_channel(
      channel,
      channel.agent,
      remote_channel,
      remote_channel_status,
      from
    )
  end

  def handle_event(
        {:call, from},
        {:update, remote_channel},
        ChannelStatus.closing(),
        channel
      ) do

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status closing, remote_channel: #{
        inspect(remote_channel)
      }"
    )

    merge(remote_channel, channel.pid, ChannelStatus.close())
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end

  def handle_event(
        {:call, from},
        {:update, remote_channel},
        ChannelStatus.close(),
        channel
      ) do

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status close, remote_channel: #{
        inspect(remote_channel)
      }"
    )

    merge(remote_channel, channel.pid, ChannelStatus.close())
    Registry.inc_channel_version(channel.pid)
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end

  def handle_event(
        {:call, from},
        {:update, remote_channel},
        channel_status,
        channel
      ) do

    Logger.info(
      "** update unexpected channel_status: #{inspect(channel_status)} with remote_channel: #{
        inspect(remote_channel)
      }"
    )

    merge(remote_channel, channel.pid)

    {:next_state, remote_channel.status, channel, [{:reply, from, :ok}]}
  end

  def terminate(reason, state, channel) do

    Agent.stop(channel.agent)

    Registry.remove_channel(channel.pid)

    Logger.info(fn ->
      [
        "channel terminated with reason: ",
        inspect(reason),
        " state: ",
        inspect(state),
        " channel: ",
        inspect(channel),
        ", and close agent"
      ]
    end)

    :ok
  end

  defp process_channel(
         channel,
         agent,
         _remote_channel,
         _remote_channel_status = ChannelStatus.open(),
         from
       ) do
    Logger.info "process readrecords and checkpoint for open channel"
    Agent.process(agent)
    {:next_state, ChannelStatus.open(), channel, [{:reply, from, :ok}]}
  end
  defp process_channel(
         channel,
         _agent,
         remote_channel,
         _remote_channel_status = ChannelStatus.closing(),
         from
       ) do
    Logger.info("process channel as closing status from heartbeat")
    merge(remote_channel, channel.pid, ChannelStatus.close())
    Registry.inc_channel_version(channel.pid)
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end
  defp process_channel(
         channel,
         _agent,
         remote_channel,
         _remote_channel_status = ChannelStatus.close(),
         from
       ) do
    Logger.info("process channel as close status from heartbeat")
    merge(remote_channel, channel.pid)
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end

  defp merge(remote_channel, pid, status \\ nil) do
    channel_id = remote_channel.channel_id
    [^channel_id, _, _, _, _cur_status, cur_version] = Registry.channel(pid)
    latest_version = remote_channel.version

    updates =
      if status != nil do
        [
          {:status, status}
        ]
      else
        [
          {:status, remote_channel.status}
        ]
      end

    updates =
      if latest_version > cur_version do
        [{:version, latest_version} | updates]
      else
        updates
      end

    Registry.update_channel(pid, updates)
  end

end
