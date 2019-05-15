defmodule ExAliyunOts.Tunnel.Channel do
  use GenStateMachine

  alias ExAliyunOts.Logger
  alias ExAliyunOts.Const.Tunnel.{ChannelStatus, ChannelConnectionStatus}
  alias ExAliyunOts.Tunnel.{Registry, EntryChannel}
  require ChannelStatus
  require ChannelConnectionStatus

  import EntryChannel

  alias __MODULE__.Connection

  def start_link(channel_id, tunnel_id, client_id, status, version, connection) do
    GenStateMachine.start_link(
      __MODULE__,
      {channel_id, tunnel_id, client_id, status, version, connection}
    )
  end

  def init({channel_id, tunnel_id, client_id, status, version, connection}) do
    channel_pid = self()

    Process.flag(:trap_exit, true)

    channel = %{
      channel_id: channel_id,
      connection: connection
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
          channel_from_heartbeat :: %ExAliyunOts.TableStoreTunnel.Channel{}
        ) :: :ok
  def update(channel, channel_from_heartbeat) do
    GenStateMachine.call(channel, {:update, channel_from_heartbeat})
  end


  def stop(channel) do
    Logger.info(">>>>>> stop finished channel <<<<<<")
    GenStateMachine.stop(channel, {:shutdown, :channel_finished})
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat},
        ChannelStatus.open(),
        channel
      ) do
    connection = channel.connection
    connection_status = Connection.status(connection)

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status open, channel_from_heartbeat: #{
        inspect(channel_from_heartbeat)
      }, connection_status: #{inspect(connection_status)}"
    )

    process_pipeline(
      channel,
      connection_status,
      connection,
      channel_from_heartbeat,
      from
    )
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat},
        ChannelStatus.closing(),
        channel
      ) do
    connection = channel.connection
    connection_status = Connection.status(connection)

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status closing, channel_from_heartbeat: #{
        inspect(channel_from_heartbeat)
      }, connection_status: #{inspect(connection_status)}"
    )

    case connection_status do
      ChannelConnectionStatus.wait() ->
        Connection.status_closed(connection)

      ChannelConnectionStatus.running() ->
        Connection.status_closing(connection)

      _ ->
        :ignore
    end

    merge(channel_from_heartbeat, ChannelStatus.close())
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat},
        ChannelStatus.close(),
        channel
      ) do
    connection = channel.connection
    connection_status = Connection.status(connection)

    Logger.info(
      ">>>>>>>>>>>>>>> update in channel_status close, channel_from_heartbeat: #{
        inspect(channel_from_heartbeat)
      }, connection_status: #{inspect(connection_status)}"
    )

    if connection_status == ChannelConnectionStatus.closing() do
      Connection.status_closed(connection)
    end

    merge(channel_from_heartbeat, ChannelStatus.close())
    Registry.inc_channel_version(channel_from_heartbeat.channel_id)
    {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat},
        channel_status,
        channel
      ) do
    connection_status = Connection.status(channel.connection)

    Logger.info(
      "** update unexpected channel_status: #{inspect(channel_status)} with channel_from_heartbeat: #{
        inspect(channel_from_heartbeat)
      }, connection_status: #{inspect(connection_status)}"
    )

    merge(channel_from_heartbeat)

    {:next_state, channel_from_heartbeat.status, channel, [{:reply, from, :ok}]}
  end

  def terminate(reason, state, channel) do
    Connection.stop(channel.connection)

    Registry.remove_channel(channel.channel_id)

    Logger.info(fn ->
      [
        "channel terminated with reason: ",
        inspect(reason),
        " state: ",
        inspect(state),
        " channel: ",
        inspect(channel),
        ", and close connection"
      ]
    end)

    :ok
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.wait(),
         connection,
         channel_from_heartbeat,
         from
       ) do
    Connection.status_running(connection)
    do_process_pipeline(channel, connection, channel_from_heartbeat, from)
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.running(),
         connection,
         channel_from_heartbeat,
         from
       ) do
    do_process_pipeline(channel, connection, channel_from_heartbeat, from)
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.closed(),
         _connection,
         _channel_from_heartbeat,
         from
       ) do
    channel_id = channel.channel_id

    case Connection.finished?(channel.connection) do
      true ->
        Registry.update_channel(channel_id, [{:status, ChannelStatus.terminated()}])
        Registry.inc_channel_version(channel_id)
        {:next_state, ChannelStatus.terminated(), channel, [{:reply, from, :ok}]}

      false ->
        Registry.update_channel(channel_id, [{:status, ChannelStatus.close()}])
        Registry.inc_channel_version(channel_id)
        {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
    end
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.closing(),
         _connection,
         channel_from_heartbeat,
         from
       ) do
    merge(channel_from_heartbeat)
    {:next_state, channel_from_heartbeat.status, channel, [{:reply, from, :ok}]}
  end

  defp do_process_pipeline(channel, connection, channel_from_heartbeat, from) do
    case channel_from_heartbeat.status do
      ChannelStatus.open() ->
        Logger.info "process readrecords and checkpoint for open channel"
        Connection.process(connection)
        {:next_state, ChannelStatus.open(), channel, [{:reply, from, :ok}]}

      ChannelStatus.closing() ->
        Logger.info("do_process_pipeline with channel closing status from heartbeat")
        Connection.status_closing(connection)
        merge(channel_from_heartbeat, ChannelStatus.close())
        Registry.inc_channel_version(channel_from_heartbeat.channel_id)
        {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}

      ChannelStatus.close() ->
        Logger.info("do_process_pipeline with channel close status from heartbeat")
        Connection.status_closed(connection)
        merge(channel_from_heartbeat)
        {:next_state, ChannelStatus.close(), channel, [{:reply, from, :ok}]}
    end
  end

  defp merge(channel_from_heartbeat, status \\ nil) do
    channel_id = channel_from_heartbeat.channel_id
    [_channel_id, _, _, _, _cur_status, cur_version] = Registry.channel(channel_id)
    latest_version = channel_from_heartbeat.version

    updates =
      if status != nil do
        [
          {:status, status}
        ]
      else
        [
          {:status, channel_from_heartbeat.status}
        ]
      end

    updates =
      if latest_version > cur_version do
        [{:version, latest_version} | updates]
      else
        updates
      end

    Registry.update_channel(channel_id, updates)
  end

end
