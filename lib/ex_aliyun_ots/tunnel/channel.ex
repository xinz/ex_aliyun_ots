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
    Logger.info("Channel GenStateMachine pid: #{inspect(channel_pid)}")

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
          channel_from_heartbeat :: %ExAliyunOts.TableStoreTunnel.Channel{},
          handler_module :: ExAliyunOts.Tunnel.Handler.t()
        ) :: :ok
  def update(channel, channel_from_heartbeat, handler_module) do
    GenStateMachine.call(channel, {:update, channel_from_heartbeat, handler_module})
  end

  def stop(channel) do
    Logger.info(">>>>>> Channel stop <<<<<<")
    GenStateMachine.stop(channel, {:shutdown, :channel_finished})
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat, handler_module},
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
      handler_module,
      channel_from_heartbeat,
      from
    )
  end

  def handle_event(
        {:call, from},
        {:update, channel_from_heartbeat, _handler_module},
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
        {:update, channel_from_heartbeat, _handler_module},
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
        {:update, channel_from_heartbeat, _handler_module},
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
         handler_module,
         channel_from_heartbeat,
         from
       ) do
    Connection.status_running(connection)
    do_process_pipeline(channel, connection, handler_module, channel_from_heartbeat, from)
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.running(),
         connection,
         handler_module,
         channel_from_heartbeat,
         from
       ) do
    do_process_pipeline(channel, connection, handler_module, channel_from_heartbeat, from)
  end

  defp process_pipeline(
         channel,
         ChannelConnectionStatus.closed(),
         _connection,
         _handler_module,
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
         _handler_module,
         channel_from_heartbeat,
         from
       ) do
    merge(channel_from_heartbeat)
    {:next_state, channel_from_heartbeat.status, channel, [{:reply, from, :ok}]}
  end

  defp do_process_pipeline(channel, connection, handler_module, channel_from_heartbeat, from) do
    case channel_from_heartbeat.status do
      ChannelStatus.open() ->
        case Connection.process(connection, handler_module) do
          :ok ->
            Logger.info(">>>> process_pipeline done")
            latest_status = channel_from_heartbeat.status
            merge(channel_from_heartbeat)
            {:next_state, latest_status, channel, [{:reply, from, :ok}]}

          :finished ->
            Logger.info(">>>> process_pipeline finished")
            Connection.status_closed(connection)
            {:stop_and_reply, {:shutdown, :channel_finished}, [{:reply, from, :channel_finished}]}

          process_error ->
            Logger.info(">>>> process_pipeline with error")
            Connection.status_closed(connection)
            {:stop_and_reply, {:shutdown, process_error}, [{:reply, from, process_error}]}
        end

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

  defp merge(channel_from_heartbeat) do
    channel_id = channel_from_heartbeat.channel_id
    {_channel_id, _, _, _, _cur_status, cur_version} = Registry.channel(channel_id)
    latest_version = channel_from_heartbeat.version

    if latest_version > cur_version do
      Registry.update_channel(channel_id, [
        {:version, latest_version},
        {:status, channel_from_heartbeat.status}
      ])
    else
      Registry.update_channel(channel_id, [{:status, channel_from_heartbeat.status}])
    end
  end

  defp merge(channel_from_heartbeat, status) do
    channel_id = channel_from_heartbeat.channel_id
    {_channel_id, _, _, _, _cur_status, cur_version} = Registry.channel(channel_id)
    latest_version = channel_from_heartbeat.version

    if latest_version > cur_version do
      Registry.update_channel(channel_id, [{:version, latest_version}, {:status, status}])
    else
      Registry.update_channel(channel_id, [{:status, status}])
    end
  end
end
