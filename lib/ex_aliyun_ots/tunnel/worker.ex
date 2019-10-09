defmodule ExAliyunOts.Tunnel.Worker do
  use GenServer

  alias ExAliyunOts.Client
  alias ExAliyunOts.Tunnel.{Registry, Channel, EntryWorker, Backoff, Utils, DynamicSupervisor}
  alias ExAliyunOts.Tunnel.Channel.Agent

  alias ExAliyunOts.Logger

  import EntryWorker

  @min_heartbeat_interval 5

  # Public

  def start_link(instance_key), do: GenServer.start_link(__MODULE__, instance_key, [])

  def init(instance_key) do
    Process.flag(:trap_exit, true)

    state = %{
      instance_key: instance_key
    }

    {:ok, state}
  end

  @spec start(instance_key :: atom(), opts :: Keyword.t()) :: term()
  def start(instance_key, opts) do
    {:ok, pid} = DynamicSupervisor.start_tunnel_worker(instance_key)
    opts = validate(opts)
    subscriber_pid = self()
    GenServer.cast(pid, {:start, opts, subscriber_pid})
    {:ok, pid}
  end

  @spec stop(tunnel_id :: String.t()) :: term()
  def stop(tunnel_id) do
    case Registry.worker(tunnel_id) do
      [_tunnel_id, _client_id, worker_pid, _meta, _subscriber] ->
        GenServer.stop(worker_pid, :shutdown)
      nil ->
        Logger.info("Stop worker but tunnel_id: #{inspect(tunnel_id)} is not existed from Registry")
    end
  end

  # Callbacks

  def handle_info({:heartbeat, opts}, state) do
    case heartbeat(state, opts) do
      :ok ->
        {:noreply, state}

      {:shutdown, :finished} ->
        {:stop, {:shutdown, :finished}, state}

      {:error, error} ->
        {:stop, {:shutdown, error}, state}
    end
  end

  def handle_info({:EXIT, pid, {:shutdown, :channel_finished}}, state) do
    success? = Registry.remove_channel(pid)

    Logger.info(fn ->
      "Worker handle_info EXIT message channel pid: #{inspect(pid)} with channel_finished reason exited, remove from registry sucess? #{
        inspect(success?)
      }"
    end)

    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, pid, _reason}, state) do
    Registry.remove_subscriber(ref, pid)
    {:noreply, state}
  end

  def handle_cast({:start, opts, subscriber_pid}, state) do
    worker_pid = self()

    tunnel_id = opts[:tunnel_id]

    Logger.info(fn ->
      "Start worker #{inspect(worker_pid)} for tunnel id #{inspect tunnel_id}"
    end)

    case Registry.new_worker(entry_worker(tunnel_id: tunnel_id, pid: self())) do
      true ->
        case connect_heartbeat(state, opts, subscriber_pid) do
          :ok ->
            {:noreply, state}

          {:error, _error} ->
            Registry.remove_worker(tunnel_id)
            {:stop, {:shutdown, :start_error}, state}
        end
      false ->
        raise ExAliyunOts.RuntimeError, "Tunnel worker #{inspect(worker_pid)} has already been exitsed."
    end
  end

  def terminate({:shutdown, :start_error}, _state) do
    Logger.info("Start worker failed, terminate it.")
    :ok
  end

  def terminate(reason, state) do
    shutdown(state)

    Logger.info(fn ->
      [
        "Tunnel worker terminated with reason:",
        inspect(reason)
      ]
    end)
  end

  # Private 

  defp connect_heartbeat(state, opts, subscriber_pid) do
    tunnel_id = opts[:tunnel_id]
    connect_timeout = opts[:connect_timeout]

    result =
      Client.connect_tunnel(
        state.instance_key,
        tunnel_id: tunnel_id,
        timeout: connect_timeout,
        client_tag: opts[:client_tag],
        request_timeout: connect_timeout * 1_000
      )

    case result do
      {:ok, response} ->
        client_id = response.client_id

        heartbeat_interval = opts[:heartbeat_interval] * 1_000

        ref = Process.monitor(subscriber_pid)

        Registry.update_worker(
          tunnel_id,
          [
            {:client_id, client_id},
            {:meta,
              %{
                heartbeat_interval: heartbeat_interval,
                heartbeat_timeout: opts[:heartbeat_timeout] * 1_000,
                timer_ref: schedule_heartbeat(opts, heartbeat_interval),
                last_heartbeat_time: DateTime.utc_now()
              }
            },
            {:subscriber, {ref, subscriber_pid}}
          ]
        )

        :ok
      error ->
        Logger.error("ConnectTunnel failed: #{inspect(error)} **")
        error
    end
  end

  defp validate(opts) do
    # default 30 seconds
    heartbeat_interval = opts[:heartbeat_interval] || 30
    # default 300 seconds
    heartbeat_timeout = opts[:heartbeat_timeout] || 300
    # default 300 seconds
    connect_timeout = opts[:connect_timeout] || 300

    tunnel_id = opts[:tunnel_id]

    if heartbeat_interval < @min_heartbeat_interval do
      raise ExAliyunOts.RuntimeError,
            "Invalid parameter, heartbeat_interval should be >= #{@min_heartbeat_interval} seconds."
    end

    if heartbeat_timeout <= heartbeat_interval do
      raise ExAliyunOts.RuntimeError,
            "Invalid parameter, heartbeat_timeout should be > heartbeat_interval(#{
              heartbeat_interval
            } seconds)."
    end

    if tunnel_id == nil do
      raise ExAliyunOts.RuntimeError, "Invalid parameter, tunnel_id is required"
    end

    [
      tunnel_id: tunnel_id,
      client_tag: opts[:client_tag],
      connect_timeout: connect_timeout,
      heartbeat_interval: heartbeat_interval,
      heartbeat_timeout: heartbeat_timeout
    ]
  end

  defp heartbeat(state, opts) do
    now = DateTime.utc_now()

    instance_key = state.instance_key

    tunnel_id = opts[:tunnel_id]

    [_tunnel_id, client_id, _worker_pid, meta, _subscriber] = Registry.worker(tunnel_id)

    last_heartbeat_time = meta.last_heartbeat_time

    if DateTime.diff(now, last_heartbeat_time) > meta.heartbeat_timeout do
      raise ExAliyunOts.RuntimeError,
            "Tunnel client heartbeat timeout, last_heartbeat_time: #{inspect(last_heartbeat_time)}"
    end

    Logger.info("Begin heartbeat channel.")

    local_channels = Registry.channels(tunnel_id)

    {channels_to_heartbeat, local_channel_ids} =
      Enum.map_reduce(local_channels, [], fn local_channel, acc ->
        [channel_id, _tunnel_id, _client_id, _channel_pid, status, version] = local_channel
        {
          [channel_id: channel_id, version: version, status: status],
          [channel_id | acc]
        }
      end)

    Logger.info("Use channels to heartbeat: #{inspect(channels_to_heartbeat)}")

    result =
      Client.heartbeat(
        instance_key,
        request_timeout: meta.heartbeat_timeout,
        channels: channels_to_heartbeat,
        tunnel_id: tunnel_id,
        client_id: client_id
      )

    Logger.info(fn -> "Remote heartbeat result: #{inspect(result)}" end)

    case result do
      {:ok, response} ->
        last_heartbeat_time = DateTime.utc_now()

        channels_from_response = response.channels

        Logger.info(fn ->
          [
            "Begin update channels, ",
            "num: ",
            inspect(length(channels_from_response)),
            ", detail: ",
            inspect(channels_from_response)
          ]
        end)

        update_channels(
          instance_key,
          tunnel_id,
          client_id,
          channels_from_response,
          local_channel_ids
        )

        updated_meta =
          meta
          |> Map.put(:last_heartbeat_time, last_heartbeat_time)
          |> Map.put(:timer_ref, schedule_heartbeat(opts, meta.heartbeat_interval))

        Registry.update_worker(tunnel_id, [
          {:meta, updated_meta}
        ])

        current_local_channels = Registry.channels(tunnel_id)

        Logger.info(fn ->
          [
            "After heartbeat current_local_channels: ",
            inspect(current_local_channels)
          ]
        end)

        :ok

      error ->
        case invalid_tunnel?(error) do
          true ->
            Logger.info(fn -> "Invalid tunnel" end)

          false ->
            Logger.info(fn ->
              [
                "Unknown error when heartbeat ",
                inspect(error)
              ]
            end)
        end

        error
    end
  end

  defp invalid_tunnel?({:error, error_msg}) do
    Logger.error(fn ->
      [
        "Get invalid_tunnel ",
        inspect(error_msg)
      ]
    end)
    String.contains?(error_msg, "OTSParameterInvalid") or String.contains?(error_msg, "OTSTunnelExpired")
  end

  defp invalid_tunnel?(error) do
    Logger.error(fn ->
      [
        "Get invalid_tunnel ",
        inspect(error),
        " occur unknown error"
      ]
    end)

    false
  end

  defp shutdown(state) do
    case Registry.worker(self()) do
      [tunnel_id, client_id, _worker_pid, meta, _subscriber] ->
        timer_ref = Map.get(meta, :timer_ref)

        if timer_ref != nil do
          Process.cancel_timer(timer_ref)
        end

        Registry.remove_worker(tunnel_id)

        if tunnel_id != nil do
          remote_shutdown_tunnel(state.instance_key, tunnel_id, client_id)
        end

      nil ->
        :ignore
    end
  end

  defp remote_shutdown_tunnel(instance_key, tunnel_id, client_id) do
    shutdown_result = Client.shutdown_tunnel(instance_key, tunnel_id: tunnel_id, client_id: client_id)

    Logger.info(fn ->
      [
        "Shutdown tunnel with tunnel_id: ",
        inspect(tunnel_id),
        " client_id: ",
        inspect(client_id),
        ", result: ",
        inspect(shutdown_result)
      ]
    end)
  end

  defp to_be_removed_channel_ids(local_channel_ids, channels_from_response) do
    # if channel id is in local but not in latest heartbeat reponse, it will be removed.
    local = MapSet.new(local_channel_ids)

    remote =
      channels_from_response
      |> Enum.map(fn channel -> channel.channel_id end)
      |> MapSet.new()

    MapSet.difference(local, remote)
  end

  defp schedule_heartbeat(opts, heartbeat_interval) do
    Process.send_after(self(), {:heartbeat, opts}, heartbeat_interval)
  end

  defp update_channels(
         instance_key,
         tunnel_id,
         client_id,
         channels_from_response,
         local_channel_ids
       ) do
    Enum.each(channels_from_response, fn channel_from_heartbeat ->
      channel_id = channel_from_heartbeat.channel_id

      case Registry.channel(channel_id, tunnel_id, client_id) do
        nil ->
          # not existed yet in local
          {:ok, channel_pid} =
            init_channel(instance_key, tunnel_id, client_id, channel_from_heartbeat)

          Channel.update(channel_pid, channel_from_heartbeat)

        [_channel_id, _tunnel_id, _client_id, channel_pid, _status, _version] ->
          # already in local
          Channel.update(channel_pid, channel_from_heartbeat)
      end
    end)

    local_channel_ids
    |> to_be_removed_channel_ids(channels_from_response)
    |> Enum.map(fn tbr_channel_id ->
      case Registry.channel(tbr_channel_id, tunnel_id, client_id) do
        nil ->
          :ok

        [_channel_id, _tunnel_id, _client_id, channel_pid, _status, _version] ->
          Logger.info(
            "Stop channel process: #{inspect(channel_pid)} for channel_id: #{
              inspect(tbr_channel_id)
            }"
          )

          Channel.stop(channel_pid)
      end
    end)
  end

  defp init_channel(instance_key, tunnel_id, client_id, channel_from_heartbeat) do
    channel_id = channel_from_heartbeat.channel_id

    result =
      Client.get_checkpoint(
        instance_key,
        tunnel_id: tunnel_id,
        client_id: client_id,
        channel_id: channel_id
      )

    case result do
      {:ok, response} ->

        start_channel(
          instance_key,
          channel_id,
          tunnel_id,
          client_id,
          channel_from_heartbeat.status,
          channel_from_heartbeat.version,
          response.checkpoint,
          response.sequence_number + 1
        )

      error_result ->
        Logger.error(fn ->
          [
            "GetCheckpointer error: ",
            inspect(error_result)
          ]
        end)

        error_result
    end
  end

  defp start_channel_agent(opts) do
    stream? = Utils.stream_token?(opts[:token])
    opts = if stream?, do: Keyword.put(opts, :backoff, Backoff.new()), else: opts
    Agent.start_link(opts)
  end

  defp start_channel(instance_key, channel_id, tunnel_id, client_id, status, version, token, sequence_number) do
    {:ok, agent} = start_channel_agent(
      tunnel_id: tunnel_id,
      channel_id: channel_id,
      client_id: client_id,
      token: token,
      instance_key: instance_key,
      sequence_number: sequence_number
    )
    Channel.start_link(channel_id, tunnel_id, client_id, status, version, agent)
  end

end
