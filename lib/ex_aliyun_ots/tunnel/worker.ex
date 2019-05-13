defmodule ExAliyunOts.Tunnel.Worker do
  use GenServer

  alias ExAliyunOts.Client
  alias ExAliyunOts.Var.Tunnel.{ConnectTunnel, Heartbeat, Shutdown, GetCheckpoint}
  alias ExAliyunOts.Var.Tunnel.Channel, as: VarChannel
  alias ExAliyunOts.Tunnel.{Registry, Channel, EntryWorker, Backoff, Utils}
  alias ExAliyunOts.Tunnel.Channel.Connection

  alias ExAliyunOts.Logger

  import EntryWorker

  @min_heartbeat_interval 5

  # Public

  def pool_name(instance_key) do
    Module.concat([instance_key, Tunnel.Pool])
  end

  def start_link(instance_key), do: GenServer.start_link(__MODULE__, instance_key, [])

  def init(instance_key) do
    Process.flag(:trap_exit, true)

    state = %{
      instance_key: instance_key,
      subscribers: %{}
    }

    {:ok, state}
  end

  @spec start(instance_key :: atom(), opts :: Keyword.t()) :: term()
  def start(instance_key, opts) do
    opts = validate(opts)
    subscriber_pid = self()

    :poolboy.transaction(
      pool_name(instance_key),
      fn worker ->
        GenServer.cast(worker, {:start, opts, subscriber_pid})
        worker
      end,
      :infinity
    )
  end

  @spec stop(tunnel_id :: String.t()) :: term()
  def stop(tunnel_id) do
    case Registry.worker(tunnel_id) do
      [_tunnel_id, _client_id, worker_pid, _meta] ->
        GenServer.stop(worker_pid, {:shutdown, :manual_stop})

      nil ->
        Logger.info("tunnel_id: #{inspect(tunnel_id)} is not existed.")
    end
  end

  def handle_records(worker, records, next_token) do
    GenServer.cast(worker, {:handle_records, records, next_token})
  end

  # Callbacks

  def handle_info({:heartbeat, opts}, state) do
    result = heartbeat(state, opts)

    Logger.info(fn ->
      ">>>>> handle_info with heartbeat: #{inspect(result)}"
    end)

    case result do
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
      "worker handle_info EXIT message channel pid: #{inspect(pid)} with channel_finished reason exited, remove from registry sucess? #{
        inspect(success?)
      }"
    end)

    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, _pid, _reason}, state) do
    subscribers = Map.drop(state.subscribers, [ref])
    {:noreply, %{state | subscribers: subscribers}}
  end

  def handle_cast({:start, opts, subscriber_pid}, state) do
    worker_pid = self()

    tunnel_id = opts[:tunnel_id]

    Logger.info("start worker #{inspect(worker_pid)} for tunnel id #{inspect tunnel_id}")

    case Registry.new_worker(entry_worker(tunnel_id: tunnel_id, pid: self())) do
      true ->
        case connect_heartbeat(state, opts) do
          :ok ->
            subscribe(subscriber_pid, state)

          {:error, _error} ->
            Registry.remove_worker(tunnel_id)
            {:stop, {:shutdown, :start_error}, state}
        end
      false ->
        raise ExAliyunOts.Error, "Tunnel worker #{inspect(worker_pid)} has already been exitsed."
    end
  end

  def handle_cast({:handle_records, records, next_token}, state) do
    Enum.each(state.subscribers, fn {_ref, subscriber_pid} ->
      send(subscriber_pid, {:record_event, self(), {records, next_token}})
    end)
    {:noreply, state}
  end

  def terminate({:shutdown, :start_error}, _state) do
    Logger.info("start worker failed, terminate it.")
    :ok
  end

  def terminate(reason, state) do
    shutdown(state)

    Logger.info(fn ->
      [
        "tunnel worker terminated, reason:",
        inspect(reason)
      ]
    end)
  end

  # Private 

  defp connect_heartbeat(state, opts) do
    tunnel_id = opts[:tunnel_id]
    connect_timeout = opts[:connect_timeout]

    var_connect = %ConnectTunnel{
      tunnel_id: tunnel_id,
      timeout: connect_timeout,
      client_tag: opts[:client_tag]
    }

    result =
      Client.connect_tunnel(state.instance_key, var_connect,
        request_timeout: connect_timeout * 1_000
      )

    case result do
      {:ok, response} ->
        client_id = response.client_id

        heartbeat_interval = opts[:heartbeat_interval] * 1_000

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
             }}
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
      raise ExAliyunOts.Error,
            "Invalid parameter, heartbeat_interval should be >= #{@min_heartbeat_interval} seconds."
    end

    if heartbeat_timeout <= heartbeat_interval do
      raise ExAliyunOts.Error,
            "Invalid parameter, heartbeat_timeout should be > heartbeat_interval(#{
              heartbeat_interval
            } seconds)."
    end

    if tunnel_id == nil do
      raise ExAliyunOts.Error, "Invalid parameter, tunnel_id is required"
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

    [_tunnel_id, client_id, worker_pid, meta] = Registry.worker(tunnel_id)

    last_heartbeat_time = meta.last_heartbeat_time

    if DateTime.diff(now, last_heartbeat_time) > meta.heartbeat_timeout do
      raise ExAliyunOts.Error,
            "Tunnel client heartbeat timeout, last_heartbeat_time: #{inspect(last_heartbeat_time)}"
    end

    Logger.info("Begin heartbeat channel.")

    local_channels = Registry.channels(tunnel_id)

    {var_channels, local_channel_ids} =
      Enum.map_reduce(local_channels, [], fn local_channel, acc ->
        [channel_id, _tunnel_id, _client_id, _channel_pid, status, version] = local_channel

        {
          %VarChannel{channel_id: channel_id, version: version, status: status},
          [channel_id | acc]
        }
      end)

    Logger.info("local var channels: #{inspect(var_channels)}")

    var_heartbeat = %Heartbeat{channels: var_channels, tunnel_id: tunnel_id, client_id: client_id}

    result =
      Client.heartbeat(instance_key, var_heartbeat, request_timeout: meta.heartbeat_timeout)

    Logger.info(fn -> "heartbeat result: #{inspect(result)}" end)

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
          worker_pid,
          channels_from_response,
          local_channel_ids
        )

        Logger.info("~~~~~~~~~~~~~~~~~~~ heartbeat after update_channels ~~~~~~~~~~~~~~~~~")

        updated_meta =
          meta
          |> Map.put(:last_heartbeat_time, last_heartbeat_time)
          |> Map.put(:timer_ref, schedule_heartbeat(opts, meta.heartbeat_interval))

        Logger.info(fn ->
          [
            "updated_meta: #{inspect(updated_meta)}"
          ]
        end)

        Registry.update_worker(tunnel_id, [
          {:meta, updated_meta}
        ])

        current_local_channels = Registry.channels(tunnel_id)

        Logger.info(fn ->
          [
            "after heartbeat, current_local_channels: ",
            inspect(current_local_channels)
          ]
        end)

        :ok

      error ->
        case invalid_tunnel?(error) do
          true ->
            Logger.info(fn -> "occur invalid tunnel" end)

          false ->
            Logger.info(fn ->
              [
                "occur unknown error when heartbeat ",
                inspect(error)
              ]
            end)
        end

        error
    end
  end

  defp invalid_tunnel?({:error, error_msg}) do
    Logger.error(">>>>>>> error_msg: #{inspect(error_msg)}")

    Logger.error(fn ->
      [
        "invalid_tunnel? ",
        inspect(error_msg)
      ]
    end)

    String.contains?(error_msg, "OTSParameterInvalid") or
      String.contains?(error_msg, "OTSTunnelExpired")
  end

  defp invalid_tunnel?(error) do
    Logger.error(fn ->
      [
        "invalid_tunnel? ",
        inspect(error),
        " occur unknown error"
      ]
    end)

    false
  end

  defp shutdown(state) do
    case Registry.worker(self()) do
      [tunnel_id, client_id, _worker_pid, meta] ->
        timer_ref = Map.get(meta, :timer_ref)

        if timer_ref != nil do
          cancel_timer_result = Process.cancel_timer(timer_ref)

          Logger.info(fn ->
            [
              "cancel_timer_result when shutdown worker: ",
              inspect(cancel_timer_result)
            ]
          end)
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
    var_shutdown = %Shutdown{tunnel_id: tunnel_id, client_id: client_id}
    shutdown_result = Client.shutdown_tunnel(instance_key, var_shutdown)

    Logger.info(fn ->
      [
        "shutdown tunnel with tunnel_id: ",
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
         worker_pid,
         channels_from_response,
         local_channel_ids
       ) do
    Enum.map(channels_from_response, fn channel_from_heartbeat ->
      channel_id = channel_from_heartbeat.channel_id

      case Registry.channel(channel_id) do
        nil ->
          # not existed yet in local
          {:ok, channel_pid} =
            init_channel(instance_key, tunnel_id, client_id, worker_pid, channel_from_heartbeat)

          Channel.update(channel_pid, channel_from_heartbeat)

        [_channel_id, _tunnel_id, _client_id, channel_pid, _status, _version] ->
          # already in local
          Channel.update(channel_pid, channel_from_heartbeat)
      end
    end)

    local_channel_ids
    |> to_be_removed_channel_ids(channels_from_response)
    |> Enum.map(fn tbr_channel_id ->
      case Registry.channel(tbr_channel_id) do
        nil ->
          :ok

        [_channel_id, _tunnel_id, _client_id, channel_pid, _status, _version] ->
          Logger.info(
            "stop channel process: #{inspect(channel_pid)} for channel_id: #{
              inspect(tbr_channel_id)
            }"
          )

          Channel.stop(channel_pid)
      end
    end)
  end

  defp init_channel(instance_key, tunnel_id, client_id, worker_pid, channel_from_heartbeat) do
    channel_id = channel_from_heartbeat.channel_id

    var = %GetCheckpoint{
      tunnel_id: tunnel_id,
      client_id: client_id,
      channel_id: channel_id
    }

    result = Client.get_checkpoint(instance_key, var)

    case result do
      {:ok, response} ->

        conn_opts = [
          worker: worker_pid,
          tunnel_id: tunnel_id,
          channel_id: channel_id,
          client_id: client_id,
          token: response.checkpoint,
          finished?: false,
          instance_key: instance_key,
          sequence_number: response.sequence_number + 1
        ]

        {:ok, connection} = start_connection(conn_opts)

        start_channel(
          channel_id,
          tunnel_id,
          client_id,
          channel_from_heartbeat.status,
          channel_from_heartbeat.version,
          connection
        )

      error_result ->
        Logger.error(fn ->
          [
            "GetCheckpointer occur error: ",
            inspect(error_result)
          ]
        end)

        error_result
    end
  end

  defp start_connection(opts) do
    stream? = Utils.stream_token?(opts[:token])
    opts = if stream?, do: Keyword.put(opts, :backoff, Backoff.new()), else: opts
    Logger.info "start_connection with opts: #{inspect opts}"
    Connection.start_link(opts)
  end

  defp start_channel(channel_id, tunnel_id, client_id, status, version, connection) do
    Channel.start_link(channel_id, tunnel_id, client_id, status, version, connection)
  end

  defp subscribe(pid, state) do
    ref = Process.monitor(pid)
    state = put_in(state, [:subscribers, ref], pid)
    {:noreply, state}
  end

end
