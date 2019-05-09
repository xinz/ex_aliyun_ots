defmodule ExAliyunOts.Tunnel.Channel.Connection do
  @moduledoc false

  use Agent

  alias ExAliyunOts.{Client, Logger}
  alias ExAliyunOts.Tunnel.{Worker, Utils, Checkpointer, Backoff}

  alias ExAliyunOts.Var.Tunnel.ReadRecords
  alias ExAliyunOts.Const.Tunnel.{Common, ChannelConnectionStatus}

  require Common
  require ChannelConnectionStatus

  @rpo_bar 500
  # 900 KB
  @rpo_size_bar 900 * 1024

  defstruct worker: nil,
            tunnel_id: nil,
            channel_id: nil,
            client_id: nil,
            token: nil,
            finished?: false,
            stream?: false,
            instance_key: nil,
            backoff: nil,
            status: ChannelConnectionStatus.wait(),
            sequence_number: nil,
            latest_checkpoint: nil

  def start_link(opts) do
    Agent.start_link(fn ->
      %__MODULE__{}
      |> struct(opts)
      |> Map.put(:latest_checkpoint, Utils.utc_now_ms())
    end)
  end

  def stop(conn) do
    if Process.alive?(conn) do
      Logger.info("stop live connection pid: #{inspect(conn)}")
      Agent.stop(conn, {:shutdown, :channel_connection_finished})
    end
  end

  def process(conn, handler_module) do
    Logger.info("connection process conn: #{inspect(conn)}")

    conn
    |> read_records()
    |> process_records(handler_module)
  end

  def status(conn) do
    Agent.get(
      conn,
      fn state ->
        state.status
      end,
      :infinity
    )
  end

  def finished?(conn) do
    Agent.get(
      conn,
      fn state ->
        state.finished?
      end,
      :infinity
    )
  end

  def status_running(conn) do
    update_status(conn, ChannelConnectionStatus.running())
  end

  def status_closing(conn) do
    update_status(conn, ChannelConnectionStatus.closing())
  end

  def status_closed(conn) do
    update_status(conn, ChannelConnectionStatus.closed())
  end

  defp read_records(conn) do
    Agent.get_and_update(
      conn,
      fn state ->
        token = state.token

        Logger.info(fn ->
          "read_records with token: #{inspect(token)}"
        end)

        if token != nil and token != Common.finish_tag() do
          result =
            Client.read_records(
              state.instance_key,
              %ReadRecords{
                tunnel_id: state.tunnel_id,
                client_id: state.client_id,
                channel_id: state.channel_id,
                token: token
              }
            )

          Logger.info(fn ->
            "read_records result: #{inspect(result)}"
          end)

          case result do
            {:ok, response, size} ->
              backoff = state.backoff
              records = response.records
              next_token = response.next_token

              if backoff != nil do
                updated_backoff =
                  if stream_full_data?(length(records), size) do
                    Logger.debug(fn -> "reset backoff" end)
                    Backoff.reset()
                  else
                    {next_backoff, sleep_ms} = Backoff.next_backoff_ms(backoff)
                    Process.sleep(sleep_ms)
                    next_backoff
                  end

                {
                  {records, next_token, conn},
                  Map.put(state, :backoff, updated_backoff)
                }
              else
                {
                  {records, next_token, conn},
                  state
                }
              end

            {:error, error_msg} ->
              Logger.error(fn ->
                [
                  "occur an error when read_records, ",
                  "tunnel_id: ",
                  inspect(state.tunnel_id),
                  " client_id: ",
                  inspect(state.client_id),
                  " channel_id: ",
                  inspect(state.channel_id),
                  " token: ",
                  inspect(token),
                  " error message: ",
                  inspect(error_msg)
                ]
              end)

              if stream_channel_expired?(error_msg) do
                # The tunnel has a 7-day life cycle, if there's an active connect-read-check loop, the tunnel will not expire.
                # raise ExAliyunOts.Error, "#{error_msg}. The current tunnel is expired, please renew one tunnel to use it."
                {:tunnel_expired, state}
              else
                {nil, state}
              end
          end
        else
          Logger.info(fn ->
            [
              "channel is finished, it will be closed, token: ",
              inspect(token)
            ]
          end)

          {nil, state}
        end
      end,
      :infinity
    )
  end

  defp process_records({records, next_token, conn}, handler_module) do
    Agent.update(
      conn,
      fn state ->
        Logger.info(
          ">>> handle_messages from connection@#{inspect(self())} with records: #{
            inspect(records)
          } <<<"
        )

        Worker.handle_messages(
          state.worker,
          records,
          next_token,
          handler_module
        )

        checkpointer = %Checkpointer{
          tunnel_id: state.tunnel_id,
          client_id: state.client_id,
          instance_key: state.instance_key,
          channel_id: state.channel_id,
          sequence_number: state.sequence_number
        }

        updated_sequence_number =
          if next_token == nil or Common.finish_tag() == next_token do
            checkpointer
            |> Map.put(:token, Common.finish_tag())
            |> Checkpointer.checkpoint()
          else
            checkpointer
            |> Map.put(:token, next_token)
            |> Checkpointer.checkpoint()
          end

        state
        |> Map.put(:token, next_token)
        |> Map.put(:sequence_number, updated_sequence_number)
      end,
      :infinity
    )

    if Common.finish_tag() == next_token do
      :finished
    else
      :ok
    end
  end

  defp process_records(:tunnel_expired, _handler_module) do
    :tunnel_expired
  end

  defp process_records(nil, _handler_module) do
    :ok
  end

  defp update_status(conn, new_status)
       when new_status == ChannelConnectionStatus.running()
       when new_status == ChannelConnectionStatus.closing()
       when new_status == ChannelConnectionStatus.closed() do
    Agent.update(
      conn,
      fn state ->
        Map.put(state, :status, new_status)
      end,
      :infinity
    )
  end

  defp update_status(_conn, new_status) do
    Logger.error(fn ->
      "update with invalid new_status: #{inspect(new_status)}"
    end)
  end

  defp stream_full_data?(records_num, size) do
    records_num > @rpo_bar or size > @rpo_size_bar
  end

  defp stream_channel_expired?(error_msg) do
    String.contains?(error_msg, "OTSTunnelServerUnavailableuOTSTrimmedDataAccess")
  end
end

defmodule ExAliyunOts.Tunnel.Channel.FailedConnection do
  # TODO
  :todo
end
