defmodule ExAliyunOts.Tunnel.Channel.Agent do
  @moduledoc false

  use Agent

  alias ExAliyunOts.{Client, Logger}
  alias ExAliyunOts.Tunnel.{Checkpointer, Backoff, Registry}

  @rpo_bar 500
  # 900 KB
  @rpo_size_bar 900 * 1024

  defstruct tunnel_id: nil,
            channel_id: nil,
            client_id: nil,
            token: nil,
            instance_key: nil,
            backoff: nil,
            sequence_number: nil

  def start_link(opts) do
    Agent.start_link(fn ->
      struct(%__MODULE__{}, opts)
    end)
  end

  def stop(conn) do
    if Process.alive?(conn) do
      Logger.info("Stop live agent pid: #{inspect(conn)}")
      Agent.stop(conn, :shutdown)
    end
  end

  def process(conn) do
    Agent.cast(conn, fn(state) ->
      state
      |> read_records()
      |> process_records()
    end)
  end


  defp read_records(state) do
    token = state.token
    finish_tag = Checkpointer.finish_tag()

    if token != nil and token != finish_tag do
      result =
        Client.read_records(
          state.instance_key,
          tunnel_id: state.tunnel_id,
          client_id: state.client_id,
          channel_id: state.channel_id,
          token: token
        )

      case result do
        {:ok, response, size} ->
          backoff = state.backoff
          records = response.records
          next_token = response.next_token

          if backoff != nil do
            updated_backoff =
              if stream_full_data?(length(records), size) do
                Logger.debug(fn -> "Reset backoff" end)
                Backoff.reset()
              else
                {next_backoff, sleep_ms} = Backoff.next_backoff_ms(backoff)
                Process.sleep(sleep_ms)
                next_backoff
              end

            {records, next_token, Map.put(state, :backoff, updated_backoff)}
          else
            {records, next_token, state}
          end

        {:error, error_msg} ->
          Logger.error(fn ->
            [
              "Occur an error when read_records, ",
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
            {:tunnel_expired, state}
          else
            {nil, state}
          end
      end
    else
      Logger.info(fn ->
          [
            "Channel is finished, it will be closed, token: ",
            inspect(token)
          ]
        end)
      {nil, state}
    end
  end

  defp process_records({records, next_token, state}) do
    Logger.info(fn ->
      [
        ">>> Handle records for channel_id: ",
        inspect(state.channel_id),
        " client_id: ",
        inspect(state.client_id),
        " with records: ",
        inspect(records)
      ]
    end)

    case Registry.subscriber(state.tunnel_id) do
      nil ->
        Logger.info "Not found subscriber for tunnel_id: #{inspect(state.tunnel_id)} consume records, so will not invoke checkpoint"
        state

      {_ref, subscriber_pid} ->

        GenServer.call(subscriber_pid, {:record_event, {records, next_token}}, :infinity)

        Logger.info "Start checkpointer after consume records for tunnel_id: #{state.tunnel_id} / client_id: #{state.client_id} / channel_id: #{state.channel_id}, next_token: #{inspect next_token}"

        checkpointer = %Checkpointer{
          tunnel_id: state.tunnel_id,
          client_id: state.client_id,
          instance_key: state.instance_key,
          channel_id: state.channel_id,
          sequence_number: state.sequence_number
        }

        finish_tag = Checkpointer.finish_tag()

        updated_sequence_number =
          if next_token == nil or finish_tag == next_token do
            checkpointer
            |> Map.put(:token, finish_tag)
            |> Checkpointer.checkpoint()
          else
            checkpointer
            |> Map.put(:token, next_token)
            |> Checkpointer.checkpoint()
          end

        Logger.info "Finish process_records in agent for tunnel_id: #{state.tunnel_id} / client_id: #{state.client_id} / channel_id: #{state.channel_id}"

        state
        |> Map.put(:token, next_token)
        |> Map.put(:sequence_number, updated_sequence_number)
    end
  end
  defp process_records({:tunnel_expired, state}) do
    state
  end
  defp process_records({:nil, state}) do
    state
  end

  defp stream_full_data?(records_num, size) do
    records_num > @rpo_bar or size > @rpo_size_bar
  end

  defp stream_channel_expired?(error_msg) do
    String.contains?(error_msg, "OTSTunnelServerUnavailableuOTSTrimmedDataAccess")
  end

end
