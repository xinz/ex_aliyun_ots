defmodule ExAliyunOts.Tunnel.Backoff do
  @moduledoc false

  @random_factor 0.25
  @multiplier 5
  @max_interval_ms 5000

  defstruct current_interval_ms: 10, start_time_ms: nil

  alias ExAliyunOts.Tunnel.Utils

  def new() do
    reset()
  end

  def reset() do
    %__MODULE__{
      start_time_ms: Utils.utc_now_ms()
    }
  end

  def next_backoff_ms(cur_backoff) do
    current_interval_ms = cur_backoff.current_interval_ms

    randomized_interval =
      random_value_interval(@random_factor, :rand.uniform(), current_interval_ms)

    updated_backoff =
      if current_interval_ms >= @max_interval_ms / @multiplier do
        Map.put(cur_backoff, :current_interval_ms, @max_interval_ms)
      else
        Map.put(cur_backoff, :current_interval_ms, current_interval_ms * @multiplier)
      end

    {updated_backoff, randomized_interval}
  end

  defp random_value_interval(factor, random, current_interval_ms) do
    delta = factor * current_interval_ms
    min_interval = current_interval_ms - delta
    max_interval = current_interval_ms + delta
    trunc(min_interval + random * (max_interval - min_interval + 1))
  end
end
