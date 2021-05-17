defmodule ExAliyunOts.Config do
  @moduledoc false

  use GenServer

  def start_link(instances) do
    GenServer.start_link(__MODULE__, instances, name: __MODULE__)
  end

  def get(instance_key) do
    case :ets.lookup(__MODULE__, instance_key) do
      [] -> nil
      [{^instance_key, info}] -> info
    end
  end

  ## Callback

  def init(instances) do
    ets = :ets.new(__MODULE__, [:set, :public, :named_table, read_concurrency: true])

    for {instance_key, instance_info} <- instances do
      :ets.insert(__MODULE__, {instance_key, instance_info})
    end

    {:ok, ets}
  end
end
