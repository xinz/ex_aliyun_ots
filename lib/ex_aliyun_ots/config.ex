defmodule ExAliyunOts.Config do
  @moduledoc false

  use GenServer

  def start_link(instances) do
    GenServer.start_link(__MODULE__, instances, name: __MODULE__)
  end

  def get(instance_key) do
    GenServer.call(__MODULE__, {:get, instance_key}, 30_000)
  end

  ## Callback
  
  def init(instances) do
    ets = :ets.new(__MODULE__, [:set, :named_table, read_concurrency: true])
    for {instance_key, instance_info} <- instances do
      :ets.insert(__MODULE__, {instance_key, instance_info})
    end
    {:ok, ets}
  end

  def handle_call({:get, instance_key}, _from, ets) do
    case :ets.lookup(__MODULE__, instance_key) do
      [] ->
        {:reply, nil, ets}
      [{^instance_key, info}] ->
        {:reply, info, ets}
    end
  end

end
