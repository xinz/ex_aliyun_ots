defmodule ExAliyunOts.Tunnel.DynamicSupervisor do
  @moduledoc false

  use DynamicSupervisor

  alias ExAliyunOts.Tunnel.Worker

  def start_link([]) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_tunnel_worker(instance) do
    spec = {Worker, instance}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  def terminate_child(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end
end
