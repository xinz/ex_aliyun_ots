defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application

  alias ExAliyunOts.Instance

  def start(_type, _args) do
    app = Application.get_application(__MODULE__)
    enable_tunnel = Application.get_env(app, :enable_tunnel, false)
    instances = Application.get_env(app, :instances, [])

    children =
      child_spec(
        app,
        enable_tunnel,
        instances
      )

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def http_name() do
    __MODULE__.Finch
  end

  defp child_spec(_app, _enable_tunnel, []) do
    raise ExAliyunOts.RuntimeError, "Not found any instances configuration"
  end

  defp child_spec(app, true, instance_keys) do
    child_spec_base(app, instance_keys) ++
      [
        ExAliyunOts.Tunnel.Registry,
        ExAliyunOts.Tunnel.DynamicSupervisor
      ]
  end

  defp child_spec(app, false, instance_keys) do
    child_spec_base(app, instance_keys)
  end

  defp child_spec_base(app, instance_keys) do
    {instances, pools} = load_instances(app, instance_keys)

    [
      child_spec_http_client(pools),
      {ExAliyunOts.Config, instances}
    ]
  end

  defp child_spec_http_client(pools) do
    {
      Finch,
      name: http_name(), pools: pools
    }
  end

  defp load_instances(app, instance_keys) do
    Enum.reduce(instance_keys, {[], %{}}, fn instance_key, {instances, pools_map} ->
      instance = struct(Instance, Application.get_env(app, instance_key))
      instances = [{instance_key, instance} | instances]

      pools_map =
        Map.put(pools_map, instance.endpoint,
          size: instance.pool_size || 100,
          count: instance.pool_count || 1
        )

      {instances, pools_map}
    end)
  end
end
