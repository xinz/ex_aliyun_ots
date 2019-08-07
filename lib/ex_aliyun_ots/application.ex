defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application

  alias ExAliyunOts.Instance

  import Supervisor.Spec, warn: false

  @app Mix.Project.config[:app]

  def start(_type, _args) do

    instance_keys = Application.get_env(@app, :instances, [])

    enable_tunnel? = Application.get_env(@app, :enable_tunnel?, false)

    children = child_spec(enable_tunnel?, instance_keys)

    Supervisor.start_link(children, [strategy: :one_for_one])
  end

  defp child_spec(_enable_tunnel, []) do
    raise ExAliyunOts.Error, "Not found any instances configuration"
  end
  defp child_spec(_enable_tunnel = true, instance_keys) do
    [
      worker(ExAliyunOts.Config, [load_instances(instance_keys)]),
      worker(ExAliyunOts.Tunnel.Registry, []),
      supervisor(ExAliyunOts.Tunnel.DynamicSupervisor, [])
    ]
  end
  defp child_spec(_enable_tunnel = false, instance_keys) do
    [
      worker(ExAliyunOts.Config, [load_instances(instance_keys)]),
    ]
  end

  defp load_instances(instance_keys) do
    Enum.reduce(instance_keys, [], fn(instance_key, acc) ->
      [
        {
          instance_key,
          Map.merge(%Instance{}, Enum.into(Application.get_env(@app, instance_key), %{}))
        } | acc
      ]
    end)
  end

end
