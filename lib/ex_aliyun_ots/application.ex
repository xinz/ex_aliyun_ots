defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application

  alias ExAliyunOts.Instance

  import Supervisor.Spec, warn: false

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

  defp child_spec(_app, _enable_tunnel, []) do
    raise ExAliyunOts.RuntimeError, "Not found any instances configuration"
  end

  defp child_spec(app, true, instance_keys) do
    [
      worker(ExAliyunOts.Config, [load_instances(app, instance_keys)]),
      worker(ExAliyunOts.Tunnel.Registry, []),
      supervisor(ExAliyunOts.Tunnel.DynamicSupervisor, [])
    ]
  end

  defp child_spec(app, false, instance_keys) do
    [
      worker(ExAliyunOts.Config, [load_instances(app, instance_keys)])
    ]
  end

  defp load_instances(app, instance_keys) do
    Enum.reduce(instance_keys, [], fn instance_key, acc ->
      [
        {
          instance_key,
          Map.merge(%Instance{}, Enum.into(Application.get_env(app, instance_key), %{}))
        }
        | acc
      ]
    end)
  end
end
