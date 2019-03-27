defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application
  require Logger
  alias ExAliyunOts.{Client, Instance}

  @app Mix.Project.config[:app]

  def start(_type, _args) do
    opts = [strategy: :one_for_one]
    Supervisor.start_link(instances_child_spec(), opts)
  end

  defp instances_child_spec do
    instances = Application.get_env(@app, :instances)

    Enum.map(instances, fn instance_key ->

      instance =
        instance_key
        |> init_instance()
        |> config_instance(Application.get_env(@app, instance_key))

      :poolboy.child_spec(
        instance_key,
        config_pool(instance),
        [
          instance
        ]
      )
    end)
  end

  defp config_pool(instance) do
    [
      {:name, {:local, instance.pool_name}},
      {:worker_module, Client},
      {:size, instance.pool_size},
      {:max_overflow, instance.pool_max_overflow},
      {:strategy, :fifo}
    ]
  end

  defp init_instance(instance_key) do
    %Instance{
      pool_name: instance_key,
      pool_size: 100,
      pool_max_overflow: 20
    }
  end

  defp config_instance(instance, config) do
    instance
    |> Map.keys()
    |> Enum.reduce(instance, fn(key, acc) ->
      case Keyword.get(config, key) do
        nil ->
          acc
        value ->
          Map.put(acc, key, value)
        end
    end)
  end
end
