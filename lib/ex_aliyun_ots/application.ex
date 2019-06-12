defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application

  alias ExAliyunOts.{Client, Instance}

  alias ExAliyunOts.Tunnel.Worker, as: TunnelWorker
  alias ExAliyunOts.Tunnel.Registry

  @app Mix.Project.config[:app]
  import Supervisor.Spec, warn: false

  def start(_type, _args) do

    opts = [strategy: :one_for_one]
    children = instances_pool_child_spec()
    Supervisor.start_link(children, opts)
  end

  defp instances_pool_child_spec() do
    instances = Application.get_env(@app, :instances)

    Enum.reduce(instances, [], fn (instance_key, acc) ->

      instance =
        instance_key
        |> init_instance()
        |> config_instance(Application.get_env(@app, instance_key))

      instance_child =
        :poolboy.child_spec(
          instance_key,
          config_pool(instance),
          instance
        )

      tunnel = instance.tunnel

      case Keyword.get(tunnel, :enabled?, false) do
        false ->
          [instance_child | acc]
        true ->
          registeries = [
            worker(Registry, []),
          ]
          tunnel_child =
            :poolboy.child_spec(
              TunnelWorker.pool_name(instance_key),
              config_tunnel_pool(instance_key, tunnel),
              instance_key
            )
          registeries ++ [tunnel_child | [instance_child | acc]]
      end

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
      case key do
        :tunnel ->
          tunnel_config = Keyword.get(config, key)
          Map.put(acc, key, [
            enabled?: Keyword.get(tunnel_config, :enable?, false),
            pool_size: Keyword.get(tunnel_config, :pool_size, 32),
            pool_max_overflow: Keyword.get(tunnel_config, :pool_max_overflow, 100)
          ])
        _ ->
          case Keyword.get(config, key) do
            nil ->
              acc
            value ->
              Map.put(acc, key, value)
          end
      end
    end)
  end

  defp config_tunnel_pool(instance_key, tunnel) do
    [
      {:name, {:local, TunnelWorker.pool_name(instance_key)}},
      {:worker_module, TunnelWorker},
      {:size, Keyword.get(tunnel, :pool_size, 32)},
      {:max_overflow, Keyword.get(tunnel, :pool_max_overflow, 100)},
      {:strategy, :fifo}
    ]
  end
end
