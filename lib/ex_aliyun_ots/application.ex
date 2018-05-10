defmodule ExAliyunOts.Application do
  @moduledoc false

  use Application
  require Logger
  alias ExAliyunOts.{Client, Instance}

  @clients_pool Application.get_env(:ex_aliyun_ots, :clients_pool, size: 100, max_overflow: 100)

  def start(_type, _args) do
    children = load_ots_clients()
    opts = [strategy: :one_for_one]
    Supervisor.start_link(children, opts)
  end

  defp load_ots_clients do
    instances = Application.fetch_env!(:ex_aliyun_ots, :instances)

    Enum.map(Map.keys(instances), fn instance_name ->
      instance_conf = Map.get(instances, instance_name)

      instance = %Instance{
        endpoint: instance_conf.endpoint,
        name: instance_name,
        access_key_id: instance_conf.access_key_id,
        access_key_secret: instance_conf.access_key_secret
      }

      :poolboy.child_spec(Client.pool_name(instance_name), pool_config_to_client(instance_name), [
        instance
      ])
    end)
  end

  defp pool_config_to_client(instance_name) do
    [
      {:name, {:local, Client.pool_name(instance_name)}},
      {:worker_module, Client},
      {:size, Keyword.get(@clients_pool, :size)},
      {:max_overflow, Keyword.get(@clients_pool, :max_overflow)},
      {:strategy, :fifo}
    ]
  end
end
