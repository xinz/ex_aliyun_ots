defmodule ExAliyunOts.Tunnel.Handler do
  alias ExAliyunOts.TableStoreTunnel.Record

  @type record :: [Record.t()]

  @callback handle_messages(records :: [record], next_token :: String.t()) :: any

  @callback shutdown() :: any

  def impl?(module) do
    module.module_info[:attributes]
    |> Keyword.get(:behaviour, [])
    |> Enum.member?(__MODULE__)
  end
end
