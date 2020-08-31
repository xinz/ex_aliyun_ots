defmodule ExAliyunOts.Logger do
  @moduledoc false

  require Logger

  def debug(fun) do
    enable_debug = Application.get_application(__MODULE__) |> Application.get_env(:debug, false)

    if enable_debug do
      Logger.debug(fun)
    end
  end

  defdelegate error(chardata_or_fun, metadata \\ []), to: Logger

  defdelegate info(chardata_or_fun, metadata \\ []), to: Logger
end
