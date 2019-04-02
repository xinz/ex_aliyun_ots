defmodule ExAliyunOts.Logger do
  @moduledoc false

  require Logger

  @debug? Application.get_env(Mix.Project.config[:app], :debug, false)

  def debug(fun) do
    if @debug? do
      Logger.debug(fun)
    end
  end

  defdelegate error(chardata_or_fun, metadata \\ []), to: Logger

  defdelegate info(chardata_or_fun, metadata \\ []), to: Logger
end
