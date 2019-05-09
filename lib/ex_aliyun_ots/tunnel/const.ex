defmodule ExAliyunOts.Const.Tunnel.ChannelStatus do
  import ExAliyunOts.Constants

  const(:wait, :WAIT)
  const(:open, :OPEN)
  const(:closing, :CLOSING)
  const(:close, :CLOSE)
  const(:terminated, :TERMINATED)
end

defmodule ExAliyunOts.Const.Tunnel.ChannelConnectionStatus do
  import ExAliyunOts.Constants

  const(:wait, :WAIT)
  const(:running, :RUNNING)
  const(:closing, :CLOSING)
  const(:closed, :CLOSED)
end

defmodule ExAliyunOts.Const.Tunnel.Common do
  import ExAliyunOts.Constants

  const(:finish_tag, "finished")
end
