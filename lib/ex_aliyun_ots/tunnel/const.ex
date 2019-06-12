defmodule ExAliyunOts.Const.TunnelType do
  import ExAliyunOts.Constants

  const(:base, :BaseData)
  const(:stream, :Stream)
  const(:base_and_stream, :BaseAndStream)
end

defmodule ExAliyunOts.Const.Tunnel.ChannelStatus do
  import ExAliyunOts.Constants

  const(:wait, :WAIT)
  const(:open, :OPEN)
  const(:closing, :CLOSING)
  const(:close, :CLOSE)
  const(:terminated, :TERMINATED)
end
