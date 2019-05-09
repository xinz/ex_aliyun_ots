defmodule ExAliyunOts.Var.Tunnel.DescribeTunnel do
  defstruct [table_name: "", tunnel_name: "", tunnel_id: nil]
end

defmodule ExAliyunOts.Var.Tunnel.ConnectTunnel do
  # timeout, default 300 seconds
  defstruct [tunnel_id: nil, timeout: 300, client_tag: nil]
end

defmodule ExAliyunOts.Var.Tunnel.Heartbeat do
  defstruct [tunnel_id: nil, client_id: nil, channels: []]
end

defmodule ExAliyunOts.Var.Tunnel.Channel do
  defstruct [channel_id: nil, version: nil, status: nil]
end

defmodule ExAliyunOts.Var.Tunnel.Shutdown do
  defstruct [tunnel_id: nil, client_id: nil]
end

defmodule ExAliyunOts.Var.Tunnel.GetCheckpoint do
  defstruct [tunnel_id: nil, client_id: nil, channel_id: nil]
end

defmodule ExAliyunOts.Var.Tunnel.Checkpoint do
  defstruct [tunnel_id: nil, client_id: nil, channel_id: nil, checkpoint: nil, sequence_number: nil]
end

defmodule ExAliyunOts.Var.Tunnel.ReadRecords do
  defstruct [tunnel_id: nil, client_id: nil, channel_id: nil, token: nil]
end

defmodule ExAliyunOts.Var.Tunnel.Start do
  # all time related variables in seconds
  defstruct [tunnel_id: nil, connect_timeout: 300, client_tag: nil, heartbeat_timeout: 300, heartbeat_interval: 30, handler_module: nil]
end
