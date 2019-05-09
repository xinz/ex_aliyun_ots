defmodule ExAliyunOts.Tunnel.EntryWorker do
  @moduledoc false

  require Record

  @fields [tunnel_id: nil, client_id: nil, pid: nil, meta: %{}]

  Record.defrecord(:entry_worker, @fields)

  @type entry_worker ::
          record(
            :entry_worker,
            tunnel_id: String.t(),
            client_id: String.t(),
            pid: pid(),
            meta: map()
          )

  def index(field) when is_atom(field) do
    Record.__access__(:entry_worker, @fields, field, __MODULE__)
  end
end

defmodule ExAliyunOts.Tunnel.EntryChannel do
  @moduledoc false

  require Record

  @fields [channel_id: nil, tunnel_id: nil, client_id: nil, pid: nil, status: nil, version: nil]

  Record.defrecord(:entry_channel, @fields)

  @type entry_channel ::
          record(
            :entry_channel,
            channel_id: String.t(),
            tunnel_id: String.t(),
            client_id: String.t(),
            pid: pid(),
            status: String.t(),
            version: integer()
          )

  def index(field) when is_atom(field) do
    Record.__access__(:entry_channel, @fields, field, __MODULE__)
  end
end
