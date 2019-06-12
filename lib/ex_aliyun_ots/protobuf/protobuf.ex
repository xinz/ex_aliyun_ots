defmodule ExAliyunOts.TableStore do
  use Protobuf, from: Path.expand("./table_store.proto", __DIR__)
end

defmodule ExAliyunOts.TableStoreFilter do
  use Protobuf, from: Path.expand("./table_store_filter.proto", __DIR__)
end

defmodule ExAliyunOts.TableStoreSearch do
  use Protobuf, from: Path.expand("./search.proto", __DIR__)
end

defmodule ExAliyunOts.TableStoreTunnel do
  use Protobuf, from: Path.expand("./tunnel_service_api.proto", __DIR__)
end
