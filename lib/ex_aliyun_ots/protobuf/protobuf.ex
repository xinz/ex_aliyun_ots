defmodule ExAliyunOts.TableStore do
  use Protobuf, from: Path.expand("./table_store.proto", __DIR__)
end

defmodule ExAliyunOts.TableStoreFilter do
  use Protobuf, from: Path.expand("./table_store_filter.proto", __DIR__)
end
