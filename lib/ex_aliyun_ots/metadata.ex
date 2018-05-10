defmodule ExAliyunOts.Error do
  defexception [:message, :error_code]

  def exception(value) do
    msg = "Error: #{inspect value}"
    %ExAliyunOts.Error{message: msg}
  end
end

defmodule ExAliyunOts.Instance do
  defstruct [
    :access_key_id,
    :access_key_secret,
    :name,
    :endpoint
  ]
end

defmodule ExAliyunOts.HTTPRequest do
  defstruct instance: nil, uri: "", body: nil, method: "POST"
end

defmodule ExAliyunOts.Var.TimeRange do
  defstruct [:start_time, :end_time, :specific_time]
end

defmodule ExAliyunOts.Var.Filter do
  defstruct [:filter, :filter_type]
end

defmodule ExAliyunOts.Var.CompositeColumnValueFilter do
  defstruct combinator: nil, sub_filters: []
end

defmodule ExAliyunOts.Var.ColumnPaginationFilter do
  defstruct offset: 0, limit: 0
end

defmodule ExAliyunOts.Var.SingleColumnValueFilter do
  defstruct comparator: nil, column_name: nil, column_value: nil, ignore_if_missing: false, latest_version_only: true
end

defmodule ExAliyunOts.Var.Condition do
  defstruct row_existence: nil, column_condition: nil
end

defmodule ExAliyunOts.Var.UpdateRow do
  alias ExAliyunOts.Const.ReturnType
  require ReturnType
  defstruct table_name: "", primary_keys: [], updates: %{}, condition: %ExAliyunOts.Var.Condition{}, return_type: ReturnType.none
end

defmodule ExAliyunOts.Var.PutRow do
  alias ExAliyunOts.Const.ReturnType
  require ReturnType
  defstruct table_name: "", primary_keys: [], attribute_columns: [], condition: %ExAliyunOts.Var.Condition{}, return_type: ReturnType.none
end

defmodule ExAliyunOts.Var.GetRow do
  defstruct table_name: "", primary_keys: [], columns_to_get: [], max_versions: 1, time_range: nil, filter: nil, start_column: nil, end_column: nil
end

defmodule ExAliyunOts.Var.DeleteRow do
  alias ExAliyunOts.Const.ReturnType
  require ReturnType
  defstruct table_name: "", primary_keys: [], condition: %ExAliyunOts.Var.Condition{}, return_type: ReturnType.none
end

defmodule ExAliyunOts.Var.StreamSpec do
  defstruct is_enabled: nil, expiration_time: nil
end

defmodule ExAliyunOts.Var.CreateTable do
  defstruct table_name: "", primary_keys: [], reserved_throughput_write: 0, reserved_throughput_read: 0, time_to_live: -1, max_versions: 1, deviation_cell_version_in_sec: 86_400, stream_spec: %ExAliyunOts.Var.StreamSpec{}
end

defmodule ExAliyunOts.Var.UpdateTable do
  defstruct table_name: "", reserved_throughput_write: nil, reserved_throughput_read: nil, time_to_live: -1, max_versions: 1, deviation_cell_version_in_sec: 86_400, stream_spec: %ExAliyunOts.Var.StreamSpec{}
end

defmodule ExAliyunOts.Var.GetRange do
  alias ExAliyunOts.Const.Direction
  require Direction
  defstruct table_name: "", direction: Direction.forward, columns_to_get: [], time_range: nil, max_versions: 1, limit: nil, inclusive_start_primary_keys: nil, exclusive_end_primary_keys: nil, filter: nil, start_column: nil, end_column: nil
end

defmodule ExAliyunOts.Var.RowInBatchWriteRequest do
  alias ExAliyunOts.Const.ReturnType
  require ReturnType
  defstruct type: nil, primary_keys: [], updates: nil, condition: %ExAliyunOts.Var.Condition{}, return_type: ReturnType.none
end

defmodule ExAliyunOts.Var.BatchWriteRequest do
  defstruct table_name: "", rows: []
end

defmodule ExAliyunOts.Var.NewSequence do
  defstruct name: "", reserved_throughput_write: 0, reserved_throughput_read: 0, deviation_cell_version_in_sec: 86_400
end

defmodule ExAliyunOts.Var.GetSequenceNextValue do
  defstruct name: "", event: "default", starter: 0, increment_offset: 1
end
