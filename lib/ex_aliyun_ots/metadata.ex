alias ExAliyunOts.TableStore.Condition

defmodule ExAliyunOts.RuntimeError do
  @moduledoc false
  defexception [:message, :error_code]

  def exception(value) do
    msg = "Error: #{inspect(value)}"
    %__MODULE__{message: msg}
  end
end

defmodule ExAliyunOts.Instance do
  @moduledoc "Describe Alibaba TableStore instance primary information."

  @type t :: %__MODULE__{}

  defstruct [
    :access_key_id,
    :access_key_secret,
    :name,
    :endpoint
  ]
end

defmodule ExAliyunOts.HTTPRequest do
  @moduledoc false
  defstruct instance: nil, uri: "", body: nil, method: "POST"
end

defmodule ExAliyunOts.Error do
  @moduledoc "Describe request error from server http response."

  @type t :: %__MODULE__{
          code: binary,
          message: binary,
          request_id: binary,
          http_status_code: binary,
          datetime: binary
        }
  defstruct code: nil, message: "", request_id: nil, http_status_code: nil, datetime: nil
end

defmodule ExAliyunOts.Var.TimeRange do
  @moduledoc false
  defstruct [:start_time, :end_time, :specific_time]
end

defmodule ExAliyunOts.Var.UpdateRow do
  @moduledoc false
  require ExAliyunOts.Const.ReturnType, as: ReturnType

  defstruct table_name: "",
            primary_keys: [],
            updates: %{},
            condition: %Condition{},
            return_type: ReturnType.none(),
            return_columns: [],
            transaction_id: nil
end

defmodule ExAliyunOts.Var.PutRow do
  @moduledoc false
  require ExAliyunOts.Const.ReturnType, as: ReturnType

  defstruct table_name: "",
            primary_keys: [],
            attribute_columns: [],
            condition: %Condition{},
            return_type: ReturnType.none(),
            transaction_id: nil
end

defmodule ExAliyunOts.Var.GetRow do
  @moduledoc false
  defstruct table_name: "",
            primary_keys: [],
            columns_to_get: [],
            max_versions: 1,
            time_range: nil,
            filter: nil,
            start_column: nil,
            end_column: nil,
            transaction_id: nil
end

defmodule ExAliyunOts.Var.DeleteRow do
  @moduledoc false
  require ExAliyunOts.Const.ReturnType, as: ReturnType

  defstruct table_name: "",
            primary_keys: [],
            condition: %Condition{},
            return_type: ReturnType.none(),
            transaction_id: nil
end

defmodule ExAliyunOts.Var.StreamSpec do
  @moduledoc false
  defstruct is_enabled: nil, expiration_time: nil
end

defmodule ExAliyunOts.Var.CreateTable do
  @moduledoc false
  defstruct table_name: "",
            primary_keys: [],
            defined_columns: [],
            reserved_throughput_write: 0,
            reserved_throughput_read: 0,
            time_to_live: -1,
            max_versions: 1,
            deviation_cell_version_in_sec: 86_400,
            stream_spec: %ExAliyunOts.Var.StreamSpec{},
            index_metas: []
end

defmodule ExAliyunOts.Var.UpdateTable do
  @moduledoc false
  defstruct table_name: "",
            reserved_throughput_write: nil,
            reserved_throughput_read: nil,
            time_to_live: -1,
            max_versions: 1,
            deviation_cell_version_in_sec: 86_400,
            stream_spec: %ExAliyunOts.Var.StreamSpec{}
end

defmodule ExAliyunOts.Var.GetRange do
  @moduledoc false
  require ExAliyunOts.Const.Direction, as: Direction

  defstruct table_name: "",
            direction: Direction.forward(),
            columns_to_get: [],
            time_range: nil,
            max_versions: 1,
            limit: nil,
            inclusive_start_primary_keys: nil,
            exclusive_end_primary_keys: nil,
            filter: nil,
            start_column: nil,
            end_column: nil,
            transaction_id: nil
end

defmodule ExAliyunOts.Var.RowInBatchWriteRequest do
  @moduledoc false
  require ExAliyunOts.Const.ReturnType, as: ReturnType

  defstruct type: nil,
            primary_keys: [],
            updates: nil,
            condition: %Condition{},
            return_type: ReturnType.none(),
            return_columns: []
end

defmodule ExAliyunOts.Var.BatchWriteRequest do
  @moduledoc false
  defstruct table_name: "", rows: []
end

defmodule ExAliyunOts.Var.NewSequence do
  @moduledoc false
  defstruct name: "",
            reserved_throughput_write: 0,
            reserved_throughput_read: 0,
            deviation_cell_version_in_sec: 86_400
end

defmodule ExAliyunOts.Var.GetSequenceNextValue do
  @moduledoc false
  defstruct name: "", event: "default", starter: 0, increment_offset: 1
end

# SearchIndex

defmodule ExAliyunOts.Var.Search do
  @moduledoc false

  defmodule IndexSetting do
    @moduledoc false
    defstruct number_of_shards: 1, routing_fields: [], routing_partition_size: nil
  end

  defmodule IndexSchema do
    @moduledoc false
    defstruct field_schemas: [], index_setting: %IndexSetting{}, index_sorts: []
  end

  defmodule CreateSearchIndexRequest do
    @moduledoc false
    defstruct table_name: "", index_name: "", index_schema: %IndexSchema{}
  end

  defmodule DeleteSearchIndexRequest do
    @moduledoc false
    defstruct table_name: "", index_name: ""
  end

  defmodule DescribeSearchIndexRequest do
    @moduledoc false
    defstruct table_name: "", index_name: ""
  end

  defmodule FieldSchema do
    @moduledoc false
    require ExAliyunOts.Const.Search.FieldType, as: FieldType

    defstruct field_name: "",
              field_type: FieldType.keyword(),
              index_options: nil,
              analyzer: nil,
              index: true,
              enable_sort_and_agg: true,
              store: true,
              field_schemas: [],
              is_array: nil
  end

  defmodule FieldSort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct field_name: "", order: SortOrder.asc(), mode: nil, nested_filter: nil
  end

  defmodule GeoDistanceSort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    require ExAliyunOts.Const.Search.GeoDistanceType, as: GeoDistanceType

    defstruct field_name: "",
              points: [],
              order: SortOrder.asc(),
              mode: nil,
              distance_type: GeoDistanceType.arc(),
              nested_filter: nil
  end

  defmodule NestedFilter do
    @moduledoc false
    defstruct path: nil, filter: nil
  end

  defmodule ScoreSort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct order: SortOrder.asc()
  end

  defmodule PrimaryKeySort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct order: SortOrder.asc()
  end

  defmodule ColumnsToGet do
    @moduledoc false
    require ExAliyunOts.Const.Search.ColumnReturnType, as: ColumnReturnType
    defstruct return_type: ColumnReturnType.all(), column_names: []
  end

  defmodule SearchQuery do
    @moduledoc false
    defstruct offset: 0,
              limit: nil,
              query: nil,
              collapse: nil,
              sort: nil,
              get_total_count: true,
              token: nil,
              aggs: nil,
              group_bys: nil
  end

  defmodule SearchRequest do
    @moduledoc false
    defstruct table_name: "",
              index_name: "",
              columns_to_get: %ColumnsToGet{},
              search_query: %SearchQuery{},
              routing_values: nil
  end

  defmodule MatchQuery do
    @moduledoc false
    defstruct field_name: "", text: "", minimum_should_match: 1, operator: nil
  end

  defmodule MatchAllQuery do
    @moduledoc false
    defstruct []
  end

  defmodule MatchPhraseQuery do
    @moduledoc false
    defstruct field_name: "", text: ""
  end

  defmodule TermQuery do
    @moduledoc false
    defstruct field_name: "", term: nil
  end

  defmodule TermsQuery do
    @moduledoc false
    defstruct field_name: "", terms: []
  end

  defmodule PrefixQuery do
    @moduledoc false
    defstruct field_name: "", prefix: nil
  end

  defmodule RangeQuery do
    @moduledoc false
    defstruct field_name: "", from: nil, to: nil, include_lower: true, include_upper: true
  end

  defmodule WildcardQuery do
    @moduledoc false
    defstruct field_name: "", value: nil
  end

  defmodule BoolQuery do
    @moduledoc false
    defstruct must: [], must_not: [], filter: [], should: [], minimum_should_match: nil
  end

  defmodule NestedQuery do
    @moduledoc false
    require ExAliyunOts.Const.Search.ScoreMode, as: ScoreMode

    # `score_mode`:
    # 多值字段获取文档得分的模式，一个字段多个值的情况下，采用哪个值来进行排序
    # 例如：有一个小学生学生状态监测系统，其中存了小学生的身高，但是小学生身高一直在长，所以“身高”这个字段，采用了array的方式。然后我们查询的时候，想根据身高进行排序，就可以设置`score_mode`为`max`，这样就能得到最近的一次身高。
    defstruct path: "", query: nil, score_mode: ScoreMode.none()
  end

  defmodule GeoDistanceQuery do
    @moduledoc false
    defstruct field_name: "", center_point: nil, distance: nil
  end

  defmodule GeoBoundingBoxQuery do
    @moduledoc false
    defstruct field_name: "", top_left: nil, bottom_right: nil
  end

  defmodule GeoPolygonQuery do
    @moduledoc false
    defstruct field_name: "", points: []
  end

  defmodule ExistsQuery do
    @moduledoc false
    defstruct field_name: ""
  end

  defmodule Aggregation do
    @moduledoc false
    defstruct field_name: "", type: nil, name: nil, missing: nil
  end

  defmodule GroupByField do
    @moduledoc false
    defstruct name: nil, field_name: "", size: nil, sub_group_bys: nil, sub_aggs: nil, sort: nil
  end

  defmodule GroupByRange do
    @moduledoc false
    defstruct name: nil, field_name: "", sub_group_bys: nil, sub_aggs: nil, ranges: nil
  end

  defmodule GroupByFilter do
    @moduledoc false
    defstruct name: nil, filters: nil, sub_group_bys: nil, sub_aggs: nil
  end

  defmodule GroupByGeoDistance do
    @moduledoc false
    defstruct name: nil,
              field_name: "",
              lat: nil,
              lon: nil,
              sub_group_bys: nil,
              sub_aggs: nil,
              ranges: nil
  end

  defmodule GroupKeySort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct order: SortOrder.asc()
  end

  defmodule RowCountSort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct order: SortOrder.asc()
  end

  defmodule SubAggSort do
    @moduledoc false
    require ExAliyunOts.Const.Search.SortOrder, as: SortOrder
    defstruct order: SortOrder.desc(), sub_agg_name: nil
  end
end

# Transaction

defmodule ExAliyunOts.Var.Transaction do
  @moduledoc false

  defmodule StartLocalTransactionRequest do
    @moduledoc false
    defstruct table_name: "", partition_key: {}
  end
end
