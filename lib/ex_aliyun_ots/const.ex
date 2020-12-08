defmodule ExAliyunOts.Constants do
  @moduledoc false

  defmacro __using__(_opts \\ []) do
    quote do
      alias ExAliyunOts.Const.{
        OperationType,
        PKType,
        ReturnType,
        Direction,
        RowExistence,
        FilterType,
        LogicOperator,
        ComparatorType,
        Search.FieldType,
        Search.QueryType,
        Search.QueryOperator,
        Search.SortOrder,
        Search.ScoreMode,
        Search.GeoDistanceType,
        Search.ColumnReturnType,
        Search.IndexOptions,
        Search.SyncPhase,
        Search.SortType
      }

      # Common
      require OperationType
      require PKType
      require ReturnType
      require Direction
      require RowExistence
      require FilterType
      require LogicOperator
      require ComparatorType

      # SearchIndex
      require FieldType
      require QueryType
      require QueryOperator
      require SortOrder
      require ScoreMode
      require GeoDistanceType
      require ColumnReturnType
      require IndexOptions
      require SyncPhase
      require SortType
    end
  end

  defmacro const(name, value) do
    quote do
      defmacro unquote(name)(), do: unquote(value)
    end
  end
end

defmodule ExAliyunOts.Const.ErrorType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:ots_client_unknown, "OTSClientUnknownError")

  #
  # Will retry when occur the following errors:
  #
  # reference: https://help.aliyun.com/document_detail/27300.html
  #
  const(:row_operation_conflict, "OTSRowOperationConflict")
  const(:not_enough_capacity_unit, "OTSNotEnoughCapacityUnit")
  const(:table_not_ready, "OTSTableNotReady")
  const(:partition_unavailable, "OTSPartitionUnavailable")
  const(:server_busy, "OTSServerBusy")
  const(:storage_server_busy, "OTSStorageServerBusy")
  const(:quota_exhausted, "OTSQuotaExhausted")
  const(:storage_timeout, "OTSTimeout")
  const(:server_unavailable, "OTSServerUnavailable")
  const(:internal_server_error, "OTSInternalServerError")

  const(
    :too_frequent_reserved_throughput_adjustment,
    "OTSTooFrequentReservedThroughputAdjustment"
  )
end

defmodule ExAliyunOts.Const.OperationType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:update, :UPDATE)
  const(:put, :PUT)
  const(:delete, :DELETE)
  const(:delete_all, :DELETE_ALL)
  const(:increment, :INCREMENT)
  const(:updates_supported, [:PUT, :DELETE, :DELETE_ALL, :INCREMENT])
end

defmodule ExAliyunOts.Const.PKType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:integer, :INTEGER)
  const(:string, :STRING)
  const(:binary, :BINARY)
  const(:auto_increment, :AUTO_INCREMENT)

  const(:inf_min, :INF_MIN)
  const(:inf_max, :INF_MAX)
end

defmodule ExAliyunOts.Const.ReturnType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:pk, :RT_PK)
  const(:none, :RT_NONE)
  const(:after_modify, :RT_AFTER_MODIFY)
end

defmodule ExAliyunOts.Const.Direction do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:forward, :FORWARD)
  const(:backward, :BACKWARD)
end

defmodule ExAliyunOts.Const.RowExistence do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:ignore, :IGNORE)
  const(:expect_exist, :EXPECT_EXIST)
  const(:expect_not_exist, :EXPECT_NOT_EXIST)
  const(:supported, [:IGNORE, :EXPECT_EXIST, :EXPECT_NOT_EXIST])

  def mapping,
    do: %{
      ignore: :IGNORE,
      expect_exist: :EXPECT_EXIST,
      expect_not_exist: :EXPECT_NOT_EXIST
    }
end

defmodule ExAliyunOts.Const.FilterType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:single_column, :FT_SINGLE_COLUMN_VALUE)
  const(:composite_column, :FT_COMPOSITE_COLUMN_VALUE)
  const(:column_pagination, :FT_COLUMN_PAGINATION)
end

defmodule ExAliyunOts.Const.LogicOperator do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:not, :LO_NOT)
  const(:and, :LO_AND)
  const(:or, :LO_OR)
  def mapping, do: %{not: :LO_NOT, and: :LO_AND, or: :LO_OR}
end

defmodule ExAliyunOts.Const.ComparatorType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:equal, :CT_EQUAL)
  const(:not_equal, :CT_NOT_EQUAL)
  const(:greater_than, :CT_GREATER_THAN)
  const(:greater_equal, :CT_GREATER_EQUAL)
  const(:less_than, :CT_LESS_THAN)
  const(:less_equal, :CT_LESS_EQUAL)
end

defmodule ExAliyunOts.Const.Search.FieldType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:long, :LONG)
  const(:double, :DOUBLE)
  const(:boolean, :BOOLEAN)
  const(:keyword, :KEYWORD)
  const(:text, :TEXT)
  const(:nested, :NESTED)
  const(:geo_point, :GEO_POINT)
end

defmodule ExAliyunOts.Const.Search.QueryType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:match, :MATCH_QUERY)
  const(:match_all, :MATCH_ALL_QUERY)
  const(:match_phrase, :MATCH_PHRASE_QUERY)
  const(:term, :TERM_QUERY)
  const(:terms, :TERMS_QUERY)
  const(:range, :RANGE_QUERY)
  const(:prefix, :PREFIX_QUERY)
  const(:wildcard, :WILDCARD_QUERY)
  const(:bool, :BOOL_QUERY)
  const(:const_score, :CONST_SCORE_QUERY)
  const(:func_score, :FUNCTION_SCORE_QUERY)
  const(:nested, :NESTED_QUERY)
  const(:geo_distance, :GEO_DISTANCE_QUERY)
  const(:geo_bounding_box, :GEO_BOUNDING_BOX_QUERY)
  const(:geo_polygon, :GEO_POLYGON_QUERY)
  const(:exists, :EXISTS_QUERY)
end

defmodule ExAliyunOts.Const.Search.QueryOperator do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:or, :OR)
  const(:and, :AND)
end

defmodule ExAliyunOts.Const.Search.ScoreMode do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:none, :SCORE_MODE_NONE)
  const(:avg, :SCORE_MODE_AVG)
  const(:max, :SCORE_MODE_MAX)
  const(:total, :SCORE_MODE_TOTAL)
  const(:min, :SCORE_MODE_MIN)
end

defmodule ExAliyunOts.Const.Search.SortOrder do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:asc, :SORT_ORDER_ASC)
  const(:desc, :SORT_ORDER_DESC)
end

defmodule ExAliyunOts.Const.Search.SortMode do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:min, :SORT_MODE_MIN)
  const(:max, :SORT_MODE_MAX)
  const(:avg, :SORT_MODE_AVG)
end

defmodule ExAliyunOts.Const.Search.GeoDistanceType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:arc, :GEO_DISTANCE_ARC)
  const(:plane, :GEO_DISTANCE_PLANE)
end

defmodule ExAliyunOts.Const.Search.ColumnReturnType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:all, :RETURN_ALL)
  const(:specified, :RETURN_SPECIFIED)
  const(:none, :RETURN_NONE)
  const(:all_from_index, :RETURN_ALL_FROM_INDEX)
end

defmodule ExAliyunOts.Const.Search.IndexOptions do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:docs, :DOCS)
  const(:freqs, :FREQS)
  const(:positions, :POSITIONS)
  const(:offsets, :OFFSETS)
end

defmodule ExAliyunOts.Const.Search.SyncPhase do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:full, :FULL)
  const(:incr, :INCR)
end

defmodule ExAliyunOts.Const.Search.SortType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:field, :field)
  const(:geo_distance, :geo_distance)
  const(:score, :score)
  const(:pk, :pk)
end

defmodule ExAliyunOts.Const.Search.AggregationType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:avg, :AGG_AVG)
  const(:distinct_count, :AGG_DISTINCT_COUNT)
  const(:max, :AGG_MAX)
  const(:min, :AGG_MIN)
  const(:sum, :AGG_SUM)
  const(:count, :AGG_COUNT)
end

defmodule ExAliyunOts.Const.Search.GroupByType do
  @moduledoc false
  import ExAliyunOts.Constants

  const(:field, :GROUP_BY_FIELD)
  const(:range, :GROUP_BY_RANGE)
  const(:filter, :GROUP_BY_FILTER)
  const(:geo_distance, :GROUP_BY_GEO_DISTANCE)
end
