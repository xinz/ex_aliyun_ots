defmodule ExAliyunOts.Constants do

  defmacro const(name, value) do
    quote do
      defmacro unquote(name)(), do: unquote(value)
    end
  end

end

defmodule ExAliyunOts.Const.OperationType do
  import ExAliyunOts.Constants

  const :update, :'UPDATE'
  const :put, :'PUT'
  const :delete, :'DELETE'
  const :delete_all, :'DELETE_ALL'
  const :updates_supported, [:'PUT', :'DELETE', :'DELETE_ALL']
end

defmodule ExAliyunOts.Const.PKType do
  import ExAliyunOts.Constants

  const :integer, :'INTEGER'
  const :string, :'STRING'
  const :binary, :'BINARY'
  const :auto_increment, :'AUTO_INCREMENT'

  const :inf_min, :'INF_MIN'
  const :inf_max, :'INF_MAX'
end

defmodule ExAliyunOts.Const.ReturnType do
  import ExAliyunOts.Constants

  const :pk, :'RT_PK'
  const :none, :'RT_NONE'
end

defmodule ExAliyunOts.Const.Direction do
  import ExAliyunOts.Constants

  const :forward, :'FORWARD'
  const :backward, :'BACKWARD'
end

defmodule ExAliyunOts.Const.RowExistence do
  import ExAliyunOts.Constants

  const :ignore, :'IGNORE'
  const :expect_exist, :'EXPECT_EXIST'
  const :expect_not_exist, :'EXPECT_NOT_EXIST'
  const :supported, [:'IGNORE', :'EXPECT_EXIST', :'EXPECT_NOT_EXIST']
end

defmodule ExAliyunOts.Const.FilterType do
  import ExAliyunOts.Constants

  const :single_column, :'FT_SINGLE_COLUMN_VALUE'
  const :composite_column, :'FT_COMPOSITE_COLUMN_VALUE'
  const :column_pagination, :'FT_COLUMN_PAGINATION'
end

defmodule ExAliyunOts.Const.LogicOperator do
  import ExAliyunOts.Constants

  const :not, :'LO_NOT'
  const :and, :'LO_AND'
  const :or, :'LO_OR'
end

defmodule ExAliyunOts.Const.ComparatorType do
  import ExAliyunOts.Constants

  const :eq, :'CT_EQUAL'
  const :not_eq, :'CT_NOT_EQUAL'
  const :gt, :'CT_GREATER_THAN'
  const :ge, :'CT_GREATER_EQUAL'
  const :lt, :'CT_LESS_THAN'
  const :le, :'CT_LESS_EQUAL'
end

defmodule ExAliyunOts.Const.Search.FieldType do
  import ExAliyunOts.Constants

  const :long, :'LONG'
  const :double, :'DOUBLE'
  const :boolean, :'BOOLEAN'
  const :keyword, :'KEYWORD'
  const :text, :'TEXT'
  const :nested, :'NESTED'
  const :geo_point, :'GEO_POINT'
end

defmodule ExAliyunOts.Const.Search.QueryType do
  import ExAliyunOts.Constants

  const :match, :'MATCH_QUERY'
  const :match_all, :'MATCH_ALL_QUERY'
  const :match_phrase, :'MATCH_PHRASE_QUERY'
  const :term, :'TERM_QUERY'
  const :terms, :'TERMS_QUERY'
  const :range, :'RANGE_QUERY'
  const :prefix, :'PREFIX_QUERY'
  const :bool, :'BOOL_QUERY'
  const :const_score, :'CONST_SCORE_QUERY'
  const :func_score, :'FUNCTION_SCORE_QUERY'
  const :nested, :'NESTED_QUERY'
  const :geo_bounding_box, :'GEO_BOUNDING_BOX_QUERY'
  const :geo_distance, :'GEO_DISTANCE_QUERY'
  const :geo_polygon, :'GEO_POLYGON_QUERY'
end

defmodule ExAliyunOts.Const.Search.QueryOperator do
  import ExAliyunOts.Constants

  const :or, :'OR'
  const :and, :'AND'
end

defmodule ExAliyunOts.Const.Search.ScoreMode do
  import ExAliyunOts.Constants

  const :none, :'SCORE_MODE_NONE'
  const :avg, :'SCORE_MODE_AVG'
  const :max, :'SCORE_MODE_MAX'
  const :total, :'SCORE_MODE_TOTAL'
  const :min, :'SCORE_MODE_MIN'
end

defmodule ExAliyunOts.Const.Search.SortOrder do
  import ExAliyunOts.Constants

  const :asc, :'SORT_ORDER_ASC'
  const :desc, :'SORT_ORDER_DESC'
end

defmodule ExAliyunOts.Const.Search.SortMode do
  import ExAliyunOts.Constants

  const :min, :'SORT_MODE_MIN'
  const :max, :'SORT_MODE_MAX'
  const :avg, :'SORT_MODE_AVG'
end

defmodule ExAliyunOts.Const.Search.GeoDistanceType do
  import ExAliyunOts.Constants

  const :arc, :'GEO_DISTANCE_ARC'
  const :plane, :'GEO_DISTANCE_PLANE'
end

defmodule ExAliyunOts.Const.Search.ColumnReturnType do
  import ExAliyunOts.Constants

  const :all, :'RETURN_ALL'
  const :specified, :'RETURN_SPECIFIED'
  const :none, :'RETURN_NONE'
end

defmodule ExAliyunOts.Const.Search.IndexOptions do
  import ExAliyunOts.Constants

  const :docs, :'DOCS'
  const :freqs, :'FREQS'
  const :positions, :'POSITIONS'
  const :offsets, :'OFFSETS'
end

defmodule ExAliyunOts.Const.Search.SyncPhase do
  import ExAliyunOts.Constants

  const :full, :'FULL'
  const :incr, :'INCR'
end

defmodule ExAliyunOts.Const.Search.SortType do
  import ExAliyunOts.Constants

  const :field, :field
  const :geo_distance, :geo_distance
  const :score, :score
  const :pk, :pk
end
