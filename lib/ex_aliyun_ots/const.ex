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
