defmodule ExAliyunOts do
  @moduledoc ~S"""
  The `ExAliyunOts` module provides a tablestore-based API as a client for working with Alibaba TableStore product servers.

  Here are links to official documents in [Chinese](https://help.aliyun.com/document_detail/27280.html) | [English](https://www.alibabacloud.com/help/product/27278.html)

  ## Configuration

      config :ex_aliyun_ots, :my_instance
        name: "MyInstanceName",
        endpoint: "MyInstanceEndpoint",
        access_key_id: "MyAliyunRAMKeyID",
        access_key_secret: "MyAliyunRAMKeySecret"

      config :ex_aliyun_ots,
        instances: [:my_instance],
        debug: false,
        enable_tunnel: false

  * `debug`, optional, specifies whether to enable debug logger, by default it's false, and please DO NOT use debug mode in production.
  * `enable_tunnel`, optional, specifies whether to enable tunnel functions, there will startup tunnel related `Supervisor` and `Registry` when enable it, by default it's false.

  ## Using ExAliyunOts

  To use `ExAliyunOts`, a module that calls `use ExAliyunOts` has to be defined:

      defmodule MyApp.TableStore do
        use ExAliyunOts, instance: :my_instance
      end

  This automatically defines some macros and functions in the `MyApp.TableStore` module, here are some examples:

      import MyApp.TableStore

      # Create table
      create_table "table",
        [{"pk1", :integer}, {"pk2", :string}]

      # Put row
      put_row "table",
        [{"pk1", "id1"}],
        [{"attr1", 10}, {"attr2", "attr2_value"}],
        condition: condition(:expect_not_exist),
        return_type: :pk

      # Search index
      search "table", "index_name",
        search_query: [
          query: match_query("age", 28),
          sort: [
            field_sort("age", order: :desc)
          ]
        ]

      # Local transaction
      start_local_transaction "table", {"partition_key", "partition_value"}

  ## ExAliyunOts API

  There are two ways to use ExAliyunOts:

    * using macros and functions from your own ExAliyunOts module, like `MyApp.TableStore`.
    * using macros and functions from the `ExAliyunOts` module.

  All defined functions and macros in `ExAliyunOts` are available and referable for your own ExAliyunOts module as well, except that the given arity of functions may
  different, because the `instance` parameter of each invoke request is NOT needed from your own ExAliyunOts module although the `ExAliyunOts` module defines it.
  """
  require ExAliyunOts.Const.OperationType, as: OperationType
  alias ExAliyunOts.{Var, Client, Utils}
  alias ExAliyunOts.TableStore.{ReturnType, Direction}

  @before_compile ExAliyunOts.MergeCompiler
  @type instance :: atom
  @type table_name :: String.t()
  @type primary_keys :: list
  @type inclusive_start_primary_keys :: list
  @type exclusive_end_primary_keys :: list
  @type index_name :: String.t()
  @type options :: Keyword.t()
  @type result :: {:ok, map()} | {:error, ExAliyunOts.Error.t()}

  require Logger

  defmacro __using__(opts \\ []) do
    opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))

    quote do
      @instance Keyword.get(unquote(opts), :instance)
      use ExAliyunOts.Constants
      import ExAliyunOts.DSL
      @before_compile ExAliyunOts.Compiler
    end
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27312.html) | [English](https://www.alibabacloud.com/help/doc-detail/27312.html)

  ## Example

      create_table "table_name2",
        [{"key1", :string}, {"key2", :auto_increment}]

      create_table "table_name3",
        [{"key1", :string}],
        reserved_throughput_write: 1,
        reserved_throughput_read: 1,
        time_to_live: 100_000,
        max_versions: 3,
        deviation_cell_version_in_sec: 6_400,
        stream_spec: [is_enabled: true, expiration_time: 2]

      create_table "table_name",
        [{"key1", :string}],
        defined_columns: [
          {"attr1", :string},
          {"attr2", :integer},
          {"attr3", :boolean},
          {"attr4", :double},
          {"attr5", :binary}
        ]

      create_table "table_name",
        [{"key1", :string}],
        index_metas: [
          {"indexname1", ["key1"], ["attr1", "attr2"]},
          {"indexname2", ["key1"], ["attr4"]}
        ]

  ## Options

    * `:reserved_throughput_write`, optional, the reserved throughput write of table, by default it is 0.
    * `:reserved_throughput_read`, optional, the reserved throughput read of table, by default it is 0.
    * `time_to_live`, optional, the data storage time to live in seconds, the minimum settable value is 864_000 seconds (one day), by default it is -1 (for permanent).
    * `:max_versions`, optional, the version of table, by default it is 1 that specifies there is only one version for columns.
    * `:deviation_cell_version_in_sec`, optional, maximum version deviation, by default it is 864_000 seconds (one day).
    * `:stream_spec`, specifies whether enable stream, by default it is not enable stream feature.
      - `:is_enabled`, enable or not enable stream, use `true` or `false`;
      - `:expiration_time`, the expiration time of stream.
    * `:index_metas`, optional, the index meta of table, each item of `:index_metas` is in {String.t(), list(), list()} format, by default it is [].
    * `:defined_columns`, optional, the indexed attribute column, which is a combination of predefined columns of the base table, each item of `:defined_columns`
    is in {String.t(), :integer | :double | :boolean | :string | :binary} format, by default it is [].
  """
  @doc table: :table
  @spec create_table(instance, table_name, primary_keys, options) ::
          :ok | {:error, ExAliyunOts.Error.t()}
  def create_table(instance, table_name, primary_keys, options \\ []) do
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: primary_keys
    }

    prepared_var = map_options(var_create_table, options)
    Client.create_table(instance, prepared_var)
  end

  @doc """
  Create global secondary indexes. Official document in [Chinese](https://help.aliyun.com/document_detail/91947.html) | [English](https://www.alibabacloud.com/help/doc-detail/91947.html)

  ## Example

      create_index "table_name",
        "table_index_name1"
        ["pk1", "pk2", "col0"],
        ["col1", "col2"]

      create_index "table_name",
        "table_index_name2"
        ["col0", "pk1"],
        ["col1", "col2", "col3"],
        include_base_data: false

  ## Options

    * `:index_update_mode`, the update mode of the index table, optional, currently only support `:IUM_ASYNC_INDEX`,
    by default it is `:IUM_ASYNC_INDEX`;
    * `:index_type`, the type of the index table, optional, currently only support `:IT_GLOBAL_INDEX`,
    by default it is `:IT_GLOBAL_INDEX`;
    * `:include_base_data`, specifies whether the index table includes the existing data in the base table, if set it to
    `true` means the index includes the existing data, if set it to `false` means the index excludes the existing data,
    optional, by default it is `true`.
  """
  @doc table: :table
  @spec create_index(
          instance,
          table_name,
          index_name,
          primary_keys :: [String.t()],
          defined_columns :: [String.t()],
          options
        ) :: :ok | {:error, ExAliyunOts.Error.t()}
  def create_index(
        instance,
        table_name,
        index_name,
        primary_keys,
        defined_columns,
        options \\ []
      ) do
    Client.create_index(
      instance,
      table_name,
      index_name,
      primary_keys,
      defined_columns,
      options
    )
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/94558.html) | [English](https://www.alibabacloud.com/help/doc-detail/94558.html)

  ## Example

      import MyApp.TableStore

      delete_index("table_name", "index_name")
  """
  @doc table: :table
  @spec delete_index(instance, table_name, index_name) :: :ok | {:error, ExAliyunOts.Error.t()}
  defdelegate delete_index(instance, table_name, index_name), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27314.html) | [English](https://www.alibabacloud.com/help/doc-detail/27314.html)

  ## Example

      import MyApp.TableStore

      delete_table("table_name")
  """
  @doc table: :table
  @spec delete_table(instance, table_name) :: :ok | {:error, ExAliyunOts.Error.t()}
  defdelegate delete_table(instance, table_name), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27313.html) | [English](https://www.alibabacloud.com/help/doc-detail/27313.html)

  ## Example

      import MyApp.TableStore

      list_table()
  """
  @doc table: :table
  @spec list_table(instance) :: result
  defdelegate list_table(instance), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27315.html) | [English](https://www.alibabacloud.com/help/doc-detail/27315.html)

  ## Example

      import MyApp.TableStore

      update_table "table_name",
        reserved_throughput_write: 10,
        time_to_live: 200_000,
        stream_spec: [is_enabled: false]

  ## Options

    Please see options of `create_table/4`.
  """
  @doc table: :table
  @spec update_table(instance, table_name, options) :: result
  def update_table(instance, table_name, options \\ []) do
    var_update_table = %Var.UpdateTable{
      table_name: table_name
    }

    prepared_var = map_options(var_update_table, options)
    Client.update_table(instance, prepared_var)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27307.html) | [English](https://www.alibabacloud.com/help/doc-detail/27307.html)

  ## Example

      import MyApp.TableStore

      describe_table(table_name)
  """
  @doc table: :table
  @spec describe_table(instance, table_name) :: result
  defdelegate describe_table(instance, table_name), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/53813.html) | [English](https://www.alibabacloud.com/help/doc-detail/53813.html)
  """
  @doc table: :table
  @spec compute_split_points_by_size(instance, table_name, splits_size :: integer()) ::
          result
  defdelegate compute_split_points_by_size(instance, table_name, splits_size), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27310.html) | [English](https://www.alibabacloud.com/help/doc-detail/27310.html)

  ## Example

      import MyApp.TableStore

      batch_get [
        get(table_name1, [[{"key1", 1}, {"key2", "1"}]]),
        get(
          table_name2,
          [{"key1", "key1"}],
          columns_to_get: ["name", "age"],
          filter: filter "age" >= 10
        )
      ]

  The batch get operation can be considered as a collection of mulitple `get/3` operations.
  """
  @doc row: :row
  @spec batch_get(instance, requests :: list()) :: result
  defdelegate batch_get(instance, requests), to: Client, as: :batch_get_row

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27311.html) | [English](https://www.alibabacloud.com/help/doc-detail/27311.html)

  ## Example

      import MyApp.TableStore

      batch_write [
        {"table1", [
          write_delete([{"key1", 5}, {"key2", "5"}],
            return_type: :pk,
            condition: condition(:expect_exist, "attr1" == 5)),
          write_put([{"key1", 6}, {"key2", "6"}],
            [{"new_put_val1", "val1"}, {"new_put_val2", "val2"}],
            condition: condition(:expect_not_exist),
            return_type: :pk)
        ]},
        {"table2", [
          write_update([{"key1", "new_tab3_id2"}],
            put: [{"new_put1", "u1"}, {"new_put2", 2.5}],
            condition: condition(:expect_not_exist)),
          write_put([{"key1", "new_tab3_id3"}],
            [{"new_put1", "put1"}, {"new_put2", 10}],
            condition: condition(:expect_not_exist))
        ]}
      ]

  The batch write operation can be considered as a collection of multiple `write_put/3`, `write_update/2` and `write_delete/2` operations.
  """
  @doc row: :row
  @spec batch_write(instance, requests :: list(), options) :: result
  def batch_write(instance, requests, options \\ [])

  def batch_write(instance, requests, options) when is_list(requests) do
    batch_write_requests =
      Enum.map(requests, fn {table_name, write_rows} ->
        %Var.BatchWriteRequest{
          table_name: table_name,
          rows: write_rows
        }
      end)

    Client.batch_write_row(instance, batch_write_requests, options)
  end

  def batch_write(instance, {table_name, write_rows}, options) do
    batch_write_request = %Var.BatchWriteRequest{
      table_name: table_name,
      rows: write_rows
    }

    Client.batch_write_row(instance, batch_write_request, options)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27305.html) | [English](https://www.alibabacloud.com/help/doc-detail/27305.html)

  ## Example

      import MyApp.TableStore

      get_row "table1",
        [{"key1", "id1"}, {"key2", "id2"}],
        columns_to_get: ["name", "level"],
        filter: filter(("name[ignore_if_missing: true, latest_version_only: true]" == var_name and "age" > 1) or ("class" == "1"))

      get_row "table2",
        [{"key", "1"}],
        start_column: "room",
        filter: pagination(offset: 0, limit: 3)

      get_row "table3",
        [{"key", "1"}],
        transaction_id: "transaction_id"

  ## Options

    * `:columns_to_get`, optional, fetch the special fields, by default it returns all fields, pass a field list to specify the expected return fields
    e.g. `["field1", "field2"]`.
    * `:start_column`, optional, specifies the start column when using for wide-row-read, the returned result contains this `:start_column`.
    * `:end_column`, optional, specifies the end column when using for wide-row-read, the returned result does not contain this `:end_column`.
    * `:filter`, optional, filter the return results in the server side, please see `filter/1` for details.
    * `:max_versions`, optional, how many versions need to return in results, by default it is 1.
    * `:time_range`, optional, read data by timestamp range, support two ways to use it:
      - `time_range: {start_timestamp, end_timestamp}`, the timestamp in the range (include `start_timestamp` but exclude `end_timestamp`)
      and then will return in the results.
      - `time_range: special_timestamp`, exactly match and then will return in the results.
      - `:time_range` and `:max_versions` are mutually exclusive, by default use `max_versions: 1` and `time_range: nil`.
    * `:transaction_id`, optional, read operation within local transaction.
  """
  @doc row: :row
  @spec get_row(instance, table_name, primary_keys, options) :: result
  def get_row(instance, table_name, primary_keys, options \\ []) do
    prepared_var = get(table_name, primary_keys, options)
    Client.get_row(instance, prepared_var)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27306.html) | [English](https://www.alibabacloud.com/help/doc-detail/27306.html)

  ## Example

      import MyApp.TableStore

      put_row "table1",
        [{"key1", "id1"}],
        [{"name", "name1"}, {"age", 20}],
        condition: condition(:expect_not_exist),
        return_type: :pk

      put_row "table2",
        [{"key1", "id1"}],
        [{"name", "name1"}, {"age", 20}],
        condition: condition(:expect_not_exist),
        transaction_id: "transaction_id"
        return_type: :pk

  ## Options

    * `:condition`, required, please see `condition/1` or `condition/2` for details.
    * `:return_type`, optional, whether return the primary keys after put row, available options are `:pk` | `:none`, by default it is `:none`.
    * `:transaction_id`, optional, write operation within local transaction.

  """
  @doc row: :row
  @spec put_row(instance, table_name, primary_keys, options) :: result
  def put_row(instance, table_name, primary_keys, attrs, options \\ []) do
    prepared_var =
      %Var.PutRow{
        table_name: table_name,
        primary_keys: primary_keys,
        attribute_columns: attrs
      }
      |> map_options(options)

    Client.put_row(instance, prepared_var)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27307.html) | [English](https://www.alibabacloud.com/help/doc-detail/27307.html)

  ## Example

      import MyApp.TableStore

      value = "1"
      update_row "table1",
        [{"key1", 2}, {"key2", "2"}],
        delete: [{"attr2", nil, 1524464460}],
        delete_all: ["attr1"],
        put: [{"attr3", "put_attr3"}],
        return_type: :pk,
        condition: condition(:expect_exist, "attr2" == value)

      update_row "table2",
        [{"key1", 1}],
        put: [{"attr1", "put_attr1"}],
        increment: [{"count", 1}],
        return_type: :after_modify,
        return_columns: ["count"],
        condition: condition(:ignore)

      update_row "table3",
        [partition_key],
        put: [{"new_attr1", "a1"}],
        delete_all: ["level", "size"],
        condition: condition(:ignore),
        transaction_id: "transaction_id"

  ## Options

    * `:put`, optional, require to be valid value, e.g. `[{"field1", "value"}, {...}]`, insert a new column if this field is not existed, or overwrite this field if existed.
    * `:delete`, optional, delete the special version of a column or columns, please pass the column's version (timestamp) in `:delete` option, e.g. [{"field1", nil, 1524464460}, ...].
    * `:delete_all`, optional, delete all versions of a column or columns, e.g. ["field1", "field2", ...].
    * `:increment`, optional, attribute column(s) base on atomic counters for increment or decrement, require the value of column is integer.
      - for increment, `increment: [{"count", 1}]`;
      - for decrement, `increment: [{"count", -1}]`.
    * `:return_type`, optional, whether return the primary keys after update row, available options are `:pk` | `:none` | `:after_modify`, by default it is `:none`.
      - if use atomic counters, must set `return_type: :after_modify`.
    * `:condition`, required, please see `condition/1` or `condition/2` for details.
    * `:transaction_id`, optional, write operation within local transaction.
  """
  @doc row: :row
  @spec update_row(instance, table_name, primary_keys, options) :: result
  def update_row(instance, table_name, primary_keys, options \\ []) do
    prepared_var =
      %Var.UpdateRow{
        table_name: table_name,
        primary_keys: primary_keys
      }
      |> map_options(options)
      |> Map.put(:updates, map_updates(options))

    Client.update_row(instance, prepared_var)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27308.html) | [English](https://www.alibabacloud.com/help/doc-detail/27308.html)

  ## Example

      import MyApp.TableStore

      delete_row "table1",
        [{"key1", 3}, {"key2", "3"}],
        condition: condition(:expect_exist, "attr2" == "value2")

      delete_row "table1",
        [{"key1", 3}, {"key2", "3"}],
        condition: condition(:expect_exist, "attr2" == "value2"),
        transaction_id: "transaction_id"

  ## Options

    * `:condition`, required, please see `condition/1` or `condition/2` for details.
    * `:transaction_id`, optional, write operation within local transaction.
  """
  @doc row: :row
  @spec delete_row(instance, table_name, primary_keys, options) :: result
  def delete_row(instance, table_name, primary_keys, options \\ []) do
    prepared_var =
      %Var.DeleteRow{
        table_name: table_name,
        primary_keys: primary_keys
      }
      |> map_options(options)

    Client.delete_row(instance, prepared_var)
  end

  @doc """
  Used in batch get operation, please see `batch_get/2` for details.

  ## Options

  The available options are same as `get_row/4`.
  """
  @doc row: :row
  @spec get(table_name, primary_keys, options) :: map()
  def get(table_name, primary_keys, options \\ []) do
    %Var.GetRow{table_name: table_name, primary_keys: primary_keys}
    |> map_options(options)
  end

  @doc """
  Used in batch write operation, please see `batch_write/2` for details.

  ## Options

  The available options are same as `put_row/5`.
  """
  @doc row: :row
  @spec write_put(primary_keys, attrs :: list(), options) :: map()
  def write_put(primary_keys, attrs, options \\ []) do
    %Var.RowInBatchWriteRequest{
      type: :PUT,
      primary_keys: primary_keys,
      updates: attrs
    }
    |> map_options(options)
  end

  @doc """
  Used in batch write operation, please see `batch_write/2` for details.

  ## Options

  The available options are same as `update_row/4`.
  """
  @doc row: :row
  @spec write_update(primary_keys, options) :: map()
  def write_update(primary_keys, options \\ []) do
    %Var.RowInBatchWriteRequest{
      type: :UPDATE,
      primary_keys: primary_keys,
      updates: map_updates(options)
    }
    |> map_options(options)
  end

  @doc """
  Used in batch write operation, please see `batch_write/2` for details.

  ## Options

  The available operation same as `delete_row/4`.
  """
  @doc row: :row
  @spec write_delete(primary_keys, options) :: map()
  def write_delete(primary_keys, options \\ []) do
    %Var.RowInBatchWriteRequest{type: :DELETE, primary_keys: primary_keys}
    |> map_options(options)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/27309.html) | [English](https://www.alibabacloud.com/help/doc-detail/27309.html)

  ## Example

      import MyApp.TableStore

      get_range "table_name",
        [{"key1", 1}, {"key2", :inf_min}],
        [{"key1", 4}, {"key2", :inf_max}],
        direction: :forward

      get_range "table_name",
        [{"key1", 1}, {"key2", :inf_min}],
        [{"key1", 4}, {"key2", :inf_max}],
        time_range: {1525922253224, 1525923253224},
        direction: :forward

      get_range "table_name",
        [{"key1", 1}, {"key2", :inf_min}],
        [{"key1", 4}, {"key2", :inf_max}],
        time_range: 1525942123224,
        direction: :forward

  Also, there is an alternative `stream_range/5` to iteratively get range of rows in stream.

  ## Options

    * `:direction`, required, the order of fetch data, available options are `:forward` | `:backward`, by it is `:forward`.
      - `:forward`, this query is performed in the order of primary key in ascending, in this case, input `inclusive_start_primary_keys` should less
      than `exclusive_end_primary_keys`;
      - `:backward`, this query is performed in the order of primary key in descending, in this case, input `inclusive_start_primary_keys` should greater
      than `exclusive_end_primary_keys`.
    * `:columns_to_get`, optional, fetch the special fields, by default it returns all fields, pass a field list to specify the expected return fields,
      e.g. `["field1", "field2"]`.
    * `:start_column`, optional, specifies the start column when using for wide-row-read, the returned result contains this `:start_column`.
    * `:end_column`, optional, specifies the end column when using for wide-row-read, the returned result does not contain this `:end_column`.
    * `:filter`, optional, filter the return results in the server side, please see `filter/1` for details.
    * `:max_versions`, optional, how many versions need to return in results, by default it is 1.
    * `:transaction_id`, optional, read operation within local transaction.
    * `:limit`, optional, the maximum number of rows of data to be returned, this value must be greater than 0, whether this option is set or not, there
      returns a maximum of 5,000 data rows and the total data size never exceeds 4 MB.
    * `:time_range`, optional, read data by timestamp range, support two ways to use it:
      - `time_range: {start_timestamp, end_timestamp}`, the timestamp in the range (include `start_timestamp` but exclude `end_timestamp`)
        and then will return in the results.
      - `time_range: special_timestamp`, exactly match and then will return in the results.
      - `:time_range` and `:max_versions` are mutually exclusive, by default use `max_versions: 1` and `time_range: nil`.
  """
  @doc row: :row
  @spec get_range(
          instance,
          inclusive_start_primary_keys,
          exclusive_end_primary_keys,
          options
        ) :: result
  def get_range(
        instance,
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options \\ []
      )

  def get_range(
        instance,
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options
      )
      when is_list(inclusive_start_primary_keys) do
    prepared_var =
      prepared_get_range(
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options
      )

    Client.get_range(instance, prepared_var, nil)
  end

  def get_range(
        instance,
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options
      )
      when is_binary(inclusive_start_primary_keys) do
    prepared_var =
      %Var.GetRange{
        table_name: table_name,
        exclusive_end_primary_keys: exclusive_end_primary_keys
      }
      |> map_options(options)

    Client.get_range(instance, prepared_var, inclusive_start_primary_keys)
  end

  @doc """
  As a wrapper built on `get_range/5` to fetch a full matched data set by iterate, if process a large items,
  recommend to use `stream_range/5`.

  ## Example

      import MyApp.TableStore

      iterate_all_range table_name1,
        [{"key1", 1}, {"key2", :inf_min}],
        [{"key1", 4}, {"key2", :inf_max}],
        direction: :forward
  ## Options

    Please see options of `get_range/5` for details.
  """
  @doc row: :row
  @spec iterate_all_range(
          instance,
          table_name,
          inclusive_start_primary_keys,
          exclusive_end_primary_keys,
          options
        ) :: result
  def iterate_all_range(
        instance,
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options \\ []
      ) do
    prepared_var =
      prepared_get_range(
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options
      )

    Client.iterate_get_all_range(instance, prepared_var)
  end

  @doc """
  As a wrapper built on `get_range/5` to create composable and lazy enumerable stream for iteration.

  ## Example

      import MyApp.TableStore

      stream =
        stream_range table_name1,
          [{"key1", 1}, {"key2", :inf_min}],
          [{"key1", 4}, {"key2", :inf_max}],
          direction: :forward

      Enum.to_list(stream, fn
        {:ok, %{rows: rows} = response} ->
          # process rows
        {:error, error} ->
          # occur error
      end)

  ## Options

    Please see options of `get_range/5` for details.
  """
  @doc row: :row
  @spec stream_range(
          instance,
          inclusive_start_primary_keys,
          exclusive_end_primary_keys,
          options
        ) :: Enumerable.t()
  def stream_range(
        instance,
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options \\ []
      ) do
    prepared_var =
      prepared_get_range(
        table_name,
        inclusive_start_primary_keys,
        exclusive_end_primary_keys,
        options
      )

    Client.stream_range(instance, prepared_var)
  end

  @compile {:inline, prepared_get_range: 4}
  defp prepared_get_range(
         table_name,
         inclusive_start_primary_keys,
         exclusive_end_primary_keys,
         options
       ) do
    map_options(
      %Var.GetRange{
        table_name: table_name,
        inclusive_start_primary_keys: inclusive_start_primary_keys,
        exclusive_end_primary_keys: exclusive_end_primary_keys
      },
      options
    )
  end

  @doc """
  The one entrance to use search index functions, please see `ExAliyunOts.Search` module for details.

  Official document in [Chinese](https://help.aliyun.com/document_detail/91974.html) | [English](https://www.alibabacloud.com/help/doc-detail/91974.html)

  ## Options

    * `:search_query`, required, the main option to use query and sort.
      - `:query`, required, bind to the query functions:
        - `ExAliyunOts.Search.bool_query/1`
        - `ExAliyunOts.Search.exists_query/1`
        - `ExAliyunOts.Search.geo_bounding_box_query/3`
        - `ExAliyunOts.Search.geo_distance_query/3`
        - `ExAliyunOts.Search.geo_polygon_query/2`
        - `ExAliyunOts.Search.match_all_query/0`
        - `ExAliyunOts.Search.match_phrase_query/2`
        - `ExAliyunOts.Search.match_query/3`
        - `ExAliyunOts.Search.nested_query/3`
        - `ExAliyunOts.Search.prefix_query/2`
        - `ExAliyunOts.Search.range_query/2`
        - `ExAliyunOts.Search.term_query/2`
        - `ExAliyunOts.Search.terms_query/2`
        - `ExAliyunOts.Search.wildcard_query/2`
      - `:sort`, optional, by default it is use `pk_sort/1`, bind to the Sort functions:
        - `ExAliyunOts.Search.field_sort/2`
        - `ExAliyunOts.Search.geo_distance_sort/3`
        - `ExAliyunOts.Search.nested_filter/2`
        - `ExAliyunOts.Search.pk_sort/1`
        - `ExAliyunOts.Search.score_sort/1`
      - `:aggs`, optional, please see official document in [Chinese](https://help.aliyun.com/document_detail/132191.html) | [English](https://www.alibabacloud.com/help/doc-detail/132191.html).
      - `:group_bys`, optional, please see official document in [Chinese](https://help.aliyun.com/document_detail/132210.html) | [English](https://www.alibabacloud.com/help/doc-detail/132210.html).
      - `:limit`, optional, the limited size of query.
      - `:offset`, optional, the offset size of query. When the total rows are less or equal than 2000, can both used`:limit` and `:offset` to pagination.
      - `:get_total_count`, optional, return the total count of the all matched rows, by default it is `true`.
      - `:token`, optional, when do not load all the matched rows in a single request, there will return a `next_token` value in that result,
      and then we can pass it to `:token` in the next same search query to continue load the rest rows.
      - `:collapse`, optional, duplicate removal by the specified field, please see official document in [Chinese](https://help.aliyun.com/document_detail/154172.html), please NOTICE that currently there does not support use `:collapse` with `:token` together.
    * `:columns_to_get`, optional, fetch the special fields, by default it returns all fields, here are available options:
      - `:all`, return all attribute column fields;
      - `:none`, do not return any attribute column fields;
      - `["field1", "field2"]`, specifies the expected return attribute column fields.
  """
  @doc search: :search
  @spec search(instance, table_name, index_name, options) :: result
  def search(instance, table_name, index_name, options) do
    prepared_var = prepared_search(table_name, index_name, options)
    Client.search(instance, prepared_var)
  end

  @doc """
  As a wrapper built on `search/4` to create composable and lazy enumerable stream for iteration.

  ## Options

    Please see options of `search/4` for details.
  """
  @doc search: :search
  @spec stream_search(instance, table_name, index_name, options) :: Enumerable.t()
  def stream_search(instance, table_name, index_name, options) do
    prepared_var = prepared_search(table_name, index_name, options)
    Client.stream_search(instance, prepared_var)
  end

  @doc """
  As a wrapper built on `search/4` to fetch a full matched data set by iterate, if process a large items,
  recommend to use `stream_search/4`.

  ## Options

    Please see options of `search/4` for details.
  """
  @doc search: :search
  @spec iterate_search(instance, table_name, index_name, options) :: result
  def iterate_search(instance, table_name, index_name, options) do
    prepared_var = prepared_search(table_name, index_name, options)
    Client.iterate_search(instance, prepared_var)
  end

  defp prepared_search(table_name, index_name, options) do
    ExAliyunOts.Search.map_search_options(
      %Var.Search.SearchRequest{table_name: table_name, index_name: index_name},
      options
    )
  end

  @doc """
  Query current supported maximum number of concurrent tasks to `parallel_scan/4` request.

  Official document in [Chinese](https://help.aliyun.com/document_detail/153862.html) | [English](https://www.alibabacloud.com/help/doc-detail/153862.htm)
  """
  @doc search: :search
  @spec compute_splits(instance, table_name, index_name) :: result
  defdelegate compute_splits(instance, table_name, index_name), to: Client

  @doc """
  Leverage concurrent tasks to query matched raw data (still be with search function) more quickly, in this use case, this function is improved for speed up
  scan query, but no guarantee to the order of query results, and does not support the aggregation of scan query.

  In general, recommend to use `iterate_parallel_scan/5` or `iterate_parallel_scan/7` for the common use case of parallel scan.

  Official document in [Chinese](https://help.aliyun.com/document_detail/153862.html) | [English](https://www.alibabacloud.com/help/doc-detail/153862.htm)

  ## Options

    * `:scan_query`, required, the main option to use query.
      - `:query`, required, bind to the query functions, the same as query option of `search/4`.
      - `:limit`, optional, the limited size of query, defaults to 2000, the maximum value of limit is 2000.
      - `:token`, optional, when do not load all the matched rows in a single request, there will return a `next_token` value in that result,
      and then we can pass it to `:token` in the next same scan query to continue load the rest rows.
      - `:max_parallel`, required, the maximum number of concurrent, as the `splits_size` value from the response of `compute_splits/3`.
      - `:current_parallel_id`, required, refer the official document, the available value is in [0, max_parallel).
    * `:columns_to_get`, optional, fetch the special fields, by default it returns all fields of the search index, here are available options:
      - `:all_from_index`, return all attribute column fields of search index;
      - `:none`, do not return any attribute column fields;
      - `["field1", "field2"]`, specifies the expected return attribute column fields.
    * `session_id`, as usual, this option is required from the response of `compute_splits/3`, if not set this option, the query result may contain
    duplicate data, refer the official document, once occurs an `OTSSessionExpired` error, must initiate another parallel scan task to re-query data.
  """
  @doc search: :search
  @spec parallel_scan(instance, table_name, index_name, options) :: result
  def parallel_scan(instance, table_name, index_name, options) do
    request = ExAliyunOts.Search.map_scan_options(table_name, index_name, options)
    Client.parallel_scan(instance, request)
  end

  @doc """
  A simple wrapper of `stream_parallel_scan/4` to take care `OTSSessionExpired` error with retry, make parallel scan
  as a stream that applies the given function to the complete result of scan query.

  In general, recommend to use this function for the common use case of parallel scan.

  ## Options

    * `:scan_query`, required, the main option to use query.
      - `:query`, required, bind to the query functions, the same as query option of `search/5`.
      - `:limit`, optional, the limited size of query, defaults to 2000, the maximum value of limit is 2000.
    * `:columns_to_get`, optional, fetch the special fields, by default it returns all fields of the search index, here are available options:
      - `:all_from_index`, return all attribute column fields of search index;
      - `:none`, do not return any attribute column fields;
      - `["field1", "field2"]`, specifies the expected return attribute column fields.
    * `:timeout`, optional, the `:timeout` option of `Task.async_stream/3`, defaults to `:infinity`.

  ## Example

      def iterate_stream(stream) do
        Enum.map(stream, fn
          {:ok, response} ->
            response
          {:error, error} ->
            error
        end)
      end

      iterate_parallel_scan(
        "table",
        "index",
        &iterate_stream/1,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 1000
        ],
        columns_to_get: ["is_actived", "name", "score"]
      )

  """
  @doc search: :search
  @spec iterate_parallel_scan(instance, table_name, index_name, fun :: (term -> term), options) ::
          term()
  def iterate_parallel_scan(instance, table_name, index_name, fun, options)
      when is_function(fun) do
    result =
      instance
      |> stream_parallel_scan(table_name, index_name, options)
      |> fun.()

    case result do
      {:error, %ExAliyunOts.Error{code: "OTSSessionExpired"}} ->
        Logger.info("scan_query session expired, will renew a parallel scan task.")
        iterate_parallel_scan(instance, table_name, index_name, fun, options)

      other ->
        other
    end
  end

  @doc """
  A simple wrapper of `stream_parallel_scan/4` to take care `OTSSessionExpired` error with retry, make parallel scan
  as a stream that applies the given function from `module` with the list of arguments `args` to the complete result of scan query.

  In general, recommend to use this function for the common use case of parallel scan.

  ## Options

    Please see options of `iterate_parallel_scan/5`.

  ## Example

      defmodule StreamHandler do
        def iterate_stream(stream) do
          Enum.map(stream, fn
            {:ok, response} ->
              response
            {:error, error} ->
              error
          end)
        end
      end

      iterate_parallel_scan(
        "table",
        "index",
        StreamHandler,
        :iterate_stream,
        [],
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 1000
        ],
        columns_to_get: ["field1", "field2"]
      )

  """
  @doc search: :search
  @spec iterate_parallel_scan(
          instance,
          table_name,
          index_name,
          mod :: module(),
          fun :: atom(),
          args :: [term],
          options
        ) :: term()
  def iterate_parallel_scan(instance, table_name, index_name, mod, fun, args, options) do
    value = stream_parallel_scan(instance, table_name, index_name, options)

    case apply(mod, fun, [value | args]) do
      {:error, %ExAliyunOts.Error{code: "OTSSessionExpired"}} ->
        Logger.info("scan_query session expired, will renew a parallel scan task.")
        iterate_parallel_scan(instance, table_name, index_name, mod, fun, args, options)

      other ->
        other
    end
  end

  @doc """
  Integrate `parallel_scan/4` with `compute_splits/3` as a complete use, base on the response of `compute_splits/3` to create the corresponding
  number of concurrency task(s), use `Task.async_stream/3` to make parallel scan as a stream which properly process `token`
  in every request of the internal, when use this function need to consider the possibility of the `OTSSessionExpired` error in the external.

  ## Options

    Please see options of `iterate_parallel_scan/5`.
  """
  @doc search: :search
  @spec stream_parallel_scan(instance, table_name, index_name, options) :: Enumerable.t()
  defdelegate stream_parallel_scan(instance, table_name, index_name, options),
    to: ExAliyunOts.Search

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/117477.html) | [English](https://www.alibabacloud.com/help/doc-detail/117477.html)

  ## Example

      import MyApp.TableStore

      list_search_index("table")
  """
  @doc search: :search
  @spec list_search_index(instance, table_name) :: result
  defdelegate list_search_index(instance, table_name), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/117452.html) | [English](https://www.alibabacloud.com/help/doc-detail/117452.html)

  ## Example

      import MyApp.TableStore

      create_search_index "table", "index_name",
        field_schemas: [
          field_schema_keyword("name"),
          field_schema_integer("age")
        ]

      create_search_index "table", "index_name",
        field_schemas: [
          field_schema_keyword("name"),
          field_schema_geo_point("location"),
          field_schema_integer("value")
        ]

      create_search_index "table", "index_name",
        field_schemas: [
          field_schema_nested(
          "content",
          field_schemas: [
            field_schema_keyword("header"),
            field_schema_keyword("body")
          ]
        )
      ]

  ## Options

    * `:field_schemas`, required, a list of predefined search-index schema fields, please see the following helper functions:
      - `ExAliyunOts.Search.field_schema_integer/2`
      - `ExAliyunOts.Search.field_schema_float/2`
      - `ExAliyunOts.Search.field_schema_boolean/2`
      - `ExAliyunOts.Search.field_schema_keyword/2`
      - `ExAliyunOts.Search.field_schema_text/2`
      - `ExAliyunOts.Search.field_schema_nested/2`
      - `ExAliyunOts.Search.field_schema_geo_point/2`
    * `:index_sorts`, optional, a list of predefined sort-index schema fields, please see the following helper functions:
      - `ExAliyunOts.Search.pk_sort/1`
      - `ExAliyunOts.Search.field_sort/2`
      - `ExAliyunOts.Search.geo_distance_sort/3`
  """
  @doc search: :search
  @spec create_search_index(instance, table_name, index_name, options) :: result
  def create_search_index(instance, table_name, index_name, options) do
    var_request = %Var.Search.CreateSearchIndexRequest{
      table_name: table_name,
      index_name: index_name,
      index_schema: %Var.Search.IndexSchema{
        field_schemas: Keyword.fetch!(options, :field_schemas),
        index_sorts: Keyword.get(options, :index_sorts)
      }
    }

    Client.create_search_index(instance, var_request)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/117478.html) | [English](https://www.alibabacloud.com/help/doc-detail/117478.html)

  ## Example

      import MyApp.TableStore

      delete_search_index("table", "index_name")
  """
  @doc search: :search
  @spec delete_search_index(instance, table_name, index_name) :: result
  def delete_search_index(instance, table_name, index_name) do
    var_delete_request = %Var.Search.DeleteSearchIndexRequest{
      table_name: table_name,
      index_name: index_name
    }

    Client.delete_search_index(instance, var_delete_request)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/117475.html) | [English](https://www.alibabacloud.com/help/doc-detail/117475.html)

  ## Example

      import MyApp.TableStore

      describe_search_index("table", "index_name")
  """
  @doc search: :search
  @spec describe_search_index(instance, table_name, index_name) :: result
  def describe_search_index(instance, table_name, index_name) do
    var_describe_request = %Var.Search.DescribeSearchIndexRequest{
      table_name: table_name,
      index_name: index_name
    }

    Client.describe_search_index(instance, var_describe_request)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/93819.html) | [English](https://www.alibabacloud.com/help/doc-detail/93819.html)

  ## Example

      import MyApp.TableStore

      partition_key = {"key", "key1"}
      start_local_transaction("table", partition_key)
  """
  @doc local_transaction: :local_transaction
  @spec start_local_transaction(instance, table_name, partition_key :: tuple()) :: result
  def start_local_transaction(instance, table_name, partition_key) do
    var_start_local_transaction = %Var.Transaction.StartLocalTransactionRequest{
      table_name: table_name,
      partition_key: partition_key
    }

    Client.start_local_transaction(instance, var_start_local_transaction)
  end

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/93819.html) | [English](https://www.alibabacloud.com/help/doc-detail/93819.html)

  ## Example

      import MyApp.TableStore

      commit_transaction("transaction_id")
  """
  @doc local_transaction: :local_transaction
  @spec commit_transaction(instance, transaction_id :: String.t()) :: result
  defdelegate commit_transaction(instance, transaction_id), to: Client

  @doc """
  Official document in [Chinese](https://help.aliyun.com/document_detail/93819.html) | [English](https://www.alibabacloud.com/help/doc-detail/93819.html)

  ## Example

      import MyApp.TableStore

      abort_transaction("transaction_id")
  """
  @doc local_transaction: :local_transaction
  defdelegate abort_transaction(instance, transaction_id), to: Client

  defp map_options(var, nil), do: var

  defp map_options(var, options) do
    options
    |> Keyword.keys()
    |> Enum.reduce(var, fn key, acc ->
      value = Keyword.get(options, key)

      if value != nil and Map.has_key?(var, key) do
        case key do
          :return_type ->
            Map.put(acc, key, map_return_type(value))

          :direction ->
            Map.put(acc, key, map_direction(value))

          :stream_spec ->
            Map.put(acc, key, struct(Var.StreamSpec, value))

          :time_range ->
            Map.put(acc, key, map_time_range(value))

          _ ->
            Map.put(acc, key, value)
        end
      else
        acc
      end
    end)
  end

  defp map_return_type(nil), do: :RT_NONE

  ReturnType.constants()
  |> Enum.map(fn {_value, type} ->
    downcase_type = type |> to_string() |> String.slice(3..-1) |> Utils.downcase_atom()

    defp map_return_type(unquote(downcase_type)), do: unquote(type)
    defp map_return_type(unquote(type)), do: unquote(type)
  end)

  defp map_return_type(invalid_return_type) do
    raise ExAliyunOts.RuntimeError, "invalid return_type: #{inspect(invalid_return_type)}"
  end

  Direction.constants()
  |> Enum.map(fn {_value, type} ->
    defp map_direction(unquote(Utils.downcase_atom(type))), do: unquote(type)
    defp map_direction(unquote(type)), do: unquote(type)
  end)

  defp map_direction(invalid_direction) do
    raise ExAliyunOts.RuntimeError, "invalid direction: #{inspect(invalid_direction)}"
  end

  defp map_time_range(specific_time) when is_integer(specific_time) do
    %Var.TimeRange{specific_time: specific_time}
  end

  defp map_time_range({start_time, end_time})
       when is_integer(start_time) and is_integer(end_time) do
    %Var.TimeRange{start_time: start_time, end_time: end_time}
  end

  @operation_type_mapping OperationType.updates_supported()
                          |> Enum.map(fn type -> {Utils.downcase_atom(type), type} end)
  defp map_updates(options) do
    Enum.reduce(@operation_type_mapping, %{}, fn {update_operation, operation_type}, acc ->
      {matched_update, _rest_opts} = Keyword.pop(options, update_operation)

      if matched_update != nil do
        Map.put(acc, operation_type, matched_update)
      else
        acc
      end
    end)
  end
end
