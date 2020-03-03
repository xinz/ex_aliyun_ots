# ExAliyunOts

[![hex.pm version](https://img.shields.io/hexpm/v/ex_aliyun_ots.svg)](https://hex.pm/packages/ex_aliyun_ots)
[![Coverage Status](https://coveralls.io/repos/github/xinz/ex_aliyun_ots/badge.svg?branch=master)](https://coveralls.io/github/xinz/ex_aliyun_ots?branch=master)

Aliyun TableStore SDK for Elixir/Erlang

> Tablestore is a NoSQL database service built on Alibaba Cloud’s Apsara distributed operating system that can store and access large volumes of structured data in real time.

## Installation

```elixir
def deps do
  [
    {:ex_aliyun_ots, "~> 0.5"}
  ]
end
```

## Configuration

配置表格存储的实例信息，支持配置多个表格存储实例在同一个应用中使用

```elixir
config :ex_aliyun_ots, MyInstance
  name: "MyInstanceName",
  endpoint: "MyInstanceEndpoint",
  access_key_id: "MyAliyunRAMKeyID",
  access_key_secret: "MyAliyunRAMKeySecret"

config :ex_aliyun_ots,
  instances: [MyInstance],
  debug: false, # Optional
  enable_tunnel: false # Optional
```

* `debug`，配置是否打开debug logger，默认值为false
* `enable_tunnel`，配置是否在`Application`启动时也加载`Tunnel`所需要的`Supervisor`及`Registry`，默认值为false

## Ecto Adapter

我们根据Ecto的Adapter规范要求实现[Tablestore adapter for Ecto](https://hex.pm/packages/ecto_tablestore)，使用它可集成更加统一、灵活的方式处理数据。

## Supported Functions

* 表格存储的表操作
* 表格存储的数据操作
* 条件更新
* 主键列自增
* 使用过滤器
* 原子计数器
* 多元索引
* 局部事务
* 通道服务
* Timeline模型

## Supported API

* [表操作](#General)
  * [CreateTable](#CreateTable)
  * [ListTable](#ListTable)
  * [DeleteTable](#DeleteTable)
  * [UpdateTable](#UpdateTable)
  * [DescribeTable](#DescribeTable)

* [行操作](#Row)
  * [PutRow](#PutRow)
  * [GetRow](#GetRow)
  * [UpdateRow](#UpdateRow)
  * [DeleteRow](#DeleteRow)
  * [GetRange](#GetRange)
  * [BatchGetRow](#BatchGetRow)
  * [BatchWriteRow](#BatchWriteRow)
  
* [多元索引](#SearchIndex)
  * [CreateSearchIndex](#CreateSearchIndex)
  * [DescribeSearchIndex](#DescribeSearchIndex)
  * [DeleteSearchIndex](#DeleteSearchIndex)
  * [Search](#Search)

* [局部事务](#LocalTransaction)
  * [StartLocalTransaction](#StartLocalTransaction)
  * [CommitTransaction](#CommitTransaction)
  * [AbortTransaction](#AbortTransaction)

* [SDK内置提供](#Other)

  * [IterateAllRange](#IterateAllRange)
  * [Sequence](#Sequence)

## Sample

添加完SDK依赖以及完成相关[配置](#Configuration)之后，请继续参考以下示例。

*更多参考请见测试用例（在 [test](https://github.com/xinz/ex_aliyun_ots/tree/master/test/mixin) 目录）*


### <a name="General"></a>表操作

#### <a name="CreateTable"></a>CreateTable

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  # 
  # Optional settings
  #
  # `reserved_throughput_write`，创建的表的初始预留写吞吐量设定，默认值为0。
  # `reserved_throughput_read`，创建的表的初始预留读吞吐量设定，默认值为0。
  # `time_to_live`，保存的数据的存活时间，单位秒，默认值为"-1"（永久）。
  # `max_versions`，表保留的最大版本，默认值为1，只设定一个版本。
  # `deviation_cell_version_in_sec`，最大版本偏差，默认值为86400秒，即1天。
  # `stream_spec`，是否打开stream相关的属性。
  #   [
  #     `is_enabled`, 是否打开stream
  #     `expiration_time`, 表的stream过期时间 
  #   ]
  # 
 
  def test() do
    create_table "table_name1",
      [{"key1", PKType.integer}, {"key2", PKType.string}]
      
    create_table "table_name2",
      [{"key1", PKType.string}, {"key2", PKType.auto_increment}]
    
    create_table "table_name3", 
      [{"key1", PKType.string}],
      reserved_throughput_write: 1,
      reserved_throughput_read: 1,
      time_to_live: 100_000,
      max_versions: 3,
      deviation_cell_version_in_sec: 6_400,
      stream_spec: [is_enabled: true, expiration_time: 2]
  end    
end
```

#### <a name="ListTable"></a>ListTable

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  def test() do
    list_table()
  end
  
end
```

#### <a name="DeleteTable"></a>DeleteTable

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
 
  def test() do
    delete_table("table_name")
  end
  
end
```

#### <a name="UpdateTable"></a>UpdateTable

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
 
  # 
  # 可选项设定
  #
  # `reserved_throughput_write`，创建的表的初始预留写吞吐量设定，默认不设定。
  # `reserved_throughput_read`，创建的表的初始预留读吞吐量设定，默认不设定。
  # `time_to_live`，保存的数据的存活时间，单位秒，默认值为"-1"（永久）。
  # `max_versions`，表保留的最大版本，默认值为1，只设定一个版本。
  # `deviation_cell_version_in_sec`，最大版本偏差，默认值为86400秒，即1天。
  # `stream_spec`，是否打开stream相关的属性。
  #   [
  #     `is_enabled`，是否打开stream
  #     `expiration_time`，表的stream过期时间 
  #   ]
  # 
  def test() do
   update_table "table_name",
     reserved_throughput_write: 10,
     time_to_live: 200_000,
     stream_spec: [is_enabled: false]
  end
  
end
```

#### <a name="DescribeTable"></a>DescribeTable

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  def test() do
    describe_table(table_name)
  end
  
end
```

### <a name="Row"></a>行操作

#### <a name="PutRow"></a>PutRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # `condition`，数据插入前是否进行存在性检查，可选项：
  #   `:ignore`，表示不做行存在性检查
  #   `:expect_not_exist`，表示期望主键对应的行不存在
  #   `:expect_exist`，表示期望的主键对应的行存在
  #   如果当前的主键中含有自增列时：
  #     如果预期该行主键已经存在，只能使用`:expect_exist`才能成功插入（覆盖）数据行；
  #     如果预期该行主键不存在，只能使用`:ignore`才能成功插入该数据行。
  #
  #
  # 可选设定项:
  # `transaction_id`，更新操作使用局部事务
  #
  def test() do
    put_row "table_name",
      [{"key1", "id1"}],
      [{"name", "name1"}, {"age", 20}],
      condition: condition(:expect_not_exist),
      return_type: :pk

    put_row "table_name",
      [{"key1", "id1"}],
      [{"name", "name1"}, {"age", 20}],
      condition: condition(:expect_not_exist),
      transaction_id: "transaction_id"
      return_type: :pk
  end
  
end
```

#### <a name="GetRow"></a>GetRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # 支持按条件过滤查询（通过使用`filter`操作）
  # 更便于理解的表达式, ">"、"<"、"=="、"and"、"or"、"()"
  # 针对不存在的属性列，可通过[ignore_if_missing: true]的语法进行条件过滤：
  #    当针对某一不存在的属性列同时又设定为ignore_if_missing: true进行条件过滤，将在返回的查询结果中忽略该这一匹配分支；
  #    当针对某一已存在的属性列，将不适用使用`ignore_if_missing: true|false`的情况，已提供的匹配条件将始终影响返回的结果，如果查询条件不满足，将不会有匹配的返回结果。
  # 通过[latest_version_only: true]的语法，如果为true，则表示只检测最新版本的值是否满足条件；如果是false，则会检测所有版本的值是否满足条件
  #
  # 可通过`filter`支持属性列上的分页读取操作（通过使用`pagination`操作），主要用于宽行读取
  #
  # 可选设定项
  # `columns_to_get`，指定获取的属性列。
  # `start_column`，指定读取时的起始列，主要用于宽行读，返回的结果中包含当前起始列。
  # `end_column`，指定读取时的结束列，主要用于宽行读，返回的结果中不包含当前结束列。
  # `filter`，支持按条件过滤查询（通过使用`filter`操作）。
  # `max_versions`，和time_range只能存在一个，读取数据时，返回的最多版本个数，默认值为1。
  # `time_range`，和max_versions只能存在一个，读取数据的版本时间戳范围，支持2种定义方式，如：
  #   time_range: {start_timestamp, end_timestamp}
  #   time_range: specail_timestamp
  # `transaction_id`，读取操作使用局部事务
  #
  def test() do
    get_row table_name1,
      [{"key1", "id1"}, {"key2", "id2"}],
      columns_to_get: ["name", "level"],
      filter: filter(("name[ignore_if_missing: true, latest_version_only: true]" == var_name and "age" > 1) or ("class" == "1"))
      
    get_row table_name,
      [{"key", "1"}],
      start_column: "room",
      filter: pagination(offset: 0, limit: 3)

    get_row table_name,
      [{"key", "1"}],
      transaction_id: "transaction_id"
  end
  
end
```

#### <a name="UpdateRow"></a>UpdateRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # Operations:
  #
  #   `put`，如果设定该操作，此时value必须为有效的属性列值。语意为如果该列不存在，则新增一列；如果该列存在，则覆盖该字段列。
  #   `delete`，如果设定该操作，值必须设定为nil，同时需要指定该列一值的timestamp。
  #   `delete_all`，如果设定该操作，只需要设定需要删除操作的列值列表。
  #   `increment`，使用原子计数自增/自减对应的属性列，只允许整型操作。
  #
  # `return_type`
  #   支持`:pk`返回主键信息，`:none`不返回任何信息。
  #   当针对原子计数操作时，可通过设定`return_type`为`ReturnType.after_modify`，同时指定返回进行原子计数操作的属性列名（`return_columns`），
  #   可通过这种方式获取原子计数操作之后的属性列的值。 注意：`return_type` 设定为 after_modify，以及设定`return_columns`仅适用于原子计数操作。
  #
  # `condition`，在数据更新前是否进行存在性检查，可选项：
  #   `:expect_exist`，表示期望行存在；
  #   `:ignore`，表示不做行存在性检查；
  #   同时支持在condition()进行条件查询，见`filter`操作。
  #   如果当前的主键中含有自增列时：
  #     如果预期该行主键已经存在，只能使用`:expect_exist`才能成功更新数据行；
  #     如果预期该行主键不存在，只能使用`:ignore`才能成功更新该数据行。
  #
  #
  # 可选设定项
  # `transaction_id`，更新操作使用局部事务
  #
  def test() do
    value = "value1"
    update_row table_name1,
      [{"key1", 2}, {"key2", "2"}],
      delete: [{"attr2", nil, 1524464460}],
      delete_all: ["attr1"],
      put: [{"attr3", "put_attr3"}],
      return_type: :pk,
      condition: condition(:expect_exist, "attr2" == value)
  end
  
  def test_atomic_inc() do  
    {:ok, response} = 
      update_row table_name, [{"key1", 1}],
        put: [{"attr1", "put_attr1"}],
        increment: [{"count", 1}],
        return_type: ReturnType.after_modify,
        return_columns: ["count"],
        condition: condition(:ignore)
  end
  
  def test_with_transaction() do
    update_row @table, [partition_key],
      put: [{"new_attr1", "a1"}],
      delete_all: ["level", "size"],
      condition: condition(:ignore),
      transaction_id: "transaction_id"
  end
end
```

#### <a name="DeleteRow"></a>DeleteRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # `condition`，数据插入前是否进行存在性检查，可选项：
  #   `:expect_exist`，表示期望的主键对应的行存在；
  #   `:ignore`，表示不做行存在性检查；
  #   同时支持在condition()进行条件查询，见`filter`操作。
  #   如果当前的主键中含有自增列时：
  #     如果预期该行主键已经存在，可以使用`:expect_exist`或`:ignore`成功删除该数据行。
  #
  #
  # 可选设定项
  #   `transaction_id`，删除操作使用局部事务
  #
  def test() do
    delete_row table_name1,
      [{"key1", 3}, {"key2", "3"}],
      condition: condition(:expect_exist, "attr2" == "value2")

    delete_row table_name1,
      [{"key1", 3}, {"key2", "3"}],
      condition: condition(:expect_exist, "attr2" == "value2"),
      transaction_id: "transaction_id"
  end
  
end
```

#### <a name="GetRange"></a>GetRange

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  # 
  # 读取指定主键范围内的数据
  # `direction`，查询数据的顺序，默认值为forward
  #   设定为`:forward`，表示此次查询按照主键由小到大的顺序进行
  #   设定为`:backward`，表示此次查询按照主键由大到小的顺序进行
  #
  #
  # 可选设定项
  # `columns_to_get`，指定获取的属性列
  # `start_column`，指定读取时的起始列，主要用于宽行读，返回的结果中包含当前起始列
  # `end_column`，指定读取时的结束列，主要用于宽行读，返回的结果中不包含当前结束列
  # `filter`，支持按条件过滤查询（通过使用`filter`操作）
  # `max_versions`，和time_range只能存在一个，读取数据时，返回的最多版本个数，默认值为1
  # `time_range`，和max_versions只能存在一个，读取数据的版本时间戳范围，支持2种定义方式，如：
  #   time_range: {start_timestamp, end_timestamp}
  #   time_range: specail_timestamp
  # `transaction_id`，读取操作使用局部事务
  #
  def test() do  
    #
    # [{"key1", 1}, {"key2", PKType.inf_min}] 作为 inclusive_start_primary_key
    # [{"key1", 4}, {"key2", PKType.inf_max}] 作为 exclusive_end_primary_keys
    #
    get_range "table_name",
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      direction: :forward
      
    get_range table_name1,
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      time_range: {1525922253224, 1525923253224},
      direction: :forward
      
    {:ok, get_range_response} =
       get_range table_name1,
         [{"key1", 1}, {"key2", PKType.inf_min}],
         [{"key1", 4}, {"key2", PKType.inf_max}],
         time_range: 1525942123224,
         direction: :forward

    #
    # 如果查询范围结果仍有未完整查询的数据，可使用上一查询结果中的`next_start_primary_key`
    # 直接作为`inclusive_start_primary_key`用于下一次的迭代查询。
    #
    get_range table_name1,
      get_range_response.next_start_primary_key,
      [{"key1", 4}, {"key2", PKType.inf_max}],
      time_range: 1525942123224,
      direction: :forward

    # 局部事务
    get_range @table_range,
      [{"key", "key1"}, {"key2", PKType.inf_min}],
      [{"key", "key1"}, {"key2", PKType.inf_max}],
      transaction_id: "transaction_id"
  end
  
end
```

#### <a name="BatchGetRow"></a>BatchGetRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # 批量读取一个或多个表中的若干行数据
  # 通过多个get()查询多个表中的记录，get()中的支持的选项与`GetRow`一致。
  #
  def test() do
    batch_get [
      get(table_name1, [[{"key1", 1}, {"key2", "1"}]]),
      get(table_name2, [{"key1", "tab2_id1"}],
        columns_to_get: ["name", "age"],
        filter: filter "age" >= 10
        )
    ]
  end
```

#### <a name="BatchWriteRow"></a>BatchWriteRow

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance
  
  #
  # 批量插入、修改或删除一个或多个表中的若干行数据，为多个 PutRow、UpdateRow、DeleteRow 操作的集合
  # 通过write_delete()、write_put()、write_update()进行更新操作，支持的选项可参考`UpdateRow`。
  #
  # 可选设定项
  # `transaction_id`，批量写操作使用局部事务
  #
  def test() do
    batch_write [
      {table_name1, [
        write_delete([{"key1", 5}, {"key2", "5"}],
          return_type: :pk,
          condition: condition(:expect_exist, "attr1" == 5)),
        write_put([{"key1", 6}, {"key2", "6"}],
          [{"new_put_val1", "val1"}, {"new_put_val2", "val2"}],
          condition: condition(:expect_not_exist),
          return_type: :pk)
      ]},
      {table_name2, [
        write_update([{"key1", "new_tab3_id2"}],
          put: [{"new_put1", "u1"}, {"new_put2", 2.5}],
          condition: condition(:expect_not_exist)),
        write_put([{"key1", "new_tab3_id3"}],
          [{"new_put1", "put1"}, {"new_put2", 10}],
          condition: condition(:expect_not_exist))
      ]}
     ]
  end
  
  #
  # 局部事务仅限于对一张表中的分区键下进行一些读写事务操作，所以当进行批量写操作时，
  # 同时使用局部事务功能时，传入的参数是一个元组{table, [write_xxx(), ...]}，
  # 而不是包含多张表的批量更新操作的列表。
  #
  def test_transaction() do
    partition_key = {"key", "key1"}
    batch_write {
      table_name1,
      [
        write_update([partition_key],
          put: [{"new_added1", 100}, {"new_added2", 101}],
          condition: condition(:ignore)
        )
      ]}, transaction_id: "transaction_id"
  end
end
```

#### <a name="SearchIndex"></a>SearchIndex

Supported SearchIndex Functions:

* MatchAllQuery
* MatchQuery
* MatchPhraseQuery
* TermQuery
* TermsQuery
* PrefixQuery
* RangeQuery
* WildcardQuery
* BoolQuery
* NestedQuery
* ExistsQuery

Not Implemented SearchIndex Functions:

* GeoBoundingBoxQuery
* GeoDistanceQuery
* GeoPolygonQuery

#### <a name="CreateSearchIndex"></a>CreateSearchIndex

当前CreateSearchIndex暂时不支持使用`use ExAliyunOts`的方式。

```elixir
defmodule Sample do

  alias ExAliyunOts.Var.Search
  alias ExAliyunOts.Const.Search.FieldType
  require FieldType

  def test() do
    index_name = "my_index_name"
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "name",
              #field_type: FieldType.keyword, # using as `keyword` field type by default
            },
            %Search.FieldSchema{
              field_name: "age",
              field_type: FieldType.long
            },
            %Search.FieldSchema{
              field_name: "score",
              field_type: FieldType.double
            },
            %Search.FieldSchema{
              field_name: "is_actived",
              field_type: FieldType.boolean
            }
          ]
        }
      }
    result = Client.create_search_index(MyInstance, var_request)
  end
  
  def test_nested() do
    sub_nested1 = %Search.FieldSchema{
      field_name: "header",
      field_type: FieldType.keyword,
    }
    sub_nested2 = %Search.FieldSchema{
      field_name: "body",
      field_type: FieldType.keyword,
    }
    var_request =
      %Search.CreateSearchIndexRequest{
        table_name: table,
        index_name: index_name,
        index_schema: %Search.IndexSchema{
          field_schemas: [
            %Search.FieldSchema{
              field_name: "content",
              field_type: FieldType.nested,
              field_schemas: [
                sub_nested1,
                sub_nested2
              ],
            }
          ]
        }
      }
    result = Client.create_search_index(MyInstance, var_request)
  end
end
```

#### <a name="DescribeSearchIndex"></a>DescribeSearchIndex

```elixir
defmodule Sample do
  use ExAliyunOts,
    instance: MyInstance
    
  def test() do
    describe_search_index("my_table", "my_index_name")
  end
end
```

#### <a name="DeleteSearchIndex"></a>DeleteSearchIndex

```elixir
defmodule Sample do
  use ExAliyunOts,
    instance: MyInstance
    
  def test() do
    delete_search_index("my_table", "my_index_name")
  end
end
```

#### <a name="ListSearchIndex"></a>ListSearchIndex

```elixir
defmodule Sample do
  use ExAliyunOts,
    instance: MyInstance
    
  def test() do
    list_search_index("my_table")
  end
end
```

#### <a name="Search"></a>Search

一些使用方法请见 [测试用例](https://github.com/xinz/ex_aliyun_ots/blob/master/test/mixin/search_test.exs)


### <a name="LocalTransaction"></a>局部事务

目前支持在以下方法中使用局部事务

读操作：`GetRow`、`GetRange`
写操作：`PutRow`、`UpdateRow`、`DeleteRow`、`BatchWriteRow`

*注*：目前存在以下情况（记录于2019年04月01日），当未来表格存储产品更新后，以下情况也将会做对应的更新。

* 如果表有主键自增列，当前不支持使用局部事务。
* 当前局部事务属于公测阶段，开通该功能是表级别的，若需要使用该功能需要向阿里云表格存储技术支持申请开通。

#### <a name="StartLocalTransaction"></a>StartLocalTransaction

```elixir
defmodule Sample do

  use ExAliyunOts,
    instance: MyInstance

  #
  # 针对表的分区键，创建对应的事务，获取到相应的transaction_id，
  # 然后使用transaction_id
  # 用于读操作：GetRow/GetRange
  # 用于写操作：PutRow/UpdateRow/DeleteRow/BatchWrite
  #
  # 请注意，这里是针对表的分区键创建局部事务，而不是表的完整的主键创建局部事务
  #
  def test() do
    partition_key = {"key", "key1"}
    {:ok, response} = start_local_transaction(@table, partition_key)
    reponse.transaction_id
  end
```

#### <a name="CommitTransaction"></a>CommitTransaction

```elixir
defmodule Sample do

  use ExAliyunOts,
    instance: MyInstance

  #
  # 使用transacation_id进行一些写操作后，
  # 确认需要完整地提交这一系列的更新操作，通过该接口可以完成整个事务操作
  #
  def test() do
    commit_transaction(transaction_id)
  end
```

#### <a name="AbortTransaction"></a>AbortTransaction

```elixir
defmodule Sample do

  use ExAliyunOts,
    instance: MyInstance

  #
  # 使用transacation_id进行一些写操作后，
  # 确认需要丢弃这一系列的更新操作，通过该接口可以回滚整个事务操作
  #
  def test() do
    abort_transaction(transaction_id)
  end
```

### <a name="Other"></a>Other

以下功能是由SDK内置提供

#### <a name="IterateAllRange"></a>IterateAllRange

```elixir
defmodule Sample do
  
  use ExAliyunOts,
    instance: MyInstance

  #
  # 读取指定主键范围内的数据，当如果有大量数据时，可通过该方法读取完整的数据
  # 该方法是对`GetRange`的包装处理后的接口，相关配置项与`GetRange`一致。
  #
  def test() do
    iterate_all_range table_name1,
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      direction: :forward
  end
```

### <a name="Sequence"></a>Sequence

```elixir

defmodule SequenceSample do

  use ExUnit.Case
  require Logger

  @instance_key MyInstance
  
  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  #
  # 基于原子计数器，在表操作层面上提供一种方式可以获取到（理论上）唯一自增步长序列
  # Sequence依旧是对表的Update操作，但它会提供更可靠安全的原子操作粒度(原子计数器)
  # 创建一张Sequence表允许有多个event，event表示所使用序列表的场景，它也将用于表的分区键。
  #
  # `GetSequenceNextValue`可选参数
  #   `starter`，首次调用next_value()初始创建的值，默认值为0
  #   `increment_offset`，每次调用next_value()自增步长，默认值为1
  #   `event`，使用序列表的场景，默认值为"default"
  #
  # 在不考虑表格存储系统故障、网络等问题，理论上可以获取到唯一的自增步长的数值，在`Sequence`
  # 有处理遇到异常情况时的重试，但这种情况，并不能保证，获取到新的值是按预期步长自增。
  # 
  # 如需要非常准确的自增步长的序列操作，请自行使用原子计数操作，并通过条件更新来实现，
  # 这一操作预期需要 Read-Increment with condition
  #
  test "next value" do
    sequence_name = "test_sequence"
    var_new = %Var.NewSequence{
      name: sequence_name
    }
    result = Sequence.create(@instance_key, var_new)
    assert result == :ok
    Process.sleep(3_000)

    concurrency_size = 10
    stream = Task.async_stream(1..concurrency_size, fn(_index) -> 
      var_next = %Var.GetSequenceNextValue{
        name: sequence_name,
      }
      Sequence.next_value(@instance_key, var_next)
    end, timeout: :infinity, max_concurrency: concurrency_size)

    result = Enum.map(stream, fn({:ok, item}) -> item end) |> MapSet.new()
    assert MapSet.size(result) == concurrency_size
    assert Enum.sort(result) == Enum.map(1..concurrency_size, fn(item) -> item end)

    del_result = Sequence.delete_event(@instance_key, sequence_name, "default")
    assert {:ok, _delete_response} = del_result

    result = Sequence.delete(@instance_key, sequence_name)
    assert result == :ok
  end

end
```

## References

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

## License

MIT
