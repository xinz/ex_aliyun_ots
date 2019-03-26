# ExAliyunOts

[![hex.pm version](https://img.shields.io/hexpm/v/ex_aliyun_ots.svg)](https://hex.pm/packages/ex_aliyun_ots)
[![Coverage Status](https://coveralls.io/repos/github/xinz/ex_aliyun_ots/badge.svg?branch=0.2)](https://coveralls.io/github/xinz/ex_aliyun_ots?branch=0.2)

Aliyun TableStore SDK for Elixir/Erlang

## Installation

```elixir
def deps do
  [
    {:ex_aliyun_ots, "~> 0.1"}
  ]
end
```

## Configuration

配置表格存储的实例信息，支持配置多个实例在应用中使用

```elixir
config :ex_aliyun_ots, MyInstance
  name: "MyInstanceName",
  endpoint: "MyInstanceEndpoint",
  access_key_id: "MyAliyunRAMKeyID",
  access_key_secret: "MyAliyunRAMKeySecret",
  pool_size: 100, # Optional
  pool_max_overflow: 20 # Optional

config :ex_aliyun_ots, instances: [MyInstance]
```

* `pool_size`，配置对应表格存储实例http请求连接池的最大可用数，默认值是100
* `pool_max_overflow`，配置对应表格存储实例，允许当连接池满负荷用尽时，允许临时创建可用的请求进程数，默认值是20

## Supported API

* [CreateTable](#CreateTable)
* [ListTable](#ListTable)
* [DeleteTable](#DeleteTable)
* [UpdateTable](#UpdateTable)
* [DescribeTable](#DescribeTable)
* [PutRow](#PutRow)
* [GetRow](#GetRow)
* [UpdateRow](#UpdateRow)
* [DeleteRow](#DeleteRow)
* [GetRange](#GetRange)
* [BatchGetRow](#BatchGetRow)
* [BatchWriteRow](#BatchWriteRow)

## Other

* [IterateAllRange](#IterateAllRange)
* [Sequence](#Sequence)

## Operation

添加 `use ExAliyunOts.Mixin` 在任意`Elixir`模块的定义当中

### Sample

*更多参考请见测试用例（在 [test](https://github.com/xinz/ex_aliyun_ots/tree/master/test) 目录）*

<a name="CreateTable"></a>CreateTable

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  alias ExAliyunOts.Const.PKType
  require PKType
  
  @instance "YOUR_INSTANCE_NAME"

  # 
  # Optional settings
  #
  # `reserved_throughput_write`，创建的表的初始预留写吞吐量设定，默认值为0
  # `reserved_throughput_read`，创建的表的初始预留读吞吐量设定，默认值为0
  # `time_to_live`，保存的数据的存活时间，单位秒，默认值为"-1"（永久）
  # `max_versions`，表保留的最大版本，默认值为1，只设定一个版本
  # `deviation_cell_version_in_sec`，最大版本偏差，默认值为86400秒，即1天
  # `stream_spec`，是否打开stream相关的属性
  #   [
  #     `is_enabled`, 是否打开stream
  #     `expiration_time`, 表的stream过期时间 
  #   ]
  # 
 
  def test() do
    create_table @instance,
      "table_name1",
      [{"key1", PKType.integer}, {"key2", PKType.string}]
      
    create_table @instance,
      "table_name2",
      [{"key1", PKType.string}, {"key2", PKType.auto_increment}]
    
    create_table @instance_name,
      "table_name3", 
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

<a name="ListTable"></a>ListTable

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  def test() do
    list_table(@instance_name)
  end
  
end
```

<a name="DeleteTable"></a>DeleteTable

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  def test() do
    delete_table @instance_name, "table_name"
  end
  
end
```

<a name="UpdateTable"></a>UpdateTable

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  # 
  # 可选项设定
  #
  # `reserved_throughput_write`，创建的表的初始预留写吞吐量设定，默认不设定
  # `reserved_throughput_read`，创建的表的初始预留读吞吐量设定，默认不设定
  # `time_to_live`，保存的数据的存活时间，单位秒，默认值为"-1"（永久）
  # `max_versions`，表保留的最大版本，默认值为1，只设定一个版本
  # `deviation_cell_version_in_sec`，最大版本偏差，默认值为86400秒，即1天
  # `stream_spec`，是否打开stream相关的属性
  #   [
  #     `is_enabled`，是否打开stream
  #     `expiration_time`，表的stream过期时间 
  #   ]
  # 
  #
  def test() do
   update_table @instance_name, "table_name",
     reserved_throughput_write: 10,
     time_to_live: 200_000,
     stream_spec: [is_enabled: false]
  end
  
end
```

<a name="DescribeTable"></a>DescribeTable

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  def test() do
    describe_table @instance_name, table_name
  end
  
end
```

<a name="PutRow"></a>PutRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  def test() do
    put_row @instance_name, "table_name", [{"key1", "id1"}],
      [{"name", "name1"}, {"age", 20}],
      condition: condition(:expect_not_exist),
      return_type: :pk
  end
  
end
```

<a name="GetRow"></a>GetRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # 支持按条件过滤查询（通过使用`filter`操作）
  # 更便于理解的表达式, ">"、"<"、"=="、"and"、"or"、"()"
  # 通过[ignore_if_missing: true]的语法，设定为true时，表示当匹配条件不满足时进行忽略该匹配逻辑；设定为false时，表示该匹配条件必须满足，否则查询结果将返回没有可匹配的结果
  # 通过[latest_version_only: true]的语法，如果为true，则表示只检测最新版本的值是否满足条件；如果是false，则会检测所有版本的值是否满足条件
  #
  # 可通过`filter`支持属性列上的分页读取操作（通过使用`pagination`操作），主要用于宽行读取
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
  #
  #
  def test() do
    get_row @instance_name, table_name1,
      [{"key1", "id1"}, {"key2", "id2"}],
      columns_to_get: ["name", "level"],
      filter: filter(("name[ignore_if_missing: true, latest_version_only: true]" == var_name and "age" > 1) or ("class" == "1"))
      
    get_row @instance_name, table_name,
      [{"key", "1"}],
      start_column: "room",
      filter: pagination(offset: 0, limit: 3)
  end
  
end
```

<a name="UpdateRow"></a>UpdateRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # `put`，如果设定该操作，此时value必须为有效的属性列值。语意为如果该列不存在，则新增一列；如果该列存在，则覆盖该字段列。
  # `delete`，如果设定该操作，值必须设定为nil，同时需要指定该列一值的timestamp
  # `delete_all`，如果设定该操作，只需要设定需要删除操作的列值列表
  # `return_type`，支持`:pk`返回主键信息，`:none`不返回任何信息
  # `condition`，在数据更新前是否进行存在性检查
  #   `:expect_exist`，表示期望行存在
  #   `:ignore`，表示不做行存在性检查
  #   同时支持在condition()进行条件查询，见`filter`操作
  #
  #
  def test() do
    value = "value1"
    update_row @instance_name, table_name1,
      [{"key1", 2}, {"key2", "2"}],
      delete: [{"attr2", nil, 1524464460}],
      delete_all: ["attr1"],
      put: [{"attr3", "put_attr3"}],
      return_type: :pk,
      condition: condition(:expect_exist, "attr2" == value)
  end
  
end
```

<a name="DeleteRow"></a>DeleteRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # 删除行操作支持按条件筛选，通过condition()
  #
  #
  def test() do
    delete_row @instance_name, table_name1,
      [{"key1", 3}, {"key2", "3"}],
      condition: condition(:expect_exist, "attr2" == "value2")
  end
  
end
```

<a name="GetRange"></a>GetRange

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  alias ExAliyunOts.Const.PKType
  require PKType
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
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
  #
  #
  def test() do  
    #
    # [{"key1", 1}, {"key2", PKType.inf_min}] 作为 inclusive_start_primary_key
    # [{"key1", 4}, {"key2", PKType.inf_max}] 作为 exclusive_end_primary_keys
    #
    get_range @instance_name, "table_name",
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      direction: :forward
      
    get_range @instance_name, table_name1,
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      time_range: {1525922253224, 1525923253224},
      direction: :forward
      
    {:ok, get_range_response} =
       get_range @instance_name, table_name1,
         [{"key1", 1}, {"key2", PKType.inf_min}],
         [{"key1", 4}, {"key2", PKType.inf_max}],
         time_range: 1525942123224,
         direction: :forward

    #
    # 如果查询范围结果仍有未完整查询的数据，可使用上一查询结果中的`next_start_primary_key`
    # 直接作为`inclusive_start_primary_key`用于下一次的迭代查询
    #
    get_range @instance_name, table_name1,
      get_range_response.next_start_primary_key,
      [{"key1", 4}, {"key2", PKType.inf_max}],
      time_range: 1525942123224,
      direction: :forward
  end
  
end
```

<a name="BatchGetRow"></a>BatchGetRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # 批量读取一个或多个表中的若干行数据
  # 通过多个get()查询多个表中的记录，get()中的支持的选项与`GetRow`一致
  #
  #
  def test() do
    batch_get @instance_name, [
      get(table_name1, [[{"key1", 1}, {"key2", "1"}]]),
      get(table_name2, [{"key1", "tab2_id1"}],
        columns_to_get: ["name", "age"],
        filter: filter "age" >= 10),
    ]
  end
```

<a name="BatchWriteRow"></a>BatchWriteRow

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin
  
  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # 批量插入、修改或删除一个或多个表中的若干行数据，为多个 PutRow、UpdateRow、DeleteRow 操作的集合
  # 通过write_delete()、write_put()、write_update()进行更新操作，支持的选项可参考`UpdateRow`
  #
  #
  def test() do
    batch_write @instance_name, [{table_name1, [
      write_delete([{"key1", 5}, {"key2", "5"}],
        return_type: :pk,
        condition: condition(:expect_exist, "attr1" == 5)),
      write_put([{"key1", 6}, {"key2", "6"}],
        [{"new_put_val1", "val1"}, {"new_put_val2", "val2"}],
        condition: condition(:expect_not_exist),
        return_type: :pk)
    ]}, {table_name2, [
      write_update([{"key1", "new_tab3_id2"}],
        put: [{"new_put1", "u1"}, {"new_put2", 2.5}],
        condition: condition(:expect_not_exist)),
      write_put([{"key1", "new_tab3_id3"}],
        [{"new_put1", "put1"}, {"new_put2", 10}],
        condition: condition(:expect_not_exist))
    ]}]
  end
  
end
```

<a name="IterateAllRange"></a>IterateAllRange

```elixir
defmodule CRUDSample do
  
  use ExAliyunOts.Mixin

  alias ExAliyunOts.Const.PKType
  require PKType

  @instance "YOUR_INSTANCE_NAME"
  
  #
  #
  # 读取指定主键范围内的数据，当如果有大量数据时，可通过该方法读取完整的数据
  # 该方法是对`GetRange`的包装处理后的接口，相关配置项与`GetRange`一致
  #
  #
  def test() do
    iterate_all_range @instance_name, table_name1,
      [{"key1", 1}, {"key2", PKType.inf_min}],
      [{"key1", 4}, {"key2", PKType.inf_max}],
      direction: :forward
  end
```

<a name="Sequence"></a>Sequence

```elixir

defmodule SequenceSample do

  use ExUnit.Case
  require Logger

  @instance_name "YOUR_INSTANCE_NAME"
  
  alias ExAliyunOts.Var
  alias ExAliyunOts.Sequence

  #
  #
  # 基于现有的表格存储支持程度，在表操作层面上提供一种方式可以获取到唯一自增步长序列
  # Sequence依旧是对表记录的操作，但它会提供更可靠安全的原子操作粒度
  # 创建一张Sequence表允许有多个event，表明所使用序列表的场景，event也将用于表的分区键
  #
  # `GetSequenceNextValue`可选参数
  #   `starter`，首次调用next_value()初始创建的值，默认值为0
  #   `increment_offset`，每次调用next_value()自增步长，默认值为1
  #   `event`，使用序列表的场景，默认值为"default"
  #
  #
  
  test "next value" do
    cur_timestamp = Timex.to_unix(Timex.now())
    sequence_name = "test_seq_#{cur_timestamp}"
    var_new_seq = %Var.NewSequence{
      name: sequence_name
    }
    # 创建Sequence
    result = Sequence.create(@instance_name, var_new_seq)
    assert result == :ok
    Process.sleep(3_000)

    # 并发测试
    concurrency_size = 200
    stream = Task.async_stream(1..concurrency_size, fn(_index) ->
      var_next_val = %Var.GetSequenceNextValue{
        name: sequence_name,
      }
      Sequence.next_value(@instance_name, var_next_val)
    end, timeout: :infinity, max_concurrency: concurrency_size)

    result = Enum.map(stream, fn({:ok, item}) -> item end)
    assert length(result) == concurrency_size

    distinct_length = MapSet.new(result) |> MapSet.to_list |> length
    assert distinct_length == concurrency_size
    assert Enum.sort(result) == Enum.map(1..concurrency_size, fn(item) -> item end)

    # 删除"default" event
    del_result = Sequence.delete_event(@instance_name, sequence_name, "default")
    assert {:ok, _delete_response} = del_result

    # 删除整个序列表
    result = Sequence.delete(@instance_name, sequence_name)
    assert result == :ok
  end

end
```

## License

MIT
