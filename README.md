# ExAliyunOts

[![Coverage Status](https://coveralls.io/repos/github/xinz/ex_aliyun_ots/badge.svg?branch=master)](https://coveralls.io/github/xinz/ex_aliyun_ots?branch=master)
[![Module Version](https://img.shields.io/hexpm/v/ex_aliyun_ots.svg)](https://hex.pm/packages/ex_aliyun_ots)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/ex_aliyun_ots/)
[![Total Download](https://img.shields.io/hexpm/dt/ex_aliyun_ots.svg)](https://hex.pm/packages/ex_aliyun_ots)
[![License](https://img.shields.io/hexpm/l/ex_aliyun_ots.svg)](https://github.com/xinz/ex_aliyun_ots/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/xinz/ex_aliyun_ots.svg)](https://github.com/xinz/ex_aliyun_ots/commits/master)

Aliyun [TableStore](https://www.alibabacloud.com/product/table-store) SDK for Elixir/Erlang

> TableStore is a NoSQL database service built on Alibaba Cloud’s Apsara distributed operating system that can store and access large volumes of structured data in real time.

## Installation

```elixir
def deps do
  [
    {:ex_aliyun_ots, "~> 0.6"}
  ]
end
```

## Configuration

Add these settings below into the `config/ots.secret.exs` file.

```elixir
config :ex_aliyun_ots, :my_instance,
  name: "MyInstanceName",
  endpoint: "MyInstanceEndpoint",
  access_key_id: "MyAliyunRAMKeyID",
  access_key_secret: "MyAliyunRAMKeySecret",
  pool_size: 100,
  pool_count: 1

config :ex_aliyun_ots,
  instances: [:my_instance],
  debug: false,
  enable_tunnel: false
```

* `debug`, optional, specifies whether to enable debug logger, by default it is false, and please DO NOT use debug mode in production.
* `enable_tunnel`, optional, specifies whether to enable tunnel functions, there will startup tunnel related supervisor and registry when enable it, by default it is false.
* `pool_size`, optional, number of connections to maintain in each pool, involved when use `Finch` as Tesla http adapter, see `Finch.request/6` for details, defaults to 100.
* `pool_count`, optional, number of pools to start, involved when use `Finch` as Tesla http adapter, see `Finch.request/6` for details, defaults to 1.


## Using ExAliyunOts

To use `ExAliyunOts`, a module that calls `use ExAliyunOts` has to be defined:

```elixir
defmodule MyApp.TableStore do
  use ExAliyunOts, instance: :my_instance
end
```

This automatically defines some macros and functions in the `MyApp.TableStore` module, here are some examples:

```elixir
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
```

## ExAliyunOts API

There are two ways to use ExAliyunOts:

* using macros and functions from your own ExAliyunOts module, like `MyApp.TableStore`.
* using macros and functions from the `ExAliyunOts` module.

All defined functions and macros in `ExAliyunOts` are available and referable for your own ExAliyunOts module as well, except that the given arity of functions may different, because the `instance` parameter of each invoke request is NOT needed from your own ExAliyunOts module although the `ExAliyunOts` module defines it.


## Ecto Adapter

Here is a [Tablestore adapter for Ecto](https://hex.pm/packages/ecto_tablestore) implementation as an alternative.

## Supported Functions

* Table Operations
* Row Operations
* Conditional update
* Auto-increment function of the primary key column
* Filter
* Atomic counters
* Search index
* Local transaction
* Tunnel service
* Timeline mode

## Supported API


* Table
  * CreateTable
  * ListTable
  * DeleteTable
  * UpdateTable
  * DescribeTable
  * ComputeSplitPointsBySize

* Row
  * PutRow
  * GetRow
  * UpdateRow
  * DeleteRow
  * GetRange
  * BatchGetRow
  * BatchWriteRow

* SearchIndex
  * CreateSearchIndex
  * DescribeSearchIndex
  * DeleteSearchIndex
  * Search
  * ParallelScan

* LocalTransaction
  * StartLocalTransaction
  * CommitTransaction
  * AbortTransaction

* SDK built-in
  * IterateAllRange
  * IterateParallelScan
  * Sequence
  * StreamParallelScan
  * StreamRange

* Tunnel service
* Timeline mode

## Thanks

Thanks very much for the help and support of the Alibaba TableStore offical support team.

## References

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

## License

This project is licensed under the MIT license. Copyright (c) 2018- Xin Zou.
