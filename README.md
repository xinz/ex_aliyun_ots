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

```elixir
config :ex_aliyun_ots, MyInstance
  name: "MyInstanceName",
  endpoint: "MyInstanceEndpoint",
  access_key_id: "MyAliyunRAMKeyID",
  access_key_secret: "MyAliyunRAMKeySecret"

config :ex_aliyun_ots,
  instances: [MyInstance],
  debug: false,
  enable_tunnel: false
```

* `debug`, optional, specifies whether to enable debug logger, by default it's false, and please DO NOT use debug mode in production.
* `enable_tunnel`, optional, specifies whether to enable tunnel functions, there will startup tunnel related `Supervisor` and `Registry` when enable it, by default it's false.


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

All defined functions and macros in `ExAliyunOts` are available and referrible for your own ExAliyunOts module as well, except that the given arity of functions may different, because the `instance` parameter of each invoke request is NOT needed from your own ExAliyunOts module although the `ExAliyunOts` module defines it.


## Ecto Adapter

We offer an [Tablestore adapter for Ecto](https://hex.pm/packages/ecto_tablestore) implementation as an alternative.

## Supported Functions

* Table Operations
* Row Operations
* Conditional update
* Auto-increment function of the primary key column
* Filter
* Atomic counters
* Search index
* Local transation
* Tunnel Service
* Timeline mode

## Supported API


* Table
  * CreateTable
  * ListTable
  * DeleteTable
  * UpdateTable
  * DescribeTable

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

* LocalTransaction
  * StartLocalTransaction
  * CommitTransaction
  * AbortTransaction

* SDK built-in

  * IterateAllRange
  * Sequence

* Tunnel Service
* Timeline Mode

## References

Alibaba Tablestore product official references:

* [English document](https://www.alibabacloud.com/help/doc-detail/27280.htm)
* [中文文档](https://help.aliyun.com/document_detail/27280.html)

## License

MIT
