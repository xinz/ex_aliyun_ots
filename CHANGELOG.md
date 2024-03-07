# Changelog

## v0.15.2 (2024-03-07)

* Add create and delete operations to the predefined columns ([#52](https://github.com/xinz/ex_aliyun_ots/issues/52)).

## v0.15.1 (2024-01-24)

* Fix compile error when use `filter` ([#49](https://github.com/xinz/ex_aliyun_ots/pull/49)).

## v0.15.0 (2024-01-22)

* Add ValueTransferRule of SingleColumnValueFilter implemention ([#38](https://github.com/xinz/ex_aliyun_ots/pull/38)).
* Enhance SearchIndex implementions about virtual field, group by histogram, aggregation with percentiles/top rows ([#39](https://github.com/xinz/ex_aliyun_ots/pull/39)).
* Fix score sort of SearchIndex in runtime error ([#40](https://github.com/xinz/ex_aliyun_ots/pull/40)).
* Add SQL query implemention ([#41](https://github.com/xinz/ex_aliyun_ots/pull/41)).
* Enhance to simplify `condition` use ([#43](https://github.com/xinz/ex_aliyun_ots/pull/43)).
* Fix to decode tunnel record `UPDATE_ROW` action with delete/delete_all attribute cols ([#44](https://github.com/xinz/ex_aliyun_ots/pull/44), [#47](https://github.com/xinz/ex_aliyun_ots/pull/47)).
* Fix to properly stop tunnel worker ([#45](https://github.com/xinz/ex_aliyun_ots/pull/45)).
* Add local transaction setting when create table. ([#46](https://github.com/xinz/ex_aliyun_ots/pull/46)).

## v0.14.4 (2021-12-14)

* Fix compile error with Elixir 1.13.0

## v0.14.3 (2021-10-14)

* Simplify dependency by removing `:timex`.

## v0.14.2 (2021-07-22)

* Implement `analyzer_parameter` when use search index.
* Remove uesless logger when generate next proper value in sequence.

## v0.14.1 (2021-05-20)

* Add `ExAliyunOts.Search.range_query/1` to provide an intuitive experssion for use.
* Update `ExAliyunOts.Search.range_query/2` to support use a range (in first..last format).
* Simplify the internal config module.
* Fix compile deprecation warning about Bitwse ^^^ in Elixir 1.12.0 version.

## v0.14.0 (2021-04-22)

* Make tunnel service usage sample and document more clear, see `ExAliyunOts.Tunnel.Worker`.
* Rename `ExAliyunOts.Tunnel.Worker`'s `start/2` function as `start_connect/2`,
  this could resolve potential obscurity with GenServer function naming.

## v0.13.1 (2021-04-20)

* Imporve further plainbuffer encoding(cover `crc_int32` and `crc_int64`) performance

## v0.13.0 (2021-04-20)

* Enhance CRC module to imporve plainbuffer encoding performance

## v0.12.4 (2021-04-12)

* Add `:is_atomic` option to `ExAliyunOts.batch_write/3`
* Tweak document of `ExAliyunOts` module

## v0.12.3 (2021-03-02)

* Make compatible to use :crypto mac

## v0.12.2 (2021-01-21)

* Add `ExAliyunOts.stream_search/4`
* Add `ExAliyunOts.iterate_search/4`
* Fix compile warning when generate docs

## v0.12.1 (2021-01-15)

* Fix Elixir 1.11 compilation warnings, explicitly add `Jason` into mix.exs deps

## v0.12.0 (2021-01-15)

* Fix Elixir 1.11 compilation warnings

## v0.11.1 (2021-01-12)

* Remove internal error logger print when occurs `OTSConditionCheckFail`, `OTSObjectAlreadyExist` and `OTSObjectNotExist`

## v0.11.0 (2020-12-10)

**NOTICE**: Since this version changes, when upgrade this library to `0.11.0` or higher version, if also use
`ecto_tablestore`, please upgrade `ecto_tablestore` to `0.8.0` or higher version.

* Use `protox` to replace `exportobuf` for google protobuf library, and maintain the generated modules in [hexpm](https://hex.pm/packages/tablestore_protos)
* Add retry for http request timeout
* Some implicit modules are removed and directly use the generated protobuf modules, cover:
    - Use ExAliyunOts.TableStoreFilter.Filter(in `tablestore_protos`) to replace `ExAliyunOts.Var.Filter`
    - Use ExAliyunOts.TableStoreFilter.SingleColumnValueFilter(in `tablestore_protos`) to replace `ExAliyunOts.Var.SingleColumnValueFilter`
    - Use ExAliyunOts.TableStoreFilter.ColumnPaginationFilter(in `tablestore_protos`) to replace `ExAliyunOts.Var.ColumnPaginationFilter`
    - Use ExAliyunOts.TableStore.Condition(in `tablestore_protos`) to replace `ExAliyunOts.Var.Condition`
* Improve module file struct/naming
* Add `ExAliyunOts.create_index/6` to create global secondary indexes
* Add `index_metas` and `defined_columns` optional options when use `ExAliyunOts.create_table/4`

## v0.10.0 (2020-12-04)
* Improve plainbuffer decoding performance

## v0.9.0 (2020-11-17)
* [Http] Use Tesla with Finch adapter

## v0.8.0 (2020-11-13)
* [Table] Add `ExAliyunOts.compute_split_points_by_size/3`
* [Table] Fix `shard_splits` of DescribeTable response in proper decoded format
* [SearchIndex] Add ParallelScan functions
	- Implement standard api `ExAliyunOts.compute_splits/3`
	- Implement standard api `ExAliyunOts.parallel_scan/4`
	- Implement built-in functions `ExAliyunOts.iterate_parallel_scan/5`, `ExAliyunOts.iterate_parallel_scan/7` and
    `ExAliyunOts.stream_parallel_scan/4` to make general use cases simple

## v0.7.1 (2020-09-15)
* Enhance condition expression and simplify some code about it

## v0.7.0 (2020-09-15)
* Update `ExAliyunOts.filter/1` for a better usage (please notice that this change is incompatible update),
  before this version uses `filter` like this:

  ```
  filter(
    "name[ignore_if_missing: true, latest_version_only: true]" == var_name and
      "age" > 1
  )
  ```

  after this version uses `filter` like this, and then the column_name "name" can bind variable of the context

  ```
  filter(
    {"name", ignore_if_missing: true, latest_version_only: true} == var_name and
      "age" > 1
  )
  ```

## v0.6.10 (2020-09-15)
* Add `ExAliyunOts.stream_range/5`
* Add index_sorts option when create search index
* Remove Mix in runtime
* Support filter expression column_name to bind variable of the context
* Enhance condition expression
* Tweak plainbuffer
* Downgrade hackney to 1.15.2
