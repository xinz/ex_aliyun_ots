# Changelog

## v0.10.0 (2020-12-04)
* Improve plainbuffer decoding performance

## v0.9.0 (2020-11-17)
* [Http] Use Tesla with Finch adapter

## v0.8.0 (2020-11-13)
* [Table] Add `ExAliyunOts.compute_split_points_by_size/2`
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
