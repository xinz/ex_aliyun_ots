# Changelog

## v0.7.1 (2020-09-15)
* Enhance condition expression and simplify some code about it

## v0.7.0 (2020-09-15)
* Update ExAliyunOts.filter/1 for a better usage (please notice that this change is incompatible update),
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
* Add ExAliyunOts.stream_range/5
* Add index_sorts option when create search index
* Remove Mix in runtime
* Support filter expression column_name to bind variable of the context
* Enhance condition expression
* Tweak plainbuffer
* Downgrade hackney to 1.15.2
