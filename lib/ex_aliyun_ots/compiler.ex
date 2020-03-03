defmodule ExAliyunOts.Compiler do
  @moduledoc false

  alias ExAliyunOts.Search

  @doc false
  defmacro __before_compile__(env) do
    instance = Module.get_attribute(env.module, :instance)

    quote do
      @instance unquote(instance)

      unquote(table_functions())
      unquote(row_functions())
      unquote(row_batch_helper_functions())
      unquote(local_transaction_functions())
      unquote(search_functions())
      unquote(search_create_index_helper_functions())
      unquote(search_helper_functions())
    end
  end

  defp table_functions() do
    quote do

      def create_table(table, pk_keys, opts \\ []) do
        ExAliyunOts.create_table(@instance, table, pk_keys, opts)
      end

      def delete_table(table) do
        ExAliyunOts.delete_table(@instance, table)
      end

      def list_table() do
        ExAliyunOts.list_table(@instance)
      end

      def update_table(table, opts \\ []) do
        ExAliyunOts.update_table(@instance, table, opts)
      end

      def describe_table(table) do
        ExAliyunOts.describe_table(@instance, table)
      end

    end
  end

  defp row_functions() do
    quote do

      def batch_get(requests) do
        ExAliyunOts.batch_get(@instance, requests)
      end

      def batch_write(requests, opts \\ []) do
        ExAliyunOts.batch_write(@instance, requests, opts)
      end

      def get_row(table, pk_keys, opts \\ []) do
        ExAliyunOts.get_row(@instance, table, pk_keys, opts)
      end

      def put_row(table, pk_keys, attrs, opts \\ []) do
        ExAliyunOts.put_row(@instance, table, pk_keys, attrs, opts)
      end

      def update_row(table, pk_keys, opts \\ []) do
        ExAliyunOts.update_row(@instance, table, pk_keys, opts)
      end

      def delete_row(table, pk_keys, opts \\ []) do
        ExAliyunOts.delete_row(@instance, table, pk_keys, opts)
      end

      def get_range(table, inclusive_start_primary_keys, exclusive_end_primary_keys, opts \\ []) do
        ExAliyunOts.get_range(@instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, opts)
      end

      def iterate_all_range(table, inclusive_start_primary_keys, exclusive_end_primary_keys, opts \\ []) do
        ExAliyunOts.iterate_all_range(@instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, opts)
      end

    end
  end

  defp row_batch_helper_functions() do
    quote do

      def get(table, pk_keys, opts \\ []) do
        ExAliyunOts.get(table, pk_keys, opts)
      end

      def write_put(pk_keys, attrs, opts \\ []) do
        ExAliyunOts.write_put(pk_keys, attrs, opts)
      end

      def write_update(pk_keys, opts \\ []) do
        ExAliyunOts.write_update(pk_keys, opts)
      end

      def write_delete(pk_keys, opts \\ []) do
        ExAliyunOts.write_delete(pk_keys, opts)
      end

    end
  end

  defp local_transaction_functions() do
    quote do

      def start_local_transaction(table, partition_key) do
        ExAliyunOts.start_local_transaction(@instance, table, partition_key)
      end

      def commit_transaction(transaction_id) do
        ExAliyunOts.commit_transaction(@instance, transaction_id)
      end

      def abort_transaction(transaction_id) do
        ExAliyunOts.abort_transaction(@instance, transaction_id)
      end

    end
  end

  defp search_functions() do
    quote do

      def create_search_index(table, index_name, opts \\ []) do
        ExAliyunOts.create_search_index(@instance, table, index_name, opts)
      end

      def search(table, index_name, opts \\ []) do
        ExAliyunOts.search(@instance, table, index_name, opts)
      end

      def list_search_index(table) do
        ExAliyunOts.list_search_index(@instance, table)
      end

      def delete_search_index(table, index_name) do
        ExAliyunOts.delete_search_index(@instance, table, index_name)
      end

      def describe_search_index(table, index_name) do
        ExAliyunOts.describe_search_index(@instance, table, index_name)
      end

    end
  end

  defp search_create_index_helper_functions() do
    quote do

      defdelegate field_schema_integer(field_name, opts \\ []), to: Search

      defdelegate field_schema_float(field_name, options \\ []), to: Search

      defdelegate field_schema_boolean(field_name, options \\ []), to: Search

      defdelegate field_schema_keyword(field_name, options \\ []), to: Search

      defdelegate field_schema_text(field_name, options \\ []), to: Search

      defdelegate field_schema_nested(field_name, options \\ []), to: Search

      defdelegate field_schema_geo_point(field_name, options \\ []), to: Search

    end
  end

  defp search_helper_functions() do
    quote do

      defdelegate match_query(field_name, text, opts \\ []), to: Search

      defdelegate match_all_query(), to: Search

      defdelegate match_phrase_query(field_name, text), to: Search

      defdelegate term_query(field_name, term), to: Search

      defdelegate terms_query(field_name, terms), to: Search

      defdelegate prefix_query(field_name, prefix), to: Search

      defdelegate range_query(field_name, opts \\ []), to: Search

      defdelegate wildcard_query(field_name, value), to: Search

      defdelegate bool_query(opts), to: Search

      defdelegate nested_query(path, query, opts \\ []), to: Search

      defdelegate geo_distance_query(field_name, distance, center_point), to: Search

      defdelegate geo_bounding_box_query(field_name, top_left, bottom_right), to: Search

      defdelegate geo_polygon_query(field_name, points), to: Search

      defdelegate exists_query(field_name), to: Search

      # Aggreation
      #
      defdelegate agg_min(agg_name, field_name, opts \\ []), to: Search

      defdelegate agg_max(agg_name, field_name, opts \\ []), to: Search

      defdelegate agg_avg(agg_name, field_name, opts \\ []), to: Search

      defdelegate agg_distinct_count(agg_name, field_name, opts \\ []), to: Search

      defdelegate agg_sum(agg_name, field_name, opts \\ []), to: Search

      defdelegate agg_count(agg_name, field_name), to: Search

      # GroupBys
      #
      defdelegate group_by_field(group_name, field_name, opts \\ []), to: Search

      defdelegate group_by_range(group_name, field_name, ranges, opts \\ []), to: Search

      defdelegate group_by_filter(group_name, filters, opts \\ []), to: Search

      defdelegate group_by_geo_distance(group_name, field_name, ranges, opts \\ []), to: Search

      # Sort
      #
      defdelegate group_key_sort(order), to: Search

      defdelegate row_count_sort(order), to: Search

      defdelegate sub_agg_sort(sub_agg_name, order), to: Search

      defdelegate pk_sort(order), to: Search

      defdelegate score_sort(order), to: Search

      defdelegate field_sort(field_name, opts \\ []), to: Search

      defdelegate geo_distance_sort(field_name, points, opts), to: Search

      defdelegate nested_filter(path, filter), to: Search

    end
  end

end
