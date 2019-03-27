defmodule ExAliyunOts do

  defmacro __using__(opts \\ []) do
    opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))

    quote do

      @instance Keyword.get(unquote(opts), :instance)

      use ExAliyunOts.Constants

      import ExAliyunOts.Mixin

      def create_table(table, pk_keys, options \\ Keyword.new()) do
        execute_create_table(@instance, table, pk_keys, options)
      end

      def delete_table(table, options \\ Keyword.new()) do
        execute_delete_table(@instance, table, options)
      end

      def list_table(options \\ Keyword.new()) do
        execute_list_table(@instance, options)
      end
      
      def update_table(table, options \\ Keyword.new()) do
        execute_update_table(@instance, table, options)
      end

      def describe_table(table, options \\ Keyword.new()) do
        execute_describe_table(@instance, table, options)
      end

      def batch_get(requests, options \\ Keyword.new()) do
        execute_batch_get(@instance, requests, options)
      end

      def batch_write(requests, options \\ Keyword.new()) do
        execute_batch_write(@instance, requests, options)
      end

      def get_row(table, pk_keys, options \\ Keyword.new()) do
        execute_get_row(@instance, table, pk_keys, options)
      end

      def get(table, pk_keys, options \\ Keyword.new()) do
        execute_get(table, pk_keys, options)
      end

      def put_row(table, pk_keys, attrs, options \\ Keyword.new()) do
        execute_put_row(@instance, table, pk_keys, attrs, options)
      end

      def update_row(table, pk_keys, options \\ Keyword.new()) do
        execute_update_row(@instance, table, pk_keys, options)
      end

      def delete_row(table, pk_keys, options \\ Keyword.new()) do
        execute_delete_row(@instance, table, pk_keys, options)
      end

      def write_put(pk_keys, attrs, options \\ Keyword.new()) do
        execute_write_put(pk_keys, attrs, options)
      end

      def write_update(pk_keys, options \\ Keyword.new()) do
        execute_write_update(pk_keys, options)
      end

      def write_delete(pk_keys, options \\ Keyword.new()) do
        execute_write_delete(pk_keys, options)
      end

      def get_range(table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ Keyword.new()) do
        execute_get_range(@instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options)
      end

      def iterate_all_range(table, inclusive_start_primary_keys, exclusive_end_primary_keys, options \\ Keyword.new()) do
        execute_iterate_all_range(@instance, table, inclusive_start_primary_keys, exclusive_end_primary_keys, options)
      end

      def search(table, index_name, options \\ Keyword.new()) do
        execute_search(@instance, table, index_name, options)
      end

      def list_search_index(table, options \\ Keyword.new()) do
        execute_list_search_index(@instance, table, options)
      end

      def delete_search_index(table, index_name, options \\ Keyword.new()) do
        execute_delete_search_index(@instance, table, index_name, options)
      end

      def describe_search_index(table, index_name, options \\ Keyword.new()) do
        execute_describe_search_index(@instance, table, index_name, options)
      end

    end
  end

end
