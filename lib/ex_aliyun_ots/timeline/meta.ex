defmodule ExAliyunOts.Timeline.Meta do
  @moduledoc """
  Tablestore Timeline meta implements.
  """

  alias ExAliyunOts.{Client, Var, Utils}
  alias ExAliyunOts.Var.Search
  alias __MODULE__

  use ExAliyunOts.Constants

  import ExAliyunOts.Utils.Guards

  require Logger

  @fields_max_size 4

  defstruct instance: nil,
            table_name: "",
            index_name: "",
            index_schema: nil,
            fields: [],
            time_to_live: -1,
            identifier: nil,
            info: nil

  defmacro __using__(opts \\ []) do
    opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))

    quote do
      @initialized_meta_opts unquote(opts)

      use ExAliyunOts.Constants

      def new(options \\ []) when is_list(options) do
        options = Keyword.merge(@initialized_meta_opts, options)
        Meta.new(options)
      end

      def search(meta, options \\ []) when is_list(options) do
        ExAliyunOts.search(
          meta.instance,
          meta.table_name,
          meta.index_name,
          options
        )
      end

      defdelegate change_identifier(meta, identifier), to: Meta

      defdelegate add_field(meta, field_name, field_type), to: Meta

      defdelegate create(meta), to: Meta

      defdelegate drop(meta), to: Meta

      defdelegate change_info(meta, info), to: Meta

      defdelegate insert(meta), to: Meta

      defdelegate update(meta), to: Meta

      defdelegate read(meta, options \\ []), to: Meta

      defdelegate delete(meta), to: Meta
    end
  end

  def new(options \\ []) when is_list(options) do
    info = Keyword.get(options, :info, nil)

    if info != nil and (is_list(info) != true and is_map(info) != true),
      do:
        raise(
          ExAliyunOts.RuntimeError,
          "Fail to new timeline meta with invalid info: #{inspect(info)}, expect it is a list or map."
        )

    %__MODULE__{
      instance: Keyword.get(options, :instance),
      table_name: Keyword.get(options, :table_name),
      index_name: Keyword.get(options, :index_name),
      index_schema: Keyword.get(options, :index_schema),
      time_to_live: Keyword.get(options, :time_to_live, -1),
      identifier: Keyword.get(options, :identifier),
      info: info
    }
  end

  def change_identifier(meta, identifier) when is_list(identifier) do
    %{meta | identifier: identifier}
  end

  def change_identifier(meta, identifier) do
    raise ExAliyunOts.RuntimeError,
          "Fail to change identifier for timeline meta: #{inspect(meta)} with identifier: #{
            inspect(identifier)
          }."
  end

  def add_field(%__MODULE__{fields: fields} = meta, field_name, field_type)
      when is_atom(field_name) and length(fields) < @fields_max_size and
             is_valid_primary_key_type(field_type) do
    add_field(meta, Atom.to_string(field_name), field_type)
  end

  def add_field(%__MODULE__{fields: fields} = meta, field_name, :integer)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{meta | fields: meta.fields ++ [{field_name, PKType.integer()}]}
  end

  def add_field(%__MODULE__{fields: fields} = meta, field_name, :string)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{meta | fields: meta.fields ++ [{field_name, PKType.string()}]}
  end

  def add_field(%__MODULE__{fields: fields} = meta, field_name, :binary)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{meta | fields: meta.fields ++ [{field_name, PKType.binary()}]}
  end

  def add_field(%__MODULE__{fields: fields} = _meta, _field_name, _field_type)
      when length(fields) >= @fields_max_size do
    raise ExAliyunOts.RuntimeError,
          "Allow up to #{@fields_max_size} fields to be added, but already has #{length(fields)} fields."
  end

  def add_field(meta, field_name, field_type) do
    raise ExAliyunOts.RuntimeError,
          "Add an invalid field: `#{inspect(field_name)}`, field type: `#{inspect(field_type)}` into meta #{
            inspect(meta)
          }, please use field type as :string | :integer | :binary"
  end

  def create(%__MODULE__{
        instance: instance,
        table_name: table_name,
        index_name: index_name,
        fields: fields,
        time_to_live: time_to_live,
        index_schema: %Search.IndexSchema{field_schemas: field_schemas} = index_schema
      })
      when is_valid_string(table_name) and is_valid_string(index_name) and length(fields) > 0 and
             length(field_schemas) > 0 and is_valid_table_ttl(time_to_live) do
    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: fields,
      time_to_live: time_to_live
    }

    create_table_result = Client.create_table(instance, var_create_table)

    Logger.info(
      "Result to create table \"#{table_name}\" for timeline meta: #{inspect(create_table_result)}"
    )

    var_create_search_index = %Search.CreateSearchIndexRequest{
      table_name: table_name,
      index_name: index_name,
      index_schema: index_schema
    }

    create_search_index_result = Client.create_search_index(instance, var_create_search_index)

    Logger.info(
      "Result to create search index \"#{index_name}\" for timeline meta: #{
        inspect(create_search_index_result)
      }"
    )

    :ok
  end

  def create(%__MODULE__{
        time_to_live: time_to_live
      })
      when is_valid_table_ttl(time_to_live) == false do
    raise ExAliyunOts.RuntimeError,
          "Invalid time_to_live, please keep it as `-1` (for permanent), greater or equal to 86400 seconds"
  end

  def create(%__MODULE__{
        fields: fields
      })
      when fields == [] or length(fields) > @fields_max_size do
    raise ExAliyunOts.RuntimeError,
          "Invalid fields size as #{length(fields)}, please keep its size greater than 0 and less or equal to #{
            @fields_max_size
          }."
  end

  def create(meta) do
    raise ExAliyunOts.RuntimeError,
          "Fail to create with invalid timeline meta: #{inspect(meta)}."
  end

  def drop(%__MODULE__{instance: instance, table_name: table_name, index_name: index_name})
      when is_valid_string(table_name) and is_valid_string(index_name) do
    var_del_search_index = %Search.DeleteSearchIndexRequest{
      table_name: table_name,
      index_name: index_name
    }

    del_search_index_result = Client.delete_search_index(instance, var_del_search_index)

    Logger.info(
      "Result to delete search index \"#{index_name}\" for timeline meta table \"#{table_name}\": #{
        inspect(del_search_index_result)
      }"
    )

    del_table_result = Client.delete_table(instance, table_name)

    Logger.info(
      "Result to delete table \"#{table_name}\" for timeline meta: #{inspect(del_table_result)}"
    )

    :ok
  end

  def drop(meta) do
    raise ExAliyunOts.RuntimeError,
          "Fail to drop with invalid timeline meta: #{inspect(meta)}."
  end

  def change_info(_, info) when is_valid_input_columns(info) == false do
    raise ExAliyunOts.RuntimeError,
          "Fail to new timeline meta with invalid info: #{inspect(info)}, expect it is a list or map."
  end

  def change_info(%__MODULE__{} = meta, info) when is_valid_input_columns(info) do
    %{meta | info: info}
  end

  def change_info(meta, info) do
    raise ExAliyunOts.RuntimeError,
          "Fail to change timeline meta: #{inspect(meta)} with info: #{inspect(info)}"
  end


  def insert(%__MODULE__{
        instance: instance,
        identifier: identifier,
        table_name: table_name,
        info: info
      })
      when is_list(identifier) and is_bitstring(table_name) and is_valid_input_columns(info) do
    var_put_row = %Var.PutRow{
      table_name: table_name,
      primary_keys: identifier,
      attribute_columns: Utils.attrs_to_row(info),
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      },
      return_type: ReturnType.pk()
    }

    ExAliyunOts.Client.put_row(instance, var_put_row)
  end

  def insert(%__MODULE__{identifier: identifier}) when not is_list(identifier) do
    raise ExAliyunOts.RuntimeError,
          "Fail to insert timeline meta with invalid identifier: #{inspect(identifier)}, expect is is a list of tuple(s), e.g. [{\"id\", 1}]"
  end

  def insert(%__MODULE__{info: info}) when not is_valid_input_columns(info) do
    raise ExAliyunOts.RuntimeError,
          "Fail to insert timeline meta with invalid info: #{inspect(info)}, expect it is a map or list."
  end

  def insert(meta) do
    raise ExAliyunOts.RuntimeError,
          "Fail to insert timeline meta with invalid: #{inspect(meta)}."
  end

  def update(%__MODULE__{info: info}) when not is_valid_input_columns(info) do
    raise ExAliyunOts.RuntimeError,
          "Fail to update timeline meta with invalid info: #{inspect(info)}, expect it is a list or map."
  end

  def update(%__MODULE__{
        instance: instance,
        identifier: identifier,
        table_name: table_name,
        info: info
      })
      when is_list(identifier) and is_bitstring(table_name) and is_valid_input_columns(info) do
    var_update_row = %Var.UpdateRow{
      table_name: table_name,
      primary_keys: identifier,
      updates: %{
        OperationType.put() => Utils.attrs_to_row(info)
      },
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      }
    }

    Client.update_row(instance, var_update_row)
  end

  def read(
        %__MODULE__{instance: instance, identifier: identifier, table_name: table_name},
        options
      )
      when is_list(identifier) and is_bitstring(table_name) do
    columns_to_get = Keyword.get(options, :columns_to_get, [])

    if not is_list(columns_to_get),
      do:
        raise(
          ExAliyunOts.RuntimeError,
          "Invalid columns_to_get: #{inspect(columns_to_get)} using GetRow, expect it is a list."
        )

    var_get_row = %Var.GetRow{
      table_name: table_name,
      primary_keys: identifier,
      columns_to_get: columns_to_get
    }

    Client.get_row(instance, var_get_row)
  end

  def delete(%__MODULE__{instance: instance, identifier: identifier, table_name: table_name})
      when is_list(identifier) and is_bitstring(table_name) do
    var_delete_row = %Var.DeleteRow{
      table_name: table_name,
      primary_keys: identifier,
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      }
    }

    Client.delete_row(instance, var_delete_row)
  end
end
