defmodule ExAliyunOts.Timeline do
  @moduledoc """
  Tablestore Timeline model implements.
  """

  alias ExAliyunOts.{Client, Var, Utils}
  alias ExAliyunOts.Var.Search
  alias __MODULE__

  use ExAliyunOts.Constants

  import ExAliyunOts.Utils.Guards

  require Logger

  @seq_id_generation_auto :auto
  @seq_id_generation_manual :manual
  @fields_max_size 3
  @default_seq_id_col_name "sequence_id"

  defstruct instance: nil,
            table_name: "",
            index_name: "",
            index_schema: nil,
            fields: [],
            time_to_live: -1,
            seq_id_generation: nil,
            seq_id_col_name: nil,
            identifier: nil

  defmodule Entry do
    @moduledoc false
    defstruct sequence_id: nil, message: nil
  end

  defmodule BatchWrite do
    @moduledoc false
    defstruct timeline: nil, entry: nil
  end

  defmacro __using__(opts \\ []) do
    opts = Macro.prewalk(opts, &Macro.expand(&1, __CALLER__))

    quote do
      @initialized_opts unquote(opts)

      use ExAliyunOts.Constants

      import ExAliyunOts.Mixin, only: [filter: 1]

      def new(options \\ []) when is_list(options) do
        options = Keyword.merge(@initialized_opts, options)
        Timeline.new(options)
      end

      def search(timeline, options \\ []) when is_list(options) do
        ExAliyunOts.Mixin.execute_search(
          timeline.instance,
          timeline.table_name,
          timeline.index_name,
          options
        )
      end

      defdelegate change_seq_id(
                    timeline,
                    seq_id_generation_type,
                    seq_id_col_name \\ Timeline.default_seq_id_col_name()
                  ),
                  to: Timeline

      defdelegate change_identifier(timeline, identifier), to: Timeline

      defdelegate add_field(timeline, field_name, field_type), to: Timeline

      defdelegate create(timeline), to: Timeline

      defdelegate drop(timeline), to: Timeline

      defdelegate store(timeline, entry), to: Timeline

      defdelegate batch_store(writes), to: Timeline

      defdelegate scan_forward(timeline, from, to, options \\ []), to: Timeline

      defdelegate scan_backward(timeline, from, to, options \\ []), to: Timeline

      defdelegate update(timeline, entry), to: Timeline

      defdelegate get(timeline, sequence_id, options \\ []), to: Timeline

      defdelegate delete(timeline, sequence_id), to: Timeline
    end
  end

  def default_seq_id_col_name(), do: @default_seq_id_col_name

  def new(options \\ []) when is_list(options) do
    %__MODULE__{
      instance: Keyword.get(options, :instance),
      table_name: Keyword.get(options, :table_name),
      index_name: Keyword.get(options, :index_name),
      index_schema: Keyword.get(options, :index_schema),
      time_to_live: Keyword.get(options, :time_to_live, -1),
      identifier: Keyword.get(options, :identifier),
      seq_id_generation: Keyword.get(options, :seq_id_generation, @seq_id_generation_auto),
      seq_id_col_name: Keyword.get(options, :seq_id_col_name, @default_seq_id_col_name)
    }
  end

  def change_seq_id(%__MODULE__{} = timeline, type, seq_id_col_name)
      when type == @seq_id_generation_manual
      when type == @seq_id_generation_auto do
    %{timeline | seq_id_col_name: seq_id_col_name, seq_id_generation: type}
  end

  def change_seq_id(timeline, type, _seq_id_col_name) do
    raise ExAliyunOts.RuntimeError,
          "Fail to change sequence_id for timeline: #{inspect(timeline)} with sequence_id generation type: #{
            inspect(type)
          }."
  end

  def change_identifier(timeline, identifier) when is_list(identifier) do
    %{timeline | identifier: identifier}
  end

  def change_identifier(timeline, identifier) do
    raise ExAliyunOts.RuntimeError,
          "Fail to change identifier for timeline: #{inspect(timeline)} with identifier: #{
            inspect(identifier)
          }."
  end

  def add_field(%__MODULE__{fields: fields} = timeline, field_name, field_type)
      when is_atom(field_name) and length(fields) < @fields_max_size and
             is_valid_primary_key_type(field_type) do
    add_field(timeline, Atom.to_string(field_name), field_type)
  end

  def add_field(%__MODULE__{fields: fields} = timeline, field_name, :integer)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{timeline | fields: timeline.fields ++ [{field_name, PKType.integer()}]}
  end

  def add_field(%__MODULE__{fields: fields} = timeline, field_name, :string)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{timeline | fields: timeline.fields ++ [{field_name, PKType.string()}]}
  end

  def add_field(%__MODULE__{fields: fields} = timeline, field_name, :binary)
      when is_bitstring(field_name) and length(fields) < @fields_max_size do
    %{timeline | fields: timeline.fields ++ [{field_name, PKType.binary()}]}
  end

  def add_field(%__MODULE__{fields: fields} = _timeline, _field_name, _field_type)
      when length(fields) >= @fields_max_size do
    raise ExAliyunOts.RuntimeError,
          "Allow up to #{@fields_max_size} fields to be added, but already has #{length(fields)} fields."
  end

  def add_field(timeline, field_name, field_type) do
    raise ExAliyunOts.RuntimeError,
          "Add an invalid field: `#{inspect(field_name)}`, field type: `#{inspect(field_type)}` into timeline #{
            inspect(timeline)
          }, please use field type as :string | :integer | :binary"
  end

  @doc """
  For manual generation sequence_id.
  """
  def generate_sequence_id() do
    Timex.Duration.now(:microseconds)
  end

  def create(%__MODULE__{
        instance: instance,
        table_name: table_name,
        index_name: index_name,
        fields: fields,
        time_to_live: time_to_live,
        index_schema: %Search.IndexSchema{field_schemas: field_schemas} = index_schema,
        seq_id_generation: seq_id_generation,
        seq_id_col_name: seq_id_col_name
      })
      when is_valid_string(table_name) and is_valid_string(index_name) and length(fields) > 0 and
             length(field_schemas) > 0 and is_valid_table_ttl(time_to_live) do
    fields =
      if seq_id_generation == @seq_id_generation_auto do
        fields ++ [{seq_id_col_name, PKType.integer(), PKType.auto_increment()}]
      else
        fields ++ [{seq_id_col_name, PKType.integer()}]
      end

    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: fields,
      time_to_live: time_to_live
    }

    create_table_result = Client.create_table(instance, var_create_table)

    Logger.info(
      "Result to create table \"#{table_name}\" for timeline: #{inspect(create_table_result)}"
    )

    var_create_search_index = %Search.CreateSearchIndexRequest{
      table_name: table_name,
      index_name: index_name,
      index_schema: index_schema
    }

    create_search_index_result = Client.create_search_index(instance, var_create_search_index)

    Logger.info(
      "Result to create search index \"#{index_name}\" for timeline: #{
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
          "Invalid fields size as #{length(fields)}, please keep its size greater than 0 and less or equal to #{@fields_max_size}."
  end

  def create(timeline) do
    raise ExAliyunOts.RuntimeError,
          "Fail to create with invalid timeline: #{inspect(timeline)}."
  end

  def drop(%__MODULE__{instance: instance, table_name: table_name, index_name: index_name})
      when is_valid_string(table_name) and is_valid_string(index_name) do
    var_del_search_index = %Search.DeleteSearchIndexRequest{
      table_name: table_name,
      index_name: index_name
    }

    del_search_index_result = Client.delete_search_index(instance, var_del_search_index)

    Logger.info(
      "Result to delete search index \"#{index_name}\" for timeline table \"#{table_name}\": #{
        inspect(del_search_index_result)
      }"
    )

    del_table_result = Client.delete_table(instance, table_name)

    Logger.info(
      "Result to delete table \"#{table_name}\" for timeline: #{inspect(del_table_result)}"
    )

    :ok
  end

  def drop(timeline) do
    raise ExAliyunOts.RuntimeError,
          "Fail to drop with invalid timeline: #{inspect(timeline)}."
  end

  def store(
        %__MODULE__{
          instance: instance,
          identifier: identifier,
          table_name: table_name,
          seq_id_generation: @seq_id_generation_auto,
          seq_id_col_name: seq_id_col_name
        },
        %Entry{message: message}
      )
      when is_list(identifier) and is_bitstring(table_name) and is_valid_input_columns(message) do
    primary_keys = identifier ++ [{seq_id_col_name, PKType.auto_increment()}]
    do_store(instance, table_name, primary_keys, message)
  end

  def store(
        %__MODULE__{
          instance: instance,
          identifier: identifier,
          table_name: table_name,
          seq_id_generation: @seq_id_generation_manual,
          seq_id_col_name: seq_id_col_name
        },
        %Entry{message: message, sequence_id: sequence_id}
      )
      when is_list(identifier) and is_bitstring(table_name) and is_integer(sequence_id) and
             is_valid_input_columns(message) do
    primary_keys = identifier ++ [{seq_id_col_name, sequence_id}]
    do_store(instance, table_name, primary_keys, message)
  end

  def store(%__MODULE__{} = _timeline, %Entry{sequence_id: sequence_id})
      when sequence_id == nil and is_integer(sequence_id) == false do
    raise ExAliyunOts.RuntimeError,
          "Fail to store timeline with invalid sequence_id: #{inspect(sequence_id)}, expect it is an integer."
  end

  def store(%__MODULE__{identifier: identifier}, _) when not is_list(identifier) do
    raise ExAliyunOts.RuntimeError,
          "Fail to store timeline with invalid identifier: #{inspect(identifier)}, expect it is a list of tuple(s), e.g. [{\"id\", 1}]."
  end

  def store(%__MODULE__{seq_id_generation: seq_id_generation}, _)
      when seq_id_generation != @seq_id_generation_auto and
             seq_id_generation != @seq_id_generation_manual do
    raise ExAliyunOts.RuntimeError,
          "Fail to store timeline with invalid seq_id_generation: #{inspect(seq_id_generation)}, expect it to be `#{
            inspect(@seq_id_generation_auto)
          }` or `#{inspect(@seq_id_generation_manual)}`."
  end

  def store(_, %Entry{message: message}) when not is_valid_input_columns(message) do
    raise ExAliyunOts.RuntimeError,
          "Fail to store timeline with invalid message: #{inspect(message)}, expect it is a map or list."
  end

  def store(timeline, entry) do
    raise ExAliyunOts.RuntimeError,
          "Fail to store timeline with invalid timeline: #{inspect(timeline)} or invalid entry: #{
            inspect(entry)
          }."
  end

  def batch_store(%BatchWrite{timeline: timeline, entry: entry}) do
    store(timeline, entry)
  end

  def batch_store([]) do
    raise ExAliyunOts.RuntimeError,
          "Fail to batch store an empty writes."
  end

  def batch_store(writes) when is_list(writes) do
    requests =
      writes
      |> Enum.map(fn write ->
        case write do
          %BatchWrite{timeline: timeline, entry: entry} ->
            %Var.BatchWriteRequest{
              table_name: timeline.table_name,
              rows: [entry_to_row_in_batch_write(timeline, entry)]
            }

          _invalid ->
            raise ExAliyunOts.RuntimeError,
                  "Fail to batch store with invalid write: #{inspect(write)}."
        end
      end)

    instance = List.first(writes).timeline.instance
    Client.batch_write_row(instance, requests)
  end

  def batch_store(writes) do
    raise ExAliyunOts.RuntimeError,
          "Fail to batch store invalid writes: #{inspect(writes)}."
  end

  def scan_forward(timeline, from, to, options \\ [])

  def scan_forward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        from,
        :max,
        options
      ) do
    start_pks = identifier ++ [{seq_id_col_name, from}]
    end_pks = identifier ++ [{seq_id_col_name, PKType.inf_max()}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.forward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_forward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        :min,
        to,
        options
      ) do
    start_pks = identifier ++ [{seq_id_col_name, 0}]
    end_pks = identifier ++ [{seq_id_col_name, to}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.forward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_forward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        from,
        to,
        options
      )
      when is_integer(from) and is_integer(to) and from >= 0 and from < to do
    start_pks = identifier ++ [{seq_id_col_name, from}]
    end_pks = identifier ++ [{seq_id_col_name, to}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.forward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_forward(timeline, from, to, options) do
    raise ExAliyunOts.RuntimeError,
          "Fail to scan forward timeline: #{inspect(timeline)}, from: #{inspect(from)}, to: #{
            inspect(to)
          }, options: #{inspect(options)}."
  end

  def scan_backward(timeline, from, to, options \\ [])

  def scan_backward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        from,
        :min,
        options
      ) do
    start_pks = identifier ++ [{seq_id_col_name, from}]
    end_pks = identifier ++ [{seq_id_col_name, 0}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.backward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_backward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        :max,
        to,
        options
      ) do
    start_pks = identifier ++ [{seq_id_col_name, PKType.inf_max()}]
    end_pks = identifier ++ [{seq_id_col_name, to}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.backward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_backward(
        %Timeline{identifier: identifier, seq_id_col_name: seq_id_col_name} = timeline,
        from,
        to,
        options
      )
      when to >= 0 and to < from do
    start_pks = identifier ++ [{seq_id_col_name, from}]
    end_pks = identifier ++ [{seq_id_col_name, to}]

    do_scan(
      timeline.instance,
      timeline.table_name,
      Direction.backward(),
      start_pks,
      end_pks,
      options
    )
  end

  def scan_backward(timeline, from, to, options) do
    raise ExAliyunOts.RuntimeError,
          """
          Fail to scan backward timeline: #{inspect(timeline)}\n \
          from: #{inspect(from)}\n \
          to: #{inspect(to)}\n \
          options: #{inspect(options)}.
          """
  end

  def update(_timeline, %Entry{sequence_id: sequence_id}) when sequence_id == nil or is_integer(sequence_id) != true do
    raise ExAliyunOts.RuntimeError,
          "Fail to update timeline with invalid sequence_id: #{inspect sequence_id}, expect it is a integer."
  end

  def update(_timeline, %Entry{message: message}) when not is_valid_input_columns(message) do
    raise ExAliyunOts.RuntimeError,
          "Fail to update timeline with invalid message: #{inspect message}, expect it is a list or map."
  end

  def update(
        %__MODULE__{
          instance: instance,
          identifier: identifier,
          table_name: table_name,
          seq_id_col_name: seq_id_col_name
        },
        %Entry{message: message, sequence_id: sequence_id}
      )
      when is_list(identifier) and is_bitstring(table_name) and is_valid_input_columns(message) do
    primary_keys = identifier ++ [{seq_id_col_name, sequence_id}]
    do_update(instance, table_name, primary_keys, message)
  end

  def update(timeline, entry) do
    raise ExAliyunOts.RuntimeError,
          "Fail to update invalid timeline: #{inspect(timeline)}, or invalid entry: #{inspect(entry)}."
  end

  def get(
        %Timeline{
          instance: instance,
          identifier: identifier,
          table_name: table_name,
          seq_id_col_name: seq_id_col_name
        },
        sequence_id,
        options \\ []
      )
      when is_list(identifier) and is_bitstring(table_name) and is_integer(sequence_id) do
    primary_keys = identifier ++ [{seq_id_col_name, sequence_id}]
    do_get(instance, table_name, primary_keys, options)
  end

  def delete(
        %Timeline{
          instance: instance,
          identifier: identifier,
          table_name: table_name,
          seq_id_col_name: seq_id_col_name
        },
        sequence_id
      )
      when is_list(identifier) and is_bitstring(table_name) and is_integer(sequence_id) do
    primary_keys = identifier ++ [{seq_id_col_name, sequence_id}]
    do_delete(instance, table_name, primary_keys)
  end

  defp do_store(instance, table_name, primary_keys, message) do
    var_put_row = %Var.PutRow{
      table_name: table_name,
      primary_keys: primary_keys,
      attribute_columns: Utils.attrs_to_row(message),
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      },
      return_type: ReturnType.pk()
    }

    ExAliyunOts.Client.put_row(instance, var_put_row)
  end

  defp do_scan(instance, table_name, direction, start_pks, end_pks, options) do
    var_get_range = %Var.GetRange{
      table_name: table_name,
      direction: direction,
      inclusive_start_primary_keys: start_pks,
      exclusive_end_primary_keys: end_pks,
      limit: Keyword.get(options, :limit, 100),
      filter: Keyword.get(options, :filter),
      columns_to_get: Keyword.get(options, :columns_to_get, [])
    }

    next_start_primary_key = Keyword.get(options, :next_start_primary_key)
    Client.get_range(instance, var_get_range, next_start_primary_key)
  end

  defp do_update(instance, table_name, primary_keys, message) do
    var_update_row = %Var.UpdateRow{
      table_name: table_name,
      primary_keys: primary_keys,
      updates: %{
        OperationType.put => Utils.attrs_to_row(message)
      },
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      }
    }
    Client.update_row(instance, var_update_row)
  end

  defp do_get(instance, table_name, primary_keys, options) do
    columns_to_get = Keyword.get(options, :columns_to_get, [])
    if not is_list(columns_to_get), do: raise ExAliyunOts.RuntimeError, "Invalid columns_to_get: #{inspect(columns_to_get)} using GetRow, expect it is a list."
    var_get_row = %Var.GetRow{
      table_name: table_name,
      primary_keys: primary_keys,
      columns_to_get: columns_to_get
    }
    Client.get_row(instance, var_get_row)
  end

  defp do_delete(instance, table_name, primary_keys) do
    var_delete_row = %Var.DeleteRow{
      table_name: table_name,
      primary_keys: primary_keys,
      condition: %Var.Condition{
        row_existence: RowExistence.ignore
      }
    }
    Client.delete_row(instance, var_delete_row)
  end

  defp entry_to_row_in_batch_write(
         %Timeline{
           identifier: identifier,
           seq_id_generation: @seq_id_generation_auto,
           seq_id_col_name: seq_id_col_name
         },
         %Entry{message: message}
       ) do
    %Var.RowInBatchWriteRequest{
      type: OperationType.put(),
      primary_keys: identifier ++ [{seq_id_col_name, PKType.auto_increment()}],
      updates: Utils.attrs_to_row(message),
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      },
      return_type: ReturnType.pk()
    }
  end

  defp entry_to_row_in_batch_write(
         %Timeline{
           identifier: identifier,
           seq_id_generation: @seq_id_generation_manual,
           seq_id_col_name: seq_id_col_name
         },
         %Entry{sequence_id: sequence_id, message: message}
       ) do
    %Var.RowInBatchWriteRequest{
      type: OperationType.put(),
      primary_keys: identifier ++ [{seq_id_col_name, sequence_id}],
      updates: Utils.attrs_to_row(message),
      condition: %Var.Condition{
        row_existence: RowExistence.ignore()
      },
      return_type: ReturnType.pk()
    }
  end

end
