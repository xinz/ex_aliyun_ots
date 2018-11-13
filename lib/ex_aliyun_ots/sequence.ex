defmodule ExAliyunOts.Sequence do
  require Logger

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Const.{OperationType, ReturnType, RowExistence, PKType, FilterType, ComparatorType}

  require OperationType
  require ReturnType
  require RowExistence
  require PKType
  require FilterType
  require ComparatorType

  use Retry

  @primary_key_name "name"
  @value_column "value"
  @retry_delay_max 3_000 # 3 second

  def create(instance_name, sequence) do
    var_to_create = %Var.CreateTable{
      table_name: sequence.name,
      primary_keys: [{@primary_key_name, PKType.string}],
      reserved_throughput_write: sequence.reserved_throughput_write,
      reserved_throughput_read: sequence.reserved_throughput_read,
      deviation_cell_version_in_sec: sequence.deviation_cell_version_in_sec
    }
    Client.create_table(instance_name, var_to_create)
  end

  def next_value(instance_name, var_get_seq_next) do
    result = retry_while with: exponential_backoff() |> randomize() |> cap(@retry_delay_max) do
      case remote_next_value(instance_name, var_get_seq_next) do
        res = {:error, _error} ->
          {:cont, res}
        res ->
          {:halt, res}
      end
    end
    Logger.info "finally return next_value: #{inspect result}"
    result
  end

  defp remote_next_value(instance_name, var_get_seq_next) do
    var_get_row = %Var.GetRow{
      table_name: var_get_seq_next.name,
      primary_keys: [{@primary_key_name, var_get_seq_next.event}],
      columns_to_get: [@value_column]
    }
    result = Client.get_row(instance_name, var_get_row)
    case result do
      {:ok, get_row_response} ->
        row = get_row_response.row
        if row == nil do
          init_value = var_get_seq_next.starter
          new_value = init_value + var_get_seq_next.increment_offset
          filter = %Var.Filter{
            filter_type: FilterType.single_column,
            filter: %Var.SingleColumnValueFilter{
              comparator: ComparatorType.eq,
              column_name: @value_column,
              column_value: new_value,
              ignore_if_missing: true
            }
          }
          condition = %Var.Condition{
            row_existence: RowExistence.expect_not_exist,
            column_condition: filter
          }
          update(instance_name, var_get_seq_next, condition, new_value)
        else
          {_pk, [{@value_column, current_value, _last_update_timestamp}]} = row
          new_value = current_value + var_get_seq_next.increment_offset
          filter = %Var.Filter{
            filter_type: FilterType.single_column,
            filter: %Var.SingleColumnValueFilter{
              comparator: ComparatorType.eq,
              column_name: @value_column,
              column_value: current_value,
              ignore_if_missing: false
            }
          }
          condition = %Var.Condition{
            row_existence: RowExistence.expect_exist,
            column_condition: filter
          }
          update(instance_name, var_get_seq_next, condition, new_value)
        end
      {:error, error} ->
        Logger.error("** ExAliyunOts: generate the next value when query occur error: #{inspect error}, will retry it")
        {:error, error}
    end
  end

  def delete(instance_name, sequence_name) do
    Client.delete_table(instance_name, sequence_name)
  end

  def delete_event(instance_name, sequence_name, event) do
    var_to_delete = %Var.DeleteRow{
      table_name: sequence_name,
      primary_keys: [{@primary_key_name, event}],
      condition: %Var.Condition{
        row_existence: RowExistence.expect_exist
      }
    }
    Client.delete_row(instance_name, var_to_delete)
  end

  defp update(instance_name, var_get_seq_next, condition, new_value) do
    var_update_row = %Var.UpdateRow{
      table_name: var_get_seq_next.name,
      primary_keys: [{@primary_key_name, var_get_seq_next.event}],
      updates: %{
        OperationType.put => [{@value_column, new_value}]
      },
      condition: condition
    }
    result = Client.update_row(instance_name, var_update_row)
    case result do
      {:ok, _update_response} ->
        new_value
      {:error, error} ->
        Logger.error("** ExAliyunOts: generate the next value when update occur error: #{inspect error}, will retry it")
        {:error, error}
    end
  end

end
