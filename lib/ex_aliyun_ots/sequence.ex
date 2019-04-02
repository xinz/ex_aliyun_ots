defmodule ExAliyunOts.Sequence do

  alias ExAliyunOts.Const.{ReturnType, PKType}
  require ReturnType
  require PKType

  use ExAliyunOts.Mixin
  use Retry

  @primary_key_name "name"
  @value_column "value"
  @retry_delay_max 3_000 # 3 second

  import ExAliyunOts.Logger, only: [info: 1, error: 1]

  def create(instance_name, sequence) do
    create_table instance_name, sequence.name,
      [{@primary_key_name, PKType.string}],
      reserved_throughput_write: sequence.reserved_throughput_write,
      reserved_throughput_read: sequence.reserved_throughput_read,
      deviation_cell_version_in_sec: sequence.deviation_cell_version_in_sec
  end

  def next_value(instance_name, var_get_seq_next) do
    result = retry_while with: exponential_backoff() |> randomize() |> cap(@retry_delay_max) do
      case generate_next_value(instance_name, var_get_seq_next) do
        res = {:error, _error} ->
          {:cont, res}
        res ->
          {:halt, res}
      end
    end
    info(fn ->
      [
        "finally return next_value: ",
        inspect(result)
      ]
    end)
    result
  end

  def delete(instance_name, sequence_name) do
    delete_table(instance_name, sequence_name)
  end

  def delete_event(instance_name, sequence_name, event) do
    delete_row instance_name, sequence_name,
      [{@primary_key_name, event}],
      condition: condition(:expect_exist)
  end

  defp generate_next_value(instance_name, var) do
    result =
      update_row instance_name, var.name,
        [{@primary_key_name, var.event}],
        increment: [{@value_column, var.increment_offset}],
        return_type: ReturnType.after_modify,
        return_columns: [@value_column],
        condition: condition(:ignore)
    return_value(result)
  end

  defp return_value({:ok, response}) do
    {_pk, [{@value_column, value, _}]} = response.row
    value
  end
  defp return_value({:error, error}) do
    error(fn ->
      [
        "** ExAliyunOts: generate the next value when update occur error: ",
        inspect(error),
        ", will retry it."
      ]
    end)
    {:error, error}
  end

end
