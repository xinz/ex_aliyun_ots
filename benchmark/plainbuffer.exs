Code.require_file("plainbuffer.ex", "./benchmark/v1")

defmodule Plainbuffer.Benchmark do
  require Integer

  def benchmark do

    serialized_row = test_data(1..10)

    input_raw_rows = load_raw_rows(1..20)

    pks = [{"id", 1000}, {"sid", "testsid_9"}]

    Benchee.run(
      %{
        "deserialize row binary old"          => fn -> bench_deserialize_row(serialized_row, ExAliyunOts.PlainBuffer.Old) end,
        "deserialize row binary new"          => fn -> bench_deserialize_row(serialized_row, ExAliyunOts.PlainBuffer) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "serialize put_row old"           => fn -> bench_serialize_for_put_row(input_raw_rows, ExAliyunOts.PlainBuffer.Old) end,
        "serialize put_row new"           => fn -> bench_serialize_for_put_row(input_raw_rows, ExAliyunOts.PlainBuffer) end
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "serialize primary keys old"           => fn -> bench_serialize_primary_keys(pks, ExAliyunOts.PlainBuffer.Old) end,
        "serialize primary keys new"           => fn -> bench_serialize_primary_keys(pks, ExAliyunOts.PlainBuffer) end
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )
  end

  def bench_deserialize_row(row, module) do
    module.deserialize_row(row)
  end

  def bench_serialize_for_put_row({pks, attrs}, module) do
    module.serialize_for_put_row(pks, attrs)
  end

  def bench_serialize_primary_keys(pks, module) do
    module.serialize_primary_keys(pks)
  end

  defp test_data(input) do
    {pks, attrs} = load_raw_rows(input)
    ExAliyunOts.PlainBuffer.serialize_for_put_row(pks, attrs)
  end

  defp load_raw_rows(input) do
    pks = [{"key1", "key1"}]
    attrs = Enum.map(input, fn(value) ->
      [{"string#{value}", "#{value}", 1551922128}, {"int#{value}", value, 1551922128}, {"boolean#{value}", Integer.is_even(value), 1551922128}]
    end) |> List.flatten()
    {pks, attrs}
  end

end

Plainbuffer.Benchmark.benchmark()
