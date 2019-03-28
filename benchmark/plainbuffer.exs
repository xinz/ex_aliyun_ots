defmodule Plainbuffer.Benchmark do
  require Integer

  def benchmark do

    row = test_data(1..10)

    IO.puts "#{byte_size(row)}"
    Benchee.run(
      %{
        "contact with ++"          => fn -> bench_func(row, ExAliyunOts.PlainBuffer) end,
      },
      time: 10,
      print: [fast_warning: false]
    )
  end

  def bench_func(row, module) do
    module.deserialize_row(row)
  end

  defp test_data(input) do
    pk = [{"key1", "key1"}]
    attrs = Enum.map(input, fn(value) ->
      [{"string#{value}", "#{value}", 1551922128}, {"int#{value}", value, 1551922128}, {"boolean#{value}", Integer.is_even(value), 1551922128}]
    end) |> List.flatten()
    ExAliyunOts.PlainBuffer.serialize_for_put_row(pk, attrs)
  end

end

Plainbuffer.Benchmark.benchmark()
