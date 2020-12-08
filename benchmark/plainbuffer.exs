Code.require_file("plainbuffer.ex", "./benchmark/v1")
Code.require_file("plainbuffer.ex", "./benchmark/a96291c")

defmodule Plainbuffer.Benchmark do
  require Integer

  def benchmark do

    serialized_row = test_data(1..10)

    input_raw_rows = load_raw_rows(1..20)

    pks = [{"id", 1000}, {"sid", "testsid_9"}]

    serialized_rows = <<117, 0, 0, 0, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 10, 57, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 168, 110, 133, 227, 86, 91, 5, 0, 10, 111, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 49, 7, 57, 122, 25, 16, 95, 1, 0, 0, 10, 194, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7, 57, 122, 25, 16, 95, 1, 0, 0, 10, 177, 9, 171, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 10, 12, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 112, 30, 134, 227, 86, 91, 5, 0, 10, 239, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 50, 7, 102, 122, 25, 16, 95, 1, 0, 0, 10, 67, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 7, 102, 122, 25, 16, 95, 1, 0, 0, 10, 189, 9, 189, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 10, 31, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 240, 92, 134, 227, 86, 91, 5, 0, 10, 172, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 51, 7, 118, 122, 25, 16, 95, 1, 0, 0, 10, 13, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 7, 118, 122, 25, 16, 95, 1, 0, 0, 10, 136, 9, 154, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 10, 102, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 184, 143, 134, 227, 86, 91, 5, 0, 10, 146, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 52, 7, 131, 122, 25, 16, 95, 1, 0, 0, 10, 0, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 7, 131, 122, 25, 16, 95, 1, 0, 0, 10, 227, 9, 225, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 10, 117, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 192, 225, 134, 227, 86, 91, 5, 0, 10, 77, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 53, 7, 152, 122, 25, 16, 95, 1, 0, 0, 10, 227, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 152, 122, 25, 16, 95, 1, 0, 0, 10, 123, 9, 80, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 10, 64, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 136, 20, 135, 227, 86, 91, 5, 0, 10, 55, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 54, 7, 165, 122, 25, 16, 95, 1, 0, 0, 10, 246, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 165, 122, 25, 16, 95, 1, 0, 0, 10, 227, 9, 110, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 10, 83, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 192, 94, 135, 227, 86, 91, 5, 0, 10, 101, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 55, 7, 184, 122, 25, 16, 95, 1, 0, 0, 10, 127, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 184, 122, 25, 16, 95, 1, 0, 0, 10, 17, 9, 37, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 10, 178, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 112, 149, 135, 227, 86, 91, 5, 0, 10, 192, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 56, 7, 198, 122, 25, 16, 95, 1, 0, 0, 10, 161, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 7, 198, 122, 25, 16, 95, 1, 0, 0, 10, 120, 9, 134, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 10, 161, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 120, 231, 135, 227, 86, 91, 5, 0, 10, 214, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 11, 0, 0, 0, 3, 6, 0, 0, 0, 110, 97, 109, 101, 95, 57, 7, 219, 122, 25, 16, 95, 1, 0, 0, 10, 40, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 7, 219, 122, 25, 16, 95, 1, 0, 0, 10, 138, 9, 14, 1, 3, 4, 13, 0, 0, 0, 112, 97, 114, 116, 105, 116, 105, 111, 110, 95, 107, 101, 121, 5, 9, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 10, 148, 3, 4, 2, 0, 0, 0, 105, 100, 5, 9, 0, 0, 0, 0, 40, 30, 136, 227, 86, 91, 5, 0, 10, 37, 2, 3, 4, 4, 0, 0, 0, 110, 97, 109, 101, 5, 12, 0, 0, 0, 3, 7, 0, 0, 0, 110, 97, 109, 101, 95, 49, 48, 7, 233, 122, 25, 16, 95, 1, 0, 0, 10, 220, 3, 4, 5, 0, 0, 0, 118, 97, 108, 117, 101, 5, 9, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 7, 233, 122, 25, 16, 95, 1, 0, 0, 10, 243, 9, 84>>


    Benchee.run(
      %{
        "deserialize row binary old"          => fn -> bench_deserialize_row(serialized_row, ExAliyunOts.PlainBuffer.Old) end,
        "deserialize row binary Commit_a96291c"          => fn -> bench_deserialize_row(serialized_row, ExAliyunOts.PlainBuffer.Commit_a96291c) end,
        "deserialize row binary new"          => fn -> bench_deserialize_row(serialized_row, ExAliyunOts.PlainBuffer) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "deserialize rows binary old"          => fn -> bench_deserialize_rows(serialized_rows, ExAliyunOts.PlainBuffer.Old) end,
        "deserialize rows binary Commit_a96291c"          => fn -> bench_deserialize_rows(serialized_rows, ExAliyunOts.PlainBuffer.Commit_a96291c) end,
        "deserialize rows binary new"          => fn -> bench_deserialize_rows(serialized_rows, ExAliyunOts.PlainBuffer) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "serialize put_row old"           => fn -> bench_serialize_for_put_row(input_raw_rows, ExAliyunOts.PlainBuffer.Old) end,
        "serialize put_row Commit_a96291c"           => fn -> bench_serialize_for_put_row(input_raw_rows, ExAliyunOts.PlainBuffer.Commit_a96291c) end,
        "serialize put_row new"           => fn -> bench_serialize_for_put_row(input_raw_rows, ExAliyunOts.PlainBuffer) end
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "serialize primary keys old"           => fn -> bench_serialize_primary_keys(pks, ExAliyunOts.PlainBuffer.Old) end,
        "serialize primary keys Commit_a96291c"           => fn -> bench_serialize_primary_keys(pks, ExAliyunOts.PlainBuffer.Commit_a96291c) end,
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

  def bench_deserialize_rows(rows, module) do
    module.deserialize_rows(rows)
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
