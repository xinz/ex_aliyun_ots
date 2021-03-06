Code.require_file("crc.ex", "./benchmark/3a4a442")
Code.require_file("crc.ex", "./benchmark/938fdd9")

defmodule Crc.Benchmark do
  require Integer

  def benchmark do

    string = "dasdsADAsEQWQWEQWEDASdasdASDAsDsafW312测试汉字123312312312312312312312测试汉字123312312312312312312312测试汉字123312312312312312312312测试汉字"

    Benchee.run(
      %{
        "crc_int8 with :lists.nth"    => fn -> ExAliyunOts.Commit3a4a442.CRC.crc_int8(0, 109) end,
        "crc_int8(Commit latest)  latest with :binary.at"    => fn -> ExAliyunOts.CRC.crc_int8(0, 109) end,
        "crc_int8(Commit 938fdd9) with :binary.at"    => fn -> ExAliyunOts.Commit938fdd9.CRC.crc_int8(0, 109) end,
      },
      time: 10,
      print: [fast_warning: false]
    )
    
    Benchee.run(
      %{
        "crc_int32 with :lists.nth"    => fn -> ExAliyunOts.Commit3a4a442.CRC.crc_int32(0, 2) end,
        "crc_int32(Commit latest)  with :binary.at"    => fn -> ExAliyunOts.CRC.crc_int32(0, 2) end,
        "crc_int32(Commit 938fdd9) with :binary.at"    => fn -> ExAliyunOts.Commit938fdd9.CRC.crc_int32(0, 2) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "crc_int64 with :lists.nth"    => fn -> ExAliyunOts.Commit3a4a442.CRC.crc_int64(0, 2) end,
        "crc_int64(Commit latest)  with :binary.at"    => fn -> ExAliyunOts.CRC.crc_int64(0, 2) end,
        "crc_int64(Commit 938fdd9) with :binary.at"    => fn -> ExAliyunOts.Commit938fdd9.CRC.crc_int64(0, 2) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

    Benchee.run(
      %{
        "crc_string with :lists.nth"  => fn -> ExAliyunOts.Commit3a4a442.CRC.crc_string(0, string) end,
        "crc_string with(Commit latest) :binary.at"  => fn -> ExAliyunOts.CRC.crc_string(0, string) end,
      },
      time: 10,
      memory_time: 2,
      print: [fast_warning: false]
    )

  end

end

Crc.Benchmark.benchmark()
