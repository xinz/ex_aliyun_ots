defmodule ExAliyunOtsTest.CRC do
  use ExUnit.Case

  test "verify crc" do
    assert ExAliyunOts.CRC.crc_string(0, "123") == 192
    assert ExAliyunOts.CRC.crc_int8(0, 1) == 7
    assert ExAliyunOts.CRC.crc_int32(0, 2) == 44
    assert ExAliyunOts.CRC.crc_int64(0, 2) == 38
    assert ExAliyunOts.CRC.crc_string(0, "abc1") == 13
  end
end
