defmodule ExAliyunOtsTest.Tunnel.Utils do
  use ExUnit.Case

  alias ExAliyunOts.Tunnel.Utils

  test "stream_token? with content v2" do
    token =
      "CAISmwESlAFHWFJsYzNSZmRIaHVYekUxTlRRek5EZzJOREV6TWprME1qRTFORGczTTJGbE5UUXRNRFZrTUMwME9XSTVMV0kwTkRBdE1ESTBNamMzTWpnd09HSXhYekUxTlRRek5EZzJOREV6TWprME1qRWNBUUFBQUFBQUFBQTNsZ0FBQUFBQUFBQUFBQUQvLy8vLy8vLy9mNEU9GAAgAA=="

    assert Utils.stream_token?(token) == true
  end

  test "stream_token? with content v1" do
    token = "CAISIQoddQAAAAEDBAMAAABrZXkFBgAAAAMBAAAANQogCa4gAA=="
    assert Utils.stream_token?(token) == false
  end

  test "stream_token? with invalid token" do
    token = "faketoken"
    assert Utils.stream_token?(token) == false

    # "faketoken" 's base64encode => "ZmFrZXRva2Vu"
    token = "ZmFrZXRva2Vu"
    assert Utils.stream_token?(token) == false
  end
end
