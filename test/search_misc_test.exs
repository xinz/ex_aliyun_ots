defmodule ExAliyunOtsTest.SearchMisc do
  use ExUnit.Case
  alias ExAliyunOts.Client.Search

  test "term to bytes & bytes to term" do
    cases = [
      # string
      {"hello", <<3, 5, 0, 0, 0, 104, 101, 108, 108, 111>>},
      {"import com.alicloud.openservices.tablestore.core.protocol.SearchVariantType;",
       <<3, 76, 0, 0, 0, 105, 109, 112, 111, 114, 116, 32, 99, 111, 109, 46, 97, 108, 105, 99,
         108, 111, 117, 100, 46, 111, 112, 101, 110, 115, 101, 114, 118, 105, 99, 101, 115, 46,
         116, 97, 98, 108, 101, 115, 116, 111, 114, 101, 46, 99, 111, 114, 101, 46, 112, 114, 111,
         116, 111, 99, 111, 108, 46, 83, 101, 97, 114, 99, 104, 86, 97, 114, 105, 97, 110, 116,
         84, 121, 112, 101, 59>>},
      # integer / long
      {1000, <<0, -24, 3, 0, 0, 0, 0, 0, 0>>},
      {99_999_999_999, <<0, -1, -25, 118, 72, 23, 0, 0, 0>>},
      # double / float
      {1.0, <<1, 0, 0, 0, 0, 0, 0, -16, 63>>},
      {999.11235135, <<1, 75, -17, 118, 24, -26, 56, -113, 64>>},
      # boolean
      {true, <<2, 1>>},
      {false, <<2, 0>>}
    ]

    for {term, bytes} <- cases do
      assert Search.term_to_bytes(term) == bytes
      assert Search.bytes_to_term(bytes) == term
    end
  end
end
