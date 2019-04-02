defmodule ExAliyunOts.Protocol do
  @moduledoc false

  @api_version "2015-12-31"
  @printable_ascii_beginning_dec 32
  @printable_ascii_end_dec 126

  import ExAliyunOts.Logger, only: [debug: 1]

  def add_x_ots_to_headers(instance, uri, request_body) do
    request = %ExAliyunOts.HTTPRequest{
      instance: instance,
      uri: uri,
      body: request_body
    }
    get_x_ots_headers_to_auth(request)
  end

  def bin_to_printable(binary) do
    # see https://en.wikipedia.org/wiki/ASCII#ASCII_printable_characters
    binary
    |> :binary.bin_to_list()
    |> Enum.filter(fn(x) -> x >= @printable_ascii_beginning_dec and x <= @printable_ascii_end_dec end)
    |> List.to_string
  end

  defp get_x_ots_headers_to_auth(request) do
    md5 = :crypto.hash(:md5, request.body) |> Base.encode16 |> Base.encode64
    date = Timex.format!(Timex.now(), "%Y-%m-%dT%H:%M:%S.000Z", :strftime)
    instance = request.instance
    headers = [
      {"x-ots-date", date},
      {"x-ots-apiversion", @api_version},
      {"x-ots-instancename", instance.name},
      {"x-ots-accesskeyid", instance.access_key_id},
      {"x-ots-contentmd5", md5},
    ]
    signature = to_signature(request, headers)
    prepared_headers = headers ++ [{"x-ots-signature", signature}]
    debug(fn ->
      [
        "calculated signature: ",
        signature,
        ?\n,
        "prepared_headers: "
        | inspect(prepared_headers)
      ]
    end)
    prepared_headers
  end

  defp to_signature(request, headers) do
    headers_str = headers_to_str(headers)
    data_to_sign = "#{request.uri}\n#{request.method}\n\n#{headers_str}\n"
    debug(fn ->
      [
        "using data: ",
        inspect(data_to_sign),
        " to signature"
      ]
    end)
    Base.encode64(:crypto.hmac(:sha, request.instance.access_key_secret, data_to_sign))
  end

  defp headers_to_str(headers) do
    headers
    |> Enum.filter(fn({header_key, _header_value}) ->
      downcase = String.downcase(header_key)
      String.starts_with?(downcase, "x-ots-") and downcase != "x-ots-signature"
    end)
    |> Enum.sort(fn({k1, _v1}, {k2, _v2}) -> k1 <= k2 end)
    |> Enum.map(fn({header_key, header_value}) -> 
      "#{String.downcase(header_key)}:#{String.trim(header_value)}"
    end)
    |> Enum.join("\n")
  end

end
