defmodule ExAliyunOts.Protocol do
  @moduledoc false

  @api_version "2015-12-31"
  @tunnel_api_version "2018-04-01"

  @printable_ascii_beginning_dec 32
  @printable_ascii_end_dec 126

  @tunnel_uris [
    "/tunnel/create",
    "/tunnel/delete",
    "/tunnel/list",
    "/tunnel/describe",
    "/tunnel/connect",
    "/tunnel/heartbeat",
    "/tunnel/shutdown",
    "/tunnel/getcheckpoint",
    "/tunnel/readrecords",
    "/tunnel/checkpoint"
  ]

  import ExAliyunOts.Logger, only: [debug: 1]

  def add_x_ots_to_headers(instance, uri, request_body) when uri in @tunnel_uris do
    md5 = :crypto.hash(:md5, request_body) |> Base.encode64()

    instance
    |> request(uri, request_body)
    |> prepare_x_ots_headers(md5, @tunnel_api_version)
  end

  def add_x_ots_to_headers(instance, uri, request_body) do
    md5 = :crypto.hash(:md5, request_body) |> Base.encode16() |> Base.encode64()

    instance
    |> request(uri, request_body)
    |> prepare_x_ots_headers(md5, @api_version)
  end

  def bin_to_printable(binary) do
    # see https://en.wikipedia.org/wiki/ASCII#ASCII_printable_characters
    binary
    |> :binary.bin_to_list()
    |> Enum.filter(fn x ->
      x >= @printable_ascii_beginning_dec and x <= @printable_ascii_end_dec
    end)
    |> List.to_string()
  end

  defp request(instance, uri, request_body) do
    %ExAliyunOts.HTTPRequest{
      instance: instance,
      uri: uri,
      body: request_body
    }
  end

  defp prepare_x_ots_headers(request, md5, api_version) do
    instance = request.instance

    headers = [
      {"x-ots-date", strftime_utc_now()},
      {"x-ots-apiversion", api_version},
      {"x-ots-instancename", instance.name},
      {"x-ots-accesskeyid", instance.access_key_id},
      {"x-ots-contentmd5", md5}
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

  defp strftime_utc_now() do
    DateTime.utc_now() |> Map.put(:microsecond, {0, 3}) |> DateTime.to_iso8601()
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

    sign_encode(request.instance.access_key_secret, data_to_sign)
  end

  # TODO: remove when we require OTP 22.1
  if Code.ensure_loaded?(:crypto) and function_exported?(:crypto, :mac, 4) do
    defp hmac_fun(digest, key), do: &:crypto.mac(:hmac, digest, key, &1)
  else
    defp hmac_fun(digest, key), do: &:crypto.hmac(digest, key, &1)
  end

  defp sign_encode(secret, data) do
    Base.encode64(hmac_fun(:sha, secret).(data))
  end

  defp headers_to_str(headers) do
    headers
    |> Enum.reduce([], fn {header_key, header_value}, acc ->
      downcase_header_key = String.downcase(header_key)

      if String.starts_with?(downcase_header_key, "x-ots-") and
           downcase_header_key != "x-ots-signature" do
        [{downcase_header_key, header_value} | acc]
      else
        acc
      end
    end)
    |> Enum.sort(fn {k1, _v1}, {k2, _v2} -> k1 <= k2 end)
    |> Enum.map(fn {header_key, header_value} ->
      "#{header_key}:#{String.trim(header_value)}"
    end)
    |> Enum.join("\n")
  end
end
