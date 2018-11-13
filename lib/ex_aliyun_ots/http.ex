defmodule ExAliyunOts.Http.Middleware do
  @behaviour Tesla.Middleware

  require Logger

  @moduledoc """
  Config and setup http request

  ### Example usage
  ```
  defmodule MyClient do
    use Tesla

    ExAliyunOts.Http.Middleware, instance: instance, uri: uri, request_body: request_body, decoder: decoder
  end
  ```

  ### Required
  - `:instance` - ots instance
  - `:uri` - ots request uri
  - `:request_body` - the body of post request
  - `:decoder` - the corrresonding decoder to response

  ### Option
  - `:deserialize_row` - need to deserialize row from response
  """

  alias ExAliyunOts.{PlainBuffer, Protocol}

  def call(env, next, options) do
    instance = Keyword.get(options, :instance)
    uri = Keyword.get(options, :uri)
    request_body = Keyword.get(options, :request_body)
    decoder = Keyword.get(options, :decoder)
    require_deserialize_row = Keyword.get(options, :deserialize_row, false)

    env
    |> prepare_request(instance, uri, request_body)
    |> Tesla.run(next)
    |> decode_response(decoder, require_deserialize_row)
  end

  defp prepare_request(env, instance, uri, request_body) do
    headers = Protocol.add_x_ots_to_headers(instance, uri, request_body)
    env
    |> Tesla.put_headers(headers)
    |> Map.put(:url, instance.endpoint <> uri)
    |> Map.put(:body, request_body)
  end

  defp decode_response({:ok, response}, nil, _require_deserialize_row) do
    if response.status == 200 do
      :ok
    else
      error_msg = Protocol.bin_to_printable(response.body)
      log_error_keyinfo(response.headers, error_msg)
      {:error, error_msg}
    end
  end
  defp decode_response({:ok, response}, decoder, require_deserialize_row) do
    if response.status == 200 do
      readable_result =
        if require_deserialize_row == true do
          make_response_row_readable(decoder.(response.body))
        else
          decoder.(response.body)
        end
      {:ok, readable_result}
    else
      error_msg = Protocol.bin_to_printable(response.body)
      log_error_keyinfo(response.headers, error_msg)
      {:error, error_msg}
    end
  end
  defp decode_response({:error, reason}, _decoder, _require_deserialize_row) do
    {:error, reason}
  end

  defp make_response_row_readable(decoded) do
    bytes_row = decoded.row
    if bytes_row != nil do
      readable_row = PlainBuffer.deserialize_row(bytes_row)
      Map.put(decoded, :row, readable_row)
    else
      decoded
    end
  end

  defp log_error_keyinfo([], error_msg) do
    Logger.error(fn -> "** ExAliyunOts Error: #{inspect error_msg}, and response with empty headers" end)
  end
  defp log_error_keyinfo(response_headers, error_msg) do
    keyinfo =
      Enum.reduce(response_headers, "", fn({key, value}, acc) ->
        cond do
          "x-ots-requestid" == key ->
            acc <> "RequestID: #{inspect value} "
          "x-ots-date" == key ->
            acc <> "Date: #{inspect value} "
          true ->
            acc
        end
      end)
    Logger.error(fn -> "** ExAliyunOts Error: #{inspect error_msg} #{keyinfo}" end)
  end

end


defmodule ExAliyunOts.Http do
  use Tesla

  plug Tesla.Middleware.Retry, delay: 500, max_retries: 10

  def client(instance, "/PutRow", request_body, decoder) do
    Tesla.client([
      {ExAliyunOts.Http.Middleware, instance: instance, uri: "/PutRow", request_body: request_body, decoder: decoder, deserialize_row: true}
    ])
  end
  def client(instance, "/GetRow", request_body, decoder) do
    Tesla.client([
      {ExAliyunOts.Http.Middleware, instance: instance, uri: "/GetRow", request_body: request_body, decoder: decoder, deserialize_row: true}
    ])
  end
  def client(instance, "/UpdateRow", request_body, decoder) do
    Tesla.client([
      {ExAliyunOts.Http.Middleware, instance: instance, uri: "/UpdateRow", request_body: request_body, decoder: decoder, deserialize_row: true}
    ])
  end
  def client(instance, uri, request_body, decoder) do
    Tesla.client([
      {ExAliyunOts.Http.Middleware, instance: instance, uri: uri, request_body: request_body, decoder: decoder}
    ])
  end

  def post(client) do
    post(client, "/", nil)
  end

end
