defmodule ExAliyunOts.Tunnel.Utils do
  @moduledoc false

  alias ExAliyunOts.TableStoreTunnel.{Token, TokenContent, TokenContentV2}
  alias ExAliyunOts.Logger

  def utc_now_ms() do
    DateTime.utc_now() |> DateTime.to_unix(:millisecond)
  end

  @doc """
  This check of channel type will be done from tunnel server.
  """
  @spec stream_token?(token :: String.t()) :: boolean()
  def stream_token?(token_str) do
    try do
      content = content_v2(token_str)

      case content do
        %TokenContentV2{} ->
          iterator = content.iterator
          iterator != nil and iterator != ""

        _ ->
          false
      end
    rescue
      runtime_error ->
        Logger.error(fn ->
          [
            "Check channel type occur an error: ",
            inspect(runtime_error),
            "\sfor token: ",
            inspect(token_str)
          ]
        end)

        false
    end
  end

  defp content_v2(token_str) do
    token = Token.decode(Base.decode64!(token_str))
    version = token.version

    case version do
      1 ->
        token_content = TokenContent.decode(token.content)

        TokenContentV2.new(
          primary_key: token_content.primary_key,
          iterator: token_content.iterator,
          timestamp: token_content.timestamp,
          total_count: 0
        )

      2 ->
        TokenContentV2.decode(token.content)

      _ ->
        Logger.error(fn ->
          [
            "Invalid token version: ",
            inspect(version),
            " found from token: ",
            inspect(token)
          ]
        end)

        nil
    end
  end
end
