defmodule ExAliyunOts.Client.SQL do
  @moduledoc false

  alias ExAliyunOts.Http
  alias ExAliyunOts.TableStore.{SQLQueryRequest, SQLQueryResponse}

  def remote_sql_query(instance, query) do
    request_body =
      %SQLQueryRequest{query: query} |> SQLQueryRequest.encode!() |> IO.iodata_to_binary()

    instance
    |> Http.client("/SQLQuery", request_body, &SQLQueryResponse.decode!/1)
    |> Http.post()
  end
end
