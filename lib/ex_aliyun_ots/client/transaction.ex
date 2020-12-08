defmodule ExAliyunOts.Client.Transaction do
  @moduledoc false

  alias ExAliyunOts.{PlainBuffer, Http}

  alias ExAliyunOts.TableStore.{
    StartLocalTransactionRequest,
    StartLocalTransactionResponse,
    CommitTransactionRequest,
    CommitTransactionResponse,
    AbortTransactionRequest,
    AbortTransactionResponse
  }

  import ExAliyunOts.Logger, only: [debug: 1]

  defp request_to_start_local_transaction(var_start_transaction) do
    table_name = var_start_transaction.table_name
    partition_key = PlainBuffer.serialize_primary_keys([var_start_transaction.partition_key])

    %StartLocalTransactionRequest{table_name: table_name, key: partition_key}
    |> StartLocalTransactionRequest.encode!()
    |> IO.iodata_to_binary()
  end

  def remote_start_local_transaction(instance, var_start_local_transaction) do
    request_body = request_to_start_local_transaction(var_start_local_transaction)

    result =
      instance
      |> Http.client(
        "/StartLocalTransaction",
        request_body,
        &StartLocalTransactionResponse.decode!/1
      )
      |> Http.post()

    debug(fn -> ["start_local_transaction result: ", inspect(result)] end)

    result
  end

  def remote_commit_transaction(instance, transaction_id) do
    request_body =
      %CommitTransactionRequest{transaction_id: transaction_id}
      |> CommitTransactionRequest.encode!()
      |> IO.iodata_to_binary()

    result =
      instance
      |> Http.client("/CommitTransaction", request_body, &CommitTransactionResponse.decode!/1)
      |> Http.post()

    debug(fn -> ["commit_transaction result: ", inspect(result)] end)

    result
  end

  def remote_abort_transaction(instance, transaction_id) do
    request_body =
      %AbortTransactionRequest{transaction_id: transaction_id}
      |> AbortTransactionRequest.encode!()
      |> IO.iodata_to_binary()

    result =
      instance
      |> Http.client("/AbortTransaction", request_body, &AbortTransactionResponse.decode!/1)
      |> Http.post()

    debug(fn -> ["abort_transaction result: ", inspect(result)] end)

    result
  end
end
