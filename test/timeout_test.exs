defmodule ExAliyunOtsTest.TimeoutTest do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Const.PKType
  require PKType

  import Mock

  test "mock timeout" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name1 = "test_timeout_#{cur_timestamp}"
    var_create_table = %Var.CreateTable{
      table_name: table_name1,
      primary_keys: [
        {"pkey1", PKType.integer}
      ],
    }
    error_timeout = {:error, :timeout}

    with_mock Client, [:passthrough], [handle_call: fn({:create_table, _request_body}, _from, state) ->
        case Enum.random(0..10) do
          0 ->
            {:reply, "final_success", state}
          _ ->
            Logger.info "mock timeout case"
            {:reply, error_timeout, state}
        end
    end] do
      result = Client.create_table(@instance_key, var_create_table) 
      assert result == "final_success"
    end
  end

end
