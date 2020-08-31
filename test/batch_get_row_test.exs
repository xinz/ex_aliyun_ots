defmodule ExAliyunOtsTest.BatchGetRow do
  use ExUnit.Case
  require Logger

  @instance_key EDCEXTestInstance

  alias ExAliyunOts.{Var, Client}
  alias ExAliyunOts.Const.{PKType, ReturnType, RowExistence}
  require PKType
  require ReturnType
  require RowExistence

  test "batch get row" do
    cur_timestamp = Timex.to_unix(Timex.now())
    table_name1 = "test_table_batch_get_row_1_#{cur_timestamp}"

    var_create_table = %Var.CreateTable{
      table_name: table_name1,
      primary_keys: [
        {"pkey1", PKType.integer()}
      ]
    }

    result = Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    table_name2 = "test_table_batch_get_row_2_#{cur_timestamp}"

    var_create_table = %Var.CreateTable{
      table_name: table_name2,
      primary_keys: [
        {"pk2", PKType.integer()},
        {"pk2_sec", PKType.integer()}
      ]
    }

    result = Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    table_not_existed = "test_table_batch_get_row_ne_#{cur_timestamp}"

    condition = %Var.Condition{
      row_existence: RowExistence.expect_not_exist()
    }

    for partition_key <- 1..3 do
      var_put_row = %Var.PutRow{
        table_name: table_name1,
        primary_keys: [{"pkey1", partition_key}],
        attribute_columns: [
          {"product_name", "pn_#{partition_key}"},
          {"size", partition_key},
          {"value", 1.1}
        ],
        condition: condition,
        return_type: ReturnType.pk()
      }

      {:ok, _result} = Client.put_row(@instance_key, var_put_row)
    end

    for partition_key <- 10..13 do
      var_put_row = %Var.PutRow{
        table_name: table_name2,
        primary_keys: [{"pk2", partition_key}, {"pk2_sec", partition_key}],
        attribute_columns: [
          {"size", partition_key * 10},
          {"value", 10.1},
          {"name", "name_#{partition_key}"}
        ],
        condition: condition,
        return_type: ReturnType.pk()
      }

      {:ok, _result} = Client.put_row(@instance_key, var_put_row)
    end

    requests_with_not_existed_tables = [
      %Var.GetRow{
        table_name: table_name1,
        primary_keys: [[{"pkey1", 1}], [{"pkey1", 2}], [{"pkey1", 3}], [{"pkey1", 10}]]
      },
      %Var.GetRow{
        table_name: table_name2,
        primary_keys: [[{"pk2", 10}, {"pk2_sec", 10}]]
      },
      %Var.GetRow{
        table_name: table_not_existed,
        primary_keys: [{"pk2", 10}]
      }
    ]

    {:error, error} = Client.batch_get_row(@instance_key, requests_with_not_existed_tables)

    assert error.code == "OTSParameterInvalid"

    requests = [
      %Var.GetRow{
        table_name: table_name1,
        primary_keys: [[{"pkey1", 1}], [{"pkey1", 2}], [{"pkey1", 3}], [{"pkey1", 10}]]
      },
      %Var.GetRow{
        table_name: table_name2,
        primary_keys: [[{"pk2", 10}, {"pk2_sec", 10}]]
      }
    ]

    {:ok, batch_get_row_response} = Client.batch_get_row(@instance_key, requests)
    tables = batch_get_row_response.tables
    assert length(tables) == 2

    Enum.with_index(tables)
    |> Enum.map(fn {table_in_reponse, index} ->
      Enum.with_index(table_in_reponse.rows)
      |> Enum.map(fn {row_in_response, row_index} ->
        case index do
          0 ->
            # for `table_name1`
            case row_index do
              0 ->
                {pk, attrs} = row_in_response.row
                assert [{"pkey1", 1}] == pk
                assert {"product_name", "pn_1", _} = Enum.at(attrs, 0)
                assert {"size", 1, _} = Enum.at(attrs, 1)
                assert {"value", 1.1, _} = Enum.at(attrs, 2)

              1 ->
                {pk, attrs} = row_in_response.row
                assert [{"pkey1", 2}] == pk
                assert {"product_name", "pn_2", _} = Enum.at(attrs, 0)
                assert {"size", 2, _} = Enum.at(attrs, 1)
                assert {"value", 1.1, _} = Enum.at(attrs, 2)

              2 ->
                {pk, attrs} = row_in_response.row
                assert [{"pkey1", 3}] == pk
                assert {"product_name", "pn_3", _} = Enum.at(attrs, 0)
                assert {"size", 3, _} = Enum.at(attrs, 1)
                assert {"value", 1.1, _} = Enum.at(attrs, 2)

              3 ->
                assert row_in_response.row == nil

              _ ->
                :ok
            end

          1 ->
            # for `table_name2`
            assert row_index == 0
            {pk, attrs} = row_in_response.row
            assert [{"pk2", 10}, {"pk2_sec", 10}] = pk
            assert {"name", "name_10", _} = Enum.at(attrs, 0)
            assert {"size", 100, _} = Enum.at(attrs, 1)
            assert {"value", 10.1, _} = Enum.at(attrs, 2)

          2 ->
            # for `table_not_existed`
            assert row_in_response.row == nil
        end
      end)
    end)

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name1)
    assert result == :ok
    result = ExAliyunOts.Client.delete_table(@instance_key, table_name2)
    assert result == :ok
  end
end
