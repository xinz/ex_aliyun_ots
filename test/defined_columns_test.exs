defmodule ExAliyunOtsTest.DefinedColumns do
  use ExUnit.Case
  require Logger
  require ExAliyunOts.Const.PKType
  alias ExAliyunOts.Var
  alias ExAliyunOts.Const.PKType

  @instance_key EDCEXTestInstance

  test "add defined columns then delete them" do
    table_name = "test_table_defind_columns_#{System.os_time(:second)}"

    var_create_table = %Var.CreateTable{
      table_name: table_name,
      primary_keys: [{"partition_key", PKType.integer()}]
    }

    result = ExAliyunOts.Client.create_table(@instance_key, var_create_table)
    assert result == :ok

    preset = %{
      "attr_binary" => %{type: :binary, validate_type: :DCT_BLOB},
      "attr_boolean" => %{type: :boolean, validate_type: :DCT_BOOLEAN},
      "attr_double" => %{type: :double, validate_type: :DCT_DOUBLE},
      "attr_int" => %{type: :integer, validate_type: :DCT_INTEGER},
      "attr_string" => %{type: :string, validate_type: :DCT_STRING}
    }

    columns = Enum.map(preset, fn {name, %{type: type}} -> {name, type} end)

    # test add
    result = ExAliyunOts.add_defined_columns(@instance_key, table_name, columns)
    assert result == :ok

    {:ok, describe_table_result} = ExAliyunOts.describe_table(@instance_key, table_name)
    assert describe_table_result.table_meta.table_name == table_name

    defined_columns = describe_table_result.table_meta.defined_column
    # validate length
    assert length(defined_columns) == 5

    # validate types
    Enum.each(defined_columns, fn %{name: name, type: defined_type} ->
      assert defined_type == get_in(preset, [name, :validate_type])
    end)

    # test delete
    result = ExAliyunOts.delete_defined_columns(@instance_key, table_name, Map.keys(preset))
    assert result == :ok

    result = ExAliyunOts.Client.delete_table(@instance_key, table_name)
    assert result == :ok
  end
end

