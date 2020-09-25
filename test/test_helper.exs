ExUnit.start(timeout: :infinity, seed: 0)

#instance = EDCEXTestInstance
#{:ok, %{table_names: table_names}} = ExAliyunOts.list_table(instance)
#
#Enum.each(table_names, fn table_name ->
#  case String.starts_with?(table_name, "test_") do
#    true -> ExAliyunOts.delete_table(instance, table_name)
#    _ -> :ignore
#  end
#end)
