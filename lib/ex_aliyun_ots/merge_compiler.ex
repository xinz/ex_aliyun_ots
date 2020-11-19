defmodule ExAliyunOts.MergeCompiler do
  @moduledoc false

  @merge_modules [ExAliyunOts.DSL]

  defmacro __before_compile__(_env) do
    Enum.flat_map(@merge_modules, fn module ->
      file = module.__info__(:compile)[:source]

      {:ok, ast} =
        file
        |> File.read!()
        |> Code.string_to_quoted()

      file =
        to_string(file)
        |> String.split("/")
        |> Enum.drop_while(&(&1 != "ex_aliyun_ots"))
        |> Path.join()

      {:defmodule, _c_m, [_alias, [do: {:__block__, _, defs}]]} = ast
      [quote(do: @external_resource(unquote(file))), defs]
    end)
  end
end
