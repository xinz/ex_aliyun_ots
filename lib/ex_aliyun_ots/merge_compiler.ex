defmodule ExAliyunOts.MergeCompiler do
  @moduledoc false

  @merge_modules [ExAliyunOts.DSL]

  defp base_path do
    Enum.find(Mix.Project.config_files, &(&1 =~ ~r/mix.exs/)) |> Path.dirname
  end

  defmacro __before_compile__(_env) do
    Enum.flat_map(@merge_modules, fn module ->
      file = module.__info__(:compile)[:source]

      {:ok, ast} =
        file
        |> File.read!()
        |> Code.string_to_quoted()

      file =
        to_string(file)
        |> Path.relative_to(base_path())

      {:defmodule, _c_m, [_alias, [do: {:__block__, _, defs}]]} = ast
      [quote(do: @external_resource(unquote(file))), defs]
    end)
  end
end
