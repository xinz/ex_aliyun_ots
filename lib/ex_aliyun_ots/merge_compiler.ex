defmodule ExAliyunOts.MergeCompiler do
  @moduledoc false

  @merge_modules [ExAliyunOts.DSL]
  @compile {:no_warn_undefined, {Mix.Project, :project_file, 0}}

  defp base_path do
    if function_exported?(Mix.Project, :project_file, 0) do
      Mix.Project.project_file() |> Path.dirname()
    else
      Enum.find(Mix.Project.config_files(), &(&1 =~ ~r/mix.exs/)) |> Path.dirname()
    end
  end

  defmacro __before_compile__(_env) do
    base_path = base_path()

    Enum.flat_map(@merge_modules, fn module ->
      file = module.__info__(:compile)[:source]

      {:ok, ast} =
        file
        |> File.read!()
        |> Code.string_to_quoted()

      file =
        to_string(file)
        |> Path.relative_to(base_path)

      {:defmodule, _c_m, [_alias, [do: {:__block__, _, defs}]]} = ast
      [quote(do: @external_resource(unquote(file))), defs]
    end)
  end
end
