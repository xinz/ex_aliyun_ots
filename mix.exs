defmodule ExAliyunOts.Mixfile do
  use Mix.Project

  def project do
    [
      app: :ex_aliyun_ots,
      version: "0.6.9",
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      source_url: "https://github.com/xinz/ex_aliyun_ots",
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ExAliyunOts.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:tablestore_protos, github: "xinz/tablestore_protos"},
      {:timex, "~> 3.3"},
      {:tesla, "~> 1.3"},
      {:hackney, "~> 1.15.2"},
      {:gen_state_machine, "~> 2.0"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:credo, "~> 1.2", only: :dev, runtime: false},
      {:benchee, "~> 1.0", only: :dev, runtime: false},
      {:mock, "~> 0.3.2", only: :test},
      {:excoveralls, "~> 0.11", only: :test}
    ]
  end

  defp description do
    "Aliyun TableStore SDK for Elixir/Erlang"
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README.md", "LICENSE.md"],
      maintainers: ["Xin Zou"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/xinz/ex_aliyun_ots"}
    ]
  end

  defp docs do
    [
      main: "readme",
      formatter_opts: [gfm: true],
      extras: [
        "README.md"
      ],
      groups_for_functions: [
        "Query": & &1[:query] == :query,
        "Sort": & &1[:sort] == :sort,
        "Aggregation": & &1[:aggs] == :aggs,
        "GroupBy": & &1[:group_bys] == :group_bys,
        "Define Field Schema": & &1[:field_schema] == :field_schema,
        "Sort in GroupByField": & &1[:sort_in_group_bys] == :sort_in_group_bys,
        "Table": & &1[:table] == :table,
        "Row": & &1[:row] == :row,
        "Local Transaction": & &1[:local_transaction] == :local_transaction,
        "Search": & &1[:search] == :search,
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

end
