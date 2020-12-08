defmodule ExAliyunOts.Mixfile do
  use Mix.Project

  @source_url "https://github.com/xinz/ex_aliyun_ots"

  def project do
    [
      app: :ex_aliyun_ots,
      version: "0.10.0",
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExAliyunOts.Application, []}
    ]
  end

  defp deps do
    [
      {:tablestore_protos, "~> 0.1"},
      {:finch, "~> 0.5"},
      {:gen_state_machine, "~> 2.0"},
      {:tesla, "~> 1.4"},
      {:timex, "~> 3.3"},
      {:benchee, "~> 1.0", only: :dev, runtime: false},
      {:credo, "~> 1.2", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:excoveralls, "~> 0.11", only: :test},
      {:mock, "~> 0.3.2", only: :test}
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
      links: %{
        "Changelog" => "#{@source_url}/blob/master/CHANGELOG.md",
        "GitHub" => @source_url
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      formatter_opts: [gfm: true],
      extras: [
        "README.md",
        "CHANGELOG.md"
      ],
      groups_for_functions: [
        Query: &(&1[:query] == :query),
        Sort: &(&1[:sort] == :sort),
        Aggregation: &(&1[:aggs] == :aggs),
        GroupBy: &(&1[:group_bys] == :group_bys),
        "Define Field Schema": &(&1[:field_schema] == :field_schema),
        "Sort in GroupByField": &(&1[:sort_in_group_bys] == :sort_in_group_bys),
        Table: &(&1[:table] == :table),
        Row: &(&1[:row] == :row),
        "Local Transaction": &(&1[:local_transaction] == :local_transaction),
        Search: &(&1[:search] == :search)
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
