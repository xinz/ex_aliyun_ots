defmodule ExAliyunOts.Mixfile do
  use Mix.Project

  def project do
    [
      app: :ex_aliyun_ots,
      version: "0.3.3",
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
      {:exprotobuf, "~> 1.2"},
      {:timex, "~> 3.3"},
      {:retry, "~> 0.11.2"},
      {:tesla, "~> 1.2"},
      {:gen_state_machine, "~> 2.0"},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false},
      {:credo, "~> 1.0", only: :dev, runtime: false},
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
    [main: "readme",
     formatter_opts: [gfm: true],
     extras: [
       "README.md"
     ]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

end
