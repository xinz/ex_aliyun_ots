defmodule ExAliyunOts.Mixfile do
  use Mix.Project

  def project do
    [
      app: :ex_aliyun_ots,
      version: "0.1.5",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      source_url: "https://github.com/xinz/ex_aliyun_ots"
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
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:exprotobuf, "~> 1.2"},
      {:timex, "~> 3.3"},
      {:poolboy, "~> 1.5"},
      {:retry, "~> 0.11.2"},
      {:mock, "~> 0.3.2", only: :test},
      {:tesla, "~> 1.2"}
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

end
