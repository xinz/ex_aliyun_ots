defmodule ExAliyunOtsTest.MergeCompile do
  use ExUnit.Case

  test "ExAliyunOts main module external resource" do

    ExAliyunOts.__info__(:attributes)[:external_resource]
    |> Enum.map(fn(file) ->
      assert File.exists?(file) == true
    end)

  end
end
