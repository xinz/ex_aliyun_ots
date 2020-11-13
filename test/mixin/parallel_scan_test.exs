defmodule ExAliyunOts.MixinTest.ParallelScan do
  use ExUnit.Case, async: false

  @instance_key EDCEXTestInstance

  use ExAliyunOts,
    instance: @instance_key

  import ExUnit.CaptureLog

  require Logger
  require Integer

  @table "test_parallelscan"
  @index "test_parallelscan_index"

  import Mock

  #
  # Notice:
  #
  # Since `test_parallelscan` table and `test_parallelscan_index` index have been manually
  # splited shard (by support team), and then its shard number is 3, its scan_query support limit `5000`,
  # so we don't delete this table and index.
  #
 
  #defp create() do
  #  create_table @table,
  #    [{"id", :string}]

  #  Process.sleep(3_000)

  #  create_search_index( 
  #    @table,
  #    @index,
  #    field_schemas: [
  #      field_schema_integer("index"),
  #      field_schema_keyword("name"),
  #      field_schema_keyword("remark"),
  #      field_schema_float("score"),
  #      field_schema_boolean("is_actived"),
  #      field_schema_keyword("tags", is_array: true)
  #    ]
  #  )
  #end

  #defp insert_test_data(size) do
  #  {part1, part2} = Enum.split_with(size, fn x -> rem(x, 2) == 0 end)

  #  tags = Jason.encode!(["a1", "b1"])

  #  remark = "51f801080f8841bda7b082a5bbd3ce9f"

  #  Enum.map(part1, fn(i) ->
  #    write_put(
  #      [{"id", "#{i}"}],
  #      [
  #        {"index", i},
  #        {"name", "test_#{i}"},
  #        {"remark", remark},
  #        {"score", 99.7},
  #        {"is_actived", true},
  #        {"tags", tags},
  #      ],
  #      condition: condition(:ignore)
  #    )
  #  end)
  #  |> Enum.chunk_every(200)
  #  |> Enum.map(fn(puts) ->
  #    batch_write(
  #      {@table, puts}
  #    )
  #  end)

  #  tags = Jason.encode!(["b2", "c2"])

  #  remark = "c4e3394b66164097be718421bc92e81e"

  #  Enum.map(part2, fn(i) ->
  #    write_put(
  #      [{"id", "#{i}"}],
  #      [
  #        {"index", i},
  #        {"name", "test2_#{i}"},
  #        {"remark", remark},
  #        {"score", 68.7},
  #        {"is_actived", false},
  #        {"tags", tags}
  #      ],
  #      condition: condition(:ignore)
  #    )
  #  end)
  #  |> Enum.chunk_every(200)
  #  |> Enum.map(fn(puts) ->
  #    batch_write(
  #      {@table, puts}
  #    )
  #  end)
  #end

  #defp batch_insert_data() do
  #  size = 1..5000
  #  Enum.chunk_every(size, 400)
  #  |> Task.async_stream(fn(size) ->
  #    insert_test_data(size)
  #  end, timeout: :infinity)
  #  |> Enum.to_list()
  #end

  #setup_all do
  #  create()
  #  batch_insert_data()
  #  Process.sleep(5_000)
  #  :ok
  #end

  test "compute splits" do
    {:ok, response} = compute_splits(@table, @index)
    assert response.session_id != nil
    splits_size = response.splits_size
    assert is_integer(splits_size) == true and splits_size >= 1
  end

  test "signal parallel scan - fetch all matched in one request" do
    {:ok, response} = compute_splits(@table, @index)
    session_id = response.session_id

    {:ok, scan_response} =
      parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 5000
        ],
        columns_to_get: :all_from_index,
        session_id: session_id
      )

    # Although the all matched data has been retrieved,
    # but the `next_token` is not nil in the first time of
    # parallel scan.
    next_token = scan_response.next_token

    assert length(scan_response.rows) == 2500
    assert next_token != nil

    {:ok, scan_response} =
      parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 5000,
          token: scan_response.next_token
        ],
        columns_to_get: :all_from_index, # optional, default it is `:all_from_index` 
        session_id: session_id
      )

    assert scan_response.next_token == nil
    assert scan_response.rows == []
  end

  defp parallel_scan_require_actived(current_parallel_id, splits_size, session_id, limit \\ 5000) do
    {:ok, response} =
      parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: limit,
          current_parallel_id: current_parallel_id,
          max_parallel: splits_size
        ],
        session_id: session_id
      )
    response
  end

  defp random_test_actived([]), do: :ignore
  defp random_test_actived(result) do
    {[{"id", id}], attrs} = Enum.random(result)

    assert List.keyfind(attrs, "is_actived", 0) == {"is_actived", true, nil}
    assert List.keyfind(attrs, "name", 0) == {"name", "test_#{id}", nil}
    assert List.keyfind(attrs, "score", 0) == {"score", 99.7, nil}
  end

  test "signal parallel scan - manual use Task" do
    {:ok, %{session_id: session_id, splits_size: splits_size}} =
      compute_splits(@table, @index)

    rows =
      0
      |> Range.new(splits_size - 1)
      |> Task.async_stream(fn(current_parallel_id) ->
        parallel_scan_require_actived(
          current_parallel_id,
          splits_size,
          session_id
        )
      end)
      |> Stream.map(fn {:ok, response} -> response end)
      |> Enum.reduce([], fn(response, acc) ->
        acc ++ response.rows
      end)

    assert length(rows) == 2500
    random_test_actived(rows)

    # explicitly drop one parallel id `2`,
    # expect to be [0, 1, 2] but it is [0, 1],
    # in this case the fetched data will miss some items from
    # the parallel_id `2`.
    rows =
      0
      |> Range.new(splits_size - 2)
      |> Task.async_stream(fn(current_parallel_id) ->
        parallel_scan_require_actived(
          current_parallel_id,
          splits_size,
          session_id
        )
      end)
      |> Stream.map(fn {:ok, response} -> response end)
      |> Enum.reduce([], fn(response, acc) ->
        acc ++ response.rows
      end)

    assert length(rows) < 2500
    random_test_actived(rows)
  end

  test "parallel scan with session expired" do
    {:ok, %{session_id: session_id, splits_size: splits_size}} =
      compute_splits(@table, @index)

    {:ok, response} =
      parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 5,
          current_parallel_id: 0,
          max_parallel: splits_size,
          alive_time: 1
        ],
        session_id: session_id
      )

    # Enforcedly make session expire may be work after 40+ seconds 
    Process.sleep(60_000)

    #{:error, error} =
    result =
      parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 5,
          current_parallel_id: 0,
          max_parallel: splits_size,
          token: response.next_token,
          alive_time: 1
        ],
        session_id: session_id
      )

    case result do
      {:error, error} ->
        assert error.code == "OTSSessionExpired"
      {:ok, response} ->
        assert length(response.rows) > 0
    end
  end

  test "stream parallel scan" do
    stream =
      stream_parallel_scan(
        @table,
        @index,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 1200
        ]
      )

    rows =
      stream
      |> Enum.reduce([], fn
        {:ok, response}, acc ->
          acc ++ response.rows
        _, acc ->
          acc
      end)

    assert length(rows) == 2500
  end

  def process_stream_parallel_scan(stream) do
    {rows, fail} =
      Enum.reduce(stream, {[], nil}, fn(response, {suc, fail}) ->
        case response do
          {:error, error} ->
            {suc, {:error, error}}
          {:ok, response} ->
            {suc ++ response.rows, fail}
        end
      end)
    if fail == nil do
      random_test_actived(rows)
      rows
    else
      fail
    end
  end

  test "iterate parallel scan with function" do
    rows =
      iterate_parallel_scan(
        @table,
        @index,
        &process_stream_parallel_scan/1,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 1000
        ],
        columns_to_get: ["is_actived", "name", "score"]
      )

    assert length(rows) == 2500 

    rows =
      iterate_parallel_scan(
        @table,
        @index,
        &process_stream_parallel_scan/1,
        scan_query: [
          query: match_query("is_actived", "true"),
          limit: 5000
        ],
        columns_to_get: ["is_actived", "name", "score"]
      )

    assert length(rows) == 2500
  end

  defmodule TmpHandler do

    alias ExAliyunOts.MixinTest.ParallelScan

    def test(stream, expected_row_size) do
      case ParallelScan.process_stream_parallel_scan(stream) do
        {:error, error} ->
          {:error, error}
        rows ->
          assert length(rows) == expected_row_size
          rows
      end
    end

  end

  test "iterate parallel scan with mfa" do

    # use `limit` to make a loop fetch with `next_token`
    iterate_parallel_scan(@table, @index,
      TmpHandler, :test, [2500],
      scan_query: [
        query: match_query("is_actived", "true"),
        limit: 1000
      ],
      columns_to_get: ["is_actived", "name", "score"]
    )

    # fetch all matched in one request
    iterate_parallel_scan(@table, @index,
      TmpHandler, :test, [2500],
      scan_query: [
        query: match_query("is_actived", "true"),
        limit: 5000
      ],
      columns_to_get: ["is_actived", "name", "score"]
    )

  end

  test "stream parallel scan with mock session expired" do

    with_mock ExAliyunOts.Client, [
      parallel_scan: fn(_, request) ->
        {
          :error,
          %ExAliyunOts.Error{
            code: "OTSSessionExpired", 
            http_status_code: 400,
            message: "ScanQuery'session is expired, please retry ComputeSplitsRequest and ScanQuery.",
            request_id: "#{request.scan_query.current_parallel_id}"
          }
        }
      end,
      compute_splits: fn(_, _, _) ->
        {
          :ok,
          %ExAliyunOts.TableStore.ComputeSplitsResponse{
            session_id: "fakesession_id",
            splits_size: 3
          }
        }
      end
    ] do

      stream =
        stream_parallel_scan(
          @table,
          @index,
          scan_query: [
            query: match_query("is_actived", "true")
          ]
        )

      [
        {:error, error1},
        {:error, error2},
        {:error, error3}
      ] = Enum.to_list(stream)

      assert error1.code == "OTSSessionExpired"
      assert error2.code == "OTSSessionExpired"
      assert error3.code == "OTSSessionExpired"
    end

    with_mock ExAliyunOts.Client, [
      parallel_scan: fn(_, req) ->
        session_expired_count = Process.get(:session_expired_count)
        if session_expired_count == nil do
          Process.put(:session_expired_count, 1)
          {
            :ok,
            %ExAliyunOts.TableStoreSearch.ParallelScanResponse{
              rows: [
                "fake1",
                "fake2",
                "fake3"
              ],
              next_token: nil
            }
          }
        else
          {
            :error,
            %ExAliyunOts.Error{
              code: "OTSSessionExpired", 
              http_status_code: 400,
              message: "ScanQuery'session is expired, please retry ComputeSplitsRequest and ScanQuery.",
              request_id: "#{req.scan_query.current_parallel_id}"
            }
          }
        end
      end,
      compute_splits: fn(_, _, _) ->
        {
          :ok,
          %ExAliyunOts.TableStore.ComputeSplitsResponse{
            session_id: "fakesession_id",
            splits_size: 3
          }
        }
      end
    ] do

      fail_count =
        @table
        |> stream_parallel_scan(
          @index,
          scan_query: [
            query: match_query("is_actived", "true")
          ]
        )
        |> Enum.reduce(0, fn 
          {:ok, response}, fail ->
            assert response.rows == ["fake1", "fake2", "fake3"]
            fail
          {:error, error}, fail ->
            assert error.code == "OTSSessionExpired"
            fail + 1
        end)

      assert fail_count == 2
    end

  end

  test "stream parallel scan with compute_splits fail" do
    error_code = "UnknownComputeSplitsError"
    with_mock ExAliyunOts.Client, [
      compute_splits: fn(_, _, _) ->
        {
          :error,
          %ExAliyunOts.Error{
            code: error_code,
            http_status_code: 400,
            message: "",
            request_id: "",
          }
        }
      end
    ] do
      result =
        stream_parallel_scan(
          @table,
          @index,
          scan_query: [
            query: match_query("is_actived", "true")
          ]
        )

      [{:error, error}] = Enum.to_list(result)
      assert error.code == error_code
    end
  end

  def iterate_stream(stream) do
    Enum.map(stream, fn
      {:ok, response} ->
        response
      {:error, error} ->
        error
    end)
  end

  test "iterate parallel scan with unknown error" do
    unknown = "UnknownParallelScanError"
    with_mock ExAliyunOts.Client, [
      parallel_scan: fn(_, request) ->
        id = request.scan_query.current_parallel_id + 1
        {
          :error,
          %ExAliyunOts.Error{
            code: unknown,
            http_status_code: 400,
            message: "",
            request_id: "fake_id#{id}"
          }
        }
      end,
      compute_splits: fn(_, _, _) ->
        {
          :ok,
          %ExAliyunOts.TableStore.ComputeSplitsResponse{
            session_id: "fakesession_id",
            splits_size: 3
          }
        }
      end
    ] do

      errors =
        iterate_parallel_scan(@table, @index,
          &iterate_stream/1,
          scan_query: [
            query: match_query("is_actived", "unknown"),
            limit: 1000,
            alive_time: 3
          ]
        )

      assert length(errors) == 3

      Enum.map(errors, fn(error) -> 
        assert error.code == unknown
      end)
    end
  end

  test "iterate parallel scan with mock session expired in the beginning processing" do

    with_mock ExAliyunOts.Client, [
      parallel_scan: fn(_, request) ->
        session_expired_count = Process.get(:session_expired_count)
        # mock for first time only
        if session_expired_count == nil do
          Process.put(:session_expired_count, 1)
          {
            :error,
            %ExAliyunOts.Error{
              code: "OTSSessionExpired", 
              http_status_code: 400,
              message: "ScanQuery'session is expired, please retry ComputeSplitsRequest and ScanQuery.",
              request_id: ""
            }
          }
        else
          id = request.scan_query.current_parallel_id + 1
          {
            :ok,
            %ExAliyunOts.TableStoreSearch.ParallelScanResponse{
              rows: [
                {
                  [{"id", id}],
                  [
                    {"is_actived", true, nil},
                    {"name", "test_#{id}", nil},
                    {"score", 99.7, nil}
                  ]
                }
              ],
              next_token: nil
            }
          }
        end
      end,
      compute_splits: fn(_, _, _) ->
        {
          :ok,
          %ExAliyunOts.TableStore.ComputeSplitsResponse{
            session_id: "fakesession_id",
            splits_size: 3
          }
        }
      end
    ] do

      capture_log(fn ->
        data =
          iterate_parallel_scan(@table, @index,
            TmpHandler, :test, [3],
            scan_query: [
              query: match_query("is_actived", "unknown"),
              limit: 1000,
              alive_time: 3
            ],
            columns_to_get: ["is_actived", "name", "score"]
          )

        assert length(data) == 3
        Enum.map(data, fn({[{"id", id}], attrs}) ->
          assert List.keyfind(attrs, "is_actived", 0) == {"is_actived", true, nil}
          assert List.keyfind(attrs, "name", 0) == {"name", "test_#{id}", nil}
          assert List.keyfind(attrs, "score", 0) == {"score", 99.7, nil}
        end)
      end) =~ "scan_query session expired, will renew a parallelscan task."

    end
  end

  test "iterate parallel scan with mock session expired in the middle processing" do

    with_mock ExAliyunOts.Client, [
      parallel_scan: fn(_, request) ->
        session_expired_count = Process.get(:session_expired_count)
        id = request.scan_query.current_parallel_id + 1
        case session_expired_count do
          nil ->
            Process.put(:session_expired_count, 1)
            {
              :ok,
              %ExAliyunOts.TableStoreSearch.ParallelScanResponse{
                rows: [
                  {
                    [{"id", id}],
                    [
                      {"is_actived", true, nil},
                      {"name", "test_#{id}", nil},
                      {"score", 99.7, nil}
                    ]
                  }
                ],
                next_token: "fake_next_token"
              }
            }
          1 ->
            Process.put(:session_expired_count, 2)
            {
              :error,
              %ExAliyunOts.Error{
                code: "OTSSessionExpired", 
                http_status_code: 400,
                message: "ScanQuery'session is expired, please retry ComputeSplitsRequest and ScanQuery.",
                request_id: ""
              }
            }
          2 ->
            {
              :ok,
              %ExAliyunOts.TableStoreSearch.ParallelScanResponse{
                rows: [
                  {
                    [{"id", id}],
                    [
                      {"is_actived", true, nil},
                      {"name", "test_#{id}", nil},
                      {"score", 99.7, nil}
                    ]
                  }
                ],
                next_token: nil
              }
            }
        end
      end,
      compute_splits: fn(_, _, _) ->
        {
          :ok,
          %ExAliyunOts.TableStore.ComputeSplitsResponse{
            session_id: "fakesession_id",
            splits_size: 3
          }
        }
      end
    ] do

      capture_log(fn ->
        data =
          iterate_parallel_scan(@table, @index,
            TmpHandler, :test, [3],
            scan_query: [
              query: match_query("is_actived", "unknown"),
              limit: 1000,
              alive_time: 3
            ],
            columns_to_get: ["is_actived", "name", "score"]
          )

        assert length(data) == 3
        Enum.map(data, fn({[{"id", id}], attrs}) ->
          assert List.keyfind(attrs, "is_actived", 0) == {"is_actived", true, nil}
          assert List.keyfind(attrs, "name", 0) == {"name", "test_#{id}", nil}
          assert List.keyfind(attrs, "score", 0) == {"score", 99.7, nil}
        end)
      end) =~ "scan_query session expired, will renew a parallelscan task."

    end
  end

end
