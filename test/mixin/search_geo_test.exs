defmodule ExAliyunOts.MixinTest.SearchGeo do

  use ExUnit.Case

  @instance_key EDCEXTestInstance

  use ExAliyunOts,
    instance: @instance_key

  alias ExAliyunOtsTest.Support.SearchGeo, as: TestSupportSearchGeo

  @table "test_search_geo"
  @index "test_search_index_geo"

  setup_all do
    Application.ensure_all_started(:ex_aliyun_ots)

    #TestSupportSearchGeo.init(@instance_key, @table, @index)

    #on_exit(fn ->
    #  TestSupportSearchGeo.clean(@instance_key, @table, @index)
    #end)
  end

  test "geo_distance_query" do
    {:ok, response} =
      search @table, @index,
        search_query: [
          query: [
            type: QueryType.geo_distance,
            field_name: "location",
            distance: 500_000,
            center_point: "5,5"
          ]
        ]

    assert response.total_hits == 2

    {:ok, response} =
      search @table, @index,
        search_query: [
          query: geo_distance_query("location", 500_000, "5,5"),
          sort: [
            [type: :geo_distance, field_name: "location", order: :asc, points: ["5.14,5.21"]]
          ]
        ]

    assert response.total_hits == 2
    [row1, _] = response.rows
    {[{"id", id}], _} = row1
    assert id == "a4"

    {:ok, response2} =
      search @table, @index,
        search_query: [
          query: geo_distance_query("location", 500_000, "5,5"),
          sort: [
            geo_distance_sort("location", ["5.14,5.21"], order: :asc)
          ]
        ]

    assert response2.rows == response.rows
  end

  test "geo_bounding_box_query" do
    {:ok, response} =
      search @table, @index,
        search_query: [
          query: geo_bounding_box_query("location", "10,-10", "-10,10")
        ]
    assert response.total_hits == 6

    {:ok, response2} =
      search @table, @index,
        search_query: [
          query: [
            field_name: "location",
            type: QueryType.geo_bounding_box,
            top_left: "10,-10",
            bottom_right: "-10,10"
          ]
        ]
    assert response2.rows == response.rows
  end

  test "geo_polygon_query" do
    {:ok, response} =
      search @table, @index,
        search_query: [
          query: geo_polygon_query("location", ["11,11", "0,0", "1,5"])
        ]

    # Geolocation point falls on the edge of the specified polygon won't be in 
    # the matched case, e.g. "0,0" is not included in the following result.
    assert response.total_hits == 2
    [row1, row2] = response.rows
    {[{"id", id1}], _} = row1
    {[{"id", id2}], _} = row2
    assert id1 == "a2" and id2 == "a4"

    {:ok, response2} =
      search @table, @index,
        search_query: [
          query: [
            type: QueryType.geo_polygon,
            field_name: "location",
            points: ["11,11", "0,0", "1,5"]
          ]
        ]

    assert response2.rows == response.rows

    {:ok, response} = 
      search @table, @index,
        search_query: [
          query: geo_polygon_query("location", ["11,11", "-0.1,-0.1", "1,5"])
        ]
    assert response.total_hits == 3
    [row1, row2, row3] = response.rows
    {[{"id", id1}], _} = row1
    {[{"id", id2}], _} = row2
    {[{"id", id3}], _} = row3
    assert id1 == "a1" and id2 == "a2" and id3 == "a4"
  end
end
