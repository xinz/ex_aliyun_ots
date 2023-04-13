defmodule ExAliyunOtsTest.SQL do
  use ExUnit.Case
  alias ExAliyunOts.Client
  alias ExAliyunOts.TableStore.SQLQueryResponse

  @instance_key EDCEXTestInstance

  test "sql_query" do
    Client.sql_query(@instance_key, "DROP MAPPING TABLE test_parallelscan")

    # CREATE:
    #   - https://help.aliyun.com/document_detail/295892.htm
    #   - https://help.aliyun.com/document_detail/427033.html
    create_sql = """
    CREATE TABLE test_parallelscan (
      id VARCHAR(1024) PRIMARY KEY,
      is_actived BOOL,
      name MEDIUMTEXT,
      score DOUBLE,
      tags MEDIUMTEXT
    )
    """

    assert {:ok, %SQLQueryResponse{rows: []}} = Client.sql_query(@instance_key, create_sql)

    assert {:error,
            %ExAliyunOts.Error{
              code: "OTSParameterInvalid",
              message: "Table 'edc-ex-test.test_parallelscan' already exists"
            }} = Client.sql_query(@instance_key, create_sql)

    # DESCRIBE: https://help.aliyun.com/document_detail/295896.html
    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "DESCRIBE test_parallelscan")

    assert 5 = length(rows)

    # https://help.aliyun.com/document_detail/295904.html
    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "SHOW INDEX IN test_parallelscan")

    assert 5 = length(rows)

    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "SHOW INDEX FROM test_parallelscan")

    assert 5 = length(rows)

    # ALTER: https://help.aliyun.com/document_detail/437170.html
    # ALTER TABLE table_name option column_name [data_type]
    assert {:ok, %SQLQueryResponse{rows: []}} =
             Client.sql_query(@instance_key, "ALTER TABLE test_parallelscan DROP COLUMN score")

    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "DESCRIBE test_parallelscan")

    assert 4 = length(rows)

    assert {:ok, %SQLQueryResponse{rows: []}} =
             Client.sql_query(
               @instance_key,
               "ALTER TABLE test_parallelscan ADD COLUMN score DOUBLE"
             )

    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "DESCRIBE test_parallelscan")

    assert 5 = length(rows)

    # SHOW: https://help.aliyun.com/document_detail/295905.html
    assert {:ok,
            %SQLQueryResponse{rows: [{[], [{"Tables_in_edc-ex-test", "test_parallelscan", nil}]}]}} =
             Client.sql_query(@instance_key, "SHOW TABLES")

    # SELECT: https://help.aliyun.com/document_detail/295900.html
    assert {:ok, %SQLQueryResponse{rows: rows}} =
             Client.sql_query(@instance_key, "SELECT * FROM test_parallelscan LIMIT 20")

    assert 20 = length(rows)
    assert [{[], row} | _] = rows
    assert 5 = length(row)

    # DROP: https://help.aliyun.com/document_detail/295893.html
    assert {:ok, %SQLQueryResponse{rows: []}} =
             Client.sql_query(@instance_key, "DROP MAPPING TABLE test_parallelscan")

    assert {:error,
            %ExAliyunOts.Error{
              code: "OTSParameterInvalid",
              message: "Unknown mapping table 'edc-ex-test.test_parallelscan'"
            }} = Client.sql_query(@instance_key, "DROP MAPPING TABLE test_parallelscan")
  end
end
