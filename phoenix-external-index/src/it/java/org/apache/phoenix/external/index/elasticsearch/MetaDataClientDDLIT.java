package org.apache.phoenix.external.index.elasticsearch;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.ExternalConstants;
import org.apache.phoenix.external.index.elasticsearch.base.IndexTestUtil;
import org.apache.phoenix.external.index.elasticsearch.base.ParallelStatsDisabledIT;
import org.apache.phoenix.external.index.elasticsearch.base.TestUtil;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 * JUnit4使用Java5中的注解（annotation），以下是JUnit4常用的几个annotation：
 * @Before：初始化方法 对于每一个测试方法都要执行一次（注意与BeforeClass区别，后者是对于所有方法执行一次）
 * @After：释放资源 对于每一个测试方法都要执行一次（注意与AfterClass区别，后者是对于所有方法执行一次）
 * @Test：测试方法，在这里可以测试期望异常和超时时间
 * @Test(expected=ArithmeticException.class)检查被测方法是否抛出ArithmeticException异常
 * @Ignore：忽略的测试方法
 * @BeforeClass：针对所有测试，只执行一次，且必须为static void
 * @AfterClass：针对所有测试，只执行一次，且必须为static void
 * 一个JUnit4的单元测试用例执行顺序为：
 * @BeforeClass -> @Before -> @Test -> @After -> @AfterClass;
 * 每一个测试方法的调用顺序为：
 *
 * @Before -> @Test -> @After;
 */
public class MetaDataClientDDLIT extends ParallelStatsDisabledIT {
    private static Properties props = null;
    private RestHighLevelClient elasticClient;
    private PhoenixConnection conn;
    protected static final ConfigurationReadableOnlyWrapper configuration = ExternalUtil.getReadOnlyConfiguration(config);
    @BeforeClass
    public static void beforeStart(){
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    }
    @AfterClass
    public static void beforeStop(){
        props.clear();
    }
    @Before
    public void init() throws SQLException {
        elasticClient = ElasticSearchClient.getSingleton(configuration).getSingletonClient();
        conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE SCHEMA if not exists " + INDEX_DATA_SCHEMA);
    }
    @After
    public void close() throws SQLException {
        conn.setAutoCommit(true);
        ElasticSearchClient.getSingleton(configuration).closeSingletonClient();
        ExternalUtil.closeSilent(conn);
        
        try {
            conn.createStatement().execute("DROP SCHEMA if exists " + INDEX_DATA_SCHEMA);
        }catch (SQLException e){  }
    }
    public boolean exists(String esIndexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(esIndexName);
        return elasticClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }
    public void testCreateIndex(String schemaName) throws Exception{
        
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        
        if(schemaName!=null && !schemaName.isEmpty()){
            conn.createStatement().execute("CREATE SCHEMA if not exists " + schemaName);
        }

        String query;
        ResultSet rs;
        String ddl ="CREATE TABLE " + fullTableName
                + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) ";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);

        query="CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (v1,v2) ";

        conn.createStatement().execute(query);

        HTableDescriptor tableDescriptor = conn.getQueryServices().getTableDescriptor( Bytes.toBytes(
                SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullIndexName),conn.getQueryServices().getConfiguration())
                        .getNameWithNamespaceInclAsString()));

        assertEquals("HTableDescriptor has "+ExternalConstants.ATTRIBUTE_INDEX_TYPE +" flag"
                , PTable.IndexType.EXTERNAL,
                PTable.IndexType.valueOf(tableDescriptor.getValue(ExternalConstants.ATTRIBUTE_INDEX_TYPE)));

        assertTrue(exists(EsUtils.getEsIndexName(indexName)));

        query="drop INDEX " + indexName + " ON " + fullTableName;
        conn.createStatement().execute(query);
    
    }
    @Test
    public void testCreateIndexNonSchema() throws Exception{
        testCreateIndex(TestUtil.DEFAULT_SCHEMA_NAME);
    }
    @Test
    public void testCreateIndexWithSchema() throws Exception{
        testCreateIndex(TestUtil.INDEX_DATA_SCHEMA);
    }
    private void dropIndex(String schemaName) throws Exception{
        
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);

        if(schemaName!=null && !schemaName.isEmpty()){
            conn.createStatement().execute("CREATE SCHEMA if not exists " + schemaName);
        }

        String query;
        ResultSet rs;
        String ddl ="CREATE TABLE " + fullTableName
                + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) ";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);

        query="CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (v1,v2) ";

        conn.createStatement().execute(query);

        HTableDescriptor tableDescriptor = conn.getQueryServices().getTableDescriptor( Bytes.toBytes(
                SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullIndexName),conn.getQueryServices().getConfiguration())
                        .getNameWithNamespaceInclAsString()));
        assertEquals(tableDescriptor.getTableName().getNameWithNamespaceInclAsString(),
                SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullIndexName),conn.getQueryServices().getConfiguration())
                        .getNameWithNamespaceInclAsString());
        PTable index = conn.getTable(new PTableKey(null,fullIndexName));

        query="drop INDEX " + indexName + " ON " + fullTableName;
        conn.createStatement().executeUpdate(query);

        assertFalse(exists(EsUtils.getEsIndexName(indexName)));
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props)){
            try{
                conn.getTable(new PTableKey(null,fullIndexName));
                fail(fullIndexName+" drop failed");
            }catch (TableNotFoundException e){ }
        }

    }
    @Test
    public void testDropIndexNonSchema() throws Exception{
        dropIndex(TestUtil.DEFAULT_SCHEMA_NAME);
    }
    @Test
    public void testDropIndexWithSchema() throws Exception{
        dropIndex(TestUtil.INDEX_DATA_SCHEMA);
    }
    // 下面来自IndexMetadataIT

    @Test
    public void testIndexCreateDrop() throws Exception {
        String indexDataTable = generateUniqueName();
        String fullIndexDataTable = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        String indexName = generateUniqueName();

        String tableDDL = "create table " + fullIndexDataTable + TestUtil.TEST_TABLE_SCHEMA;
        conn.createStatement().execute(tableDDL);
        String ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullIndexDataTable
                + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                + " INCLUDE (int_col1, int_col2)";
        conn.createStatement().execute(ddl);

        TestUtil.printResult(conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false),
                indexDataTable+" metadata");
        // Verify the metadata for index is correct.
        ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 1, ":VARCHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 2, ":CHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 3, ":INT_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 4, ":LONG_PK", TestUtil.Order.DESC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 5, ":DECIMAL_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 6, ":DATE_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 7, "A:VARCHAR_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 8, "B:VARCHAR_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 9, "A:INT_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 10, "B:INT_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 11, "0:EXTERNAL_QUERY", TestUtil.Order.ASC);
        assertFalse(rs.next());

        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), StringUtil.escapeLike(indexName ), new String[] {PTableType.INDEX.getValue().getString() });
        assertTrue(rs.next());
        assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));

        // Verify that there is a row inserted into the data table for the index table.
        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(1));
        assertFalse(rs.next());

        TestUtil.assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        TestUtil.assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " USABLE";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        TestUtil.assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " DISABLE";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.DISABLE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        TestUtil.assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        try {
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " USABLE";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
        }
        try {
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
        }

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " REBUILD";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        TestUtil.assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " REBUILD ASYNC";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.BUILDING.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute(ddl);

        TestUtil.assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

        // Assert the rows for index table is completely removed.
        rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        assertFalse(rs.next());

        // Assert the row in the original data table is removed.
        // Verify that there is a row inserted into the data table for the index table.
        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
        assertFalse(rs.next());

        // Create another two indexes, and drops the table, verifies the indexes are dropped as well.
        ddl = "CREATE EXTERNAL INDEX " + indexName + "1 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable
                + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                + " INCLUDE (int_col1, int_col2)";
        conn.createStatement().execute(ddl);

        ddl = "CREATE EXTERNAL INDEX " + indexName + "2 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable
                + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                + " INCLUDE (long_pk, int_col2)";
        conn.createStatement().execute(ddl);
        rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        TestUtil.printResult(conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false),
                indexDataTable+" metadata");
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 1, ":VARCHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 2, ":CHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 3, ":INT_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 4, ":LONG_PK", TestUtil.Order.DESC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 5, ":DECIMAL_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 6, ":DATE_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 7, "A:VARCHAR_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 8, "B:VARCHAR_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 9, "A:INT_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 10, "B:INT_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"1", 11, "0:EXTERNAL_QUERY", TestUtil.Order.ASC);

        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 1, ":VARCHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 2, ":CHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 3, ":INT_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 4, ":LONG_PK", TestUtil.Order.DESC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 5, ":DECIMAL_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 6, ":DATE_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 7, "A:VARCHAR_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 8, "B:VARCHAR_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 9, "B:INT_COL2", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName +"2", 10, "0:EXTERNAL_QUERY", TestUtil.Order.ASC);
        assertFalse(rs.next());

        // Create another table in the same schema
        String diffTableNameInSameSchema = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + "2";
        conn.createStatement().execute("CREATE TABLE " + diffTableNameInSameSchema + "(k INTEGER PRIMARY KEY)");
        try {
            conn.createStatement().execute("DROP INDEX " + indexName + "1 ON " + diffTableNameInSameSchema);
            fail("Should have realized index " + indexName + "1 is not on the table");
        } catch (TableNotFoundException ignore) {

        }
        ddl = "DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute(ddl);

        rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        assertFalse(rs.next());
        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1");
        assertFalse(rs.next());
        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2");
        assertFalse(rs.next());

    }

    @Test
    public void testIndexDefinitionWithNullableFixedWidthColInPK() throws Exception {
        // If we have nullable fixed width column in the PK, we convert those types into a compatible variable type
        // column. The definition is defined in IndexUtil.getIndexColumnDataType.
        String indexDataTable = generateUniqueName();
        String indexName = generateUniqueName();
  
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true");
        String ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName
                + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                + " INCLUDE (int_col1)";
        conn.createStatement().execute(ddl);

        // Verify the CHAR, INT and LONG are converted to right type.
        // 因为外部索引把char_col1，int_col2，long_col2放到了include中，所以类型不再验证
        ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        
        TestUtil.printResult(conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false),
                indexDataTable + " metadata");
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 1, ":VARCHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 2, ":CHAR_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 3, ":INT_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 4, ":LONG_PK", TestUtil.Order.DESC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 5, ":DECIMAL_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 6, ":DATE_PK", TestUtil.Order.ASC);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 7, "A:CHAR_COL1", null, Types.CHAR);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 8, "B:INT_COL2", null,Types.INTEGER);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 9, "B:LONG_COL2", null,Types.BIGINT);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 10, "A:INT_COL1", null);
        TestUtil.assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 11, "0:EXTERNAL_QUERY", TestUtil.Order.ASC);

        assertFalse(rs.next());

        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(1));
        assertFalse(rs.next());

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals(indexName , rs.getString(3));
        assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute(ddl);

        // Assert the rows for index table is completely removed.
        rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        assertFalse(rs.next());

        // Assert the row in the original data table is removed.
        // Verify that there is a row inserted into the data table for the index table.
        rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
        assertFalse(rs.next());

    }

    @Test
    public void testAlterIndexWithLowerCaseName() throws Exception {

        String indexName = "\"lowerCaseIndex\"";
        String indexDataTable = generateUniqueName();

        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true");
        String ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName
                + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                + " INCLUDE (int_col1)";
        conn.createStatement().execute(ddl);

        ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
        conn.createStatement().execute(ddl);
        // Verify the metadata for index is correct.
        ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "lowerCaseIndex", new String[] {PTableType.INDEX.toString()});
        assertTrue(rs.next());
        assertEquals("lowerCaseIndex", rs.getString(3));

        ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        conn.createStatement().execute(ddl);

        // Assert the rows for index table is completely removed.
        rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
        assertFalse(rs.next());

    }

    @Test
    public void testIndexDefinitionWithRepeatedColumns() throws Exception {
        // Test index creation when the columns is included in both the PRIMARY and INCLUDE section. Test de-duplication.

        String indexDataTable = generateUniqueName();
        String indexName = generateUniqueName();
        try {
            String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            String sql = "create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true";
            System.out.println(sql);
            conn.createStatement().execute(sql);
            String ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName
                    + " (a.int_col1, a.long_col1, b.int_col2, b.long_col2)"
                    + " INCLUDE(int_col1, int_col2)";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testIndexDefinitionWithSameColumnNamesInTwoFamily() throws Exception {
        String testTable = generateUniqueName();
        String indexName = generateUniqueName();
        String ddl = "create table " + testTable  + " (char_pk varchar not null,"
                + " a.int_col integer, a.long_col integer,"
                + " b.int_col integer, b.long_col integer"
                + " constraint pk primary key (char_pk))";
        conn.createStatement().execute(ddl);

        ddl = "CREATE EXTERNAL INDEX " + indexName + "1 ON " + testTable  + " (a.int_col, b.int_col)";
        conn.createStatement().execute(ddl);

        try {
            ddl = "CREATE EXTERNAL INDEX " + indexName + "2 ON " + testTable  + " (int_col)";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception");
        } catch (AmbiguousColumnException e) {
            assertEquals(SQLExceptionCode.AMBIGUOUS_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testBinaryNonnullableIndex() throws Exception {
        String testTable = generateUniqueName();
        String indexName = generateUniqueName();

        String ddl =
                "CREATE TABLE " + testTable  + " ( "
                        + "v1 BINARY(64) NOT NULL, "
                        + "v2 VARCHAR, "
                        + "v3 BINARY(64), "
                        + "v4 VARCHAR "
                        + "CONSTRAINT PK PRIMARY KEY (v1))";
        conn.createStatement().execute(ddl);
        conn.commit();

        conn.createStatement().execute("CREATE EXTERNAL INDEX " + indexName + "4 ON " + testTable  + " (v4) INCLUDE (v2)");
        conn.commit();

        conn.createStatement().execute("CREATE EXTERNAL INDEX varbinLastInRow ON " + testTable  + " (v1, v3)");
        conn.commit();

        conn.createStatement().execute( "CREATE EXTERNAL INDEX " + indexName + "5 ON " + testTable  + " (v2) INCLUDE (v4, v3, v1)");
        conn.commit();

        conn.createStatement().executeQuery(
                "select v1,v2,v3,v4 FROM " + testTable  + " where v2 = 'abc' and v3 != 'a'");

    }

    @Test
    public void testAsyncCreatedDate() throws Exception {
        Date d0 = new Date(System.currentTimeMillis());

        String testTable = generateUniqueName();

        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar, v2 varchar, v3 varchar)";
        conn.createStatement().execute(ddl);
        String indexName = "ASYNCIND_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + "1 ON " + testTable  + " (v1) ASYNC";
        conn.createStatement().execute(ddl);
        ddl = "CREATE EXTERNAL INDEX " + indexName + "2 ON " + testTable  + " (v2) ASYNC";
        conn.createStatement().execute(ddl);
        ddl = "CREATE EXTERNAL INDEX " + indexName + "3 ON " + testTable  + " (v3)";
        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery(
                "select table_name, " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " +
                        "from \"SYSTEM\".catalog (" + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() + ") " +
                        "where " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " is not null and table_name like 'ASYNCIND_%' " +
                        "order by " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE
        );
        assertTrue(rs.next());
        assertEquals(indexName + "1", rs.getString(1));
        Date d1 = rs.getDate(2);
        assertTrue(d1.after(d0));
        assertTrue(rs.next());
        assertEquals(indexName + "2", rs.getString(1));
        Date d2 = rs.getDate(2);
        assertTrue(d2.after(d1));
        assertFalse(rs.next());
    }

    @Test
    public void testAsyncRebuildTimestamp() throws Exception {
        long startTimestamp = System.currentTimeMillis();

        String testTable = generateUniqueName();
        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar, v2 varchar, v3 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "R_ASYNCIND_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + "1 ON " + testTable  + " (v1) ";
        stmt.execute(ddl);
        ddl = "CREATE EXTERNAL INDEX " + indexName + "2 ON " + testTable  + " (v2) ";
        stmt.execute(ddl);
        ddl = "CREATE EXTERNAL INDEX " + indexName + "3 ON " + testTable  + " (v3)";
        stmt.execute(ddl);
        conn.createStatement().execute("ALTER INDEX "+indexName+"1 ON " + testTable +" DISABLE ");
        conn.createStatement().execute("ALTER INDEX "+indexName+"2 ON " + testTable +" REBUILD ");
        conn.createStatement().execute("ALTER INDEX "+indexName+"3 ON " + testTable +" REBUILD ASYNC");

        ResultSet rs = conn.createStatement().executeQuery(
                "select table_name, " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " +
                        "from \"SYSTEM\".catalog (" + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() + ") " +
                        "where " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " !=0 and table_name like 'R_ASYNCIND_%' " +
                        "order by table_name");
        assertTrue(rs.next());
        assertEquals(indexName + "3", rs.getString(1));
        long asyncTimestamp = rs.getLong(2);
        assertTrue("Async timestamp is recent timestamp", asyncTimestamp > startTimestamp);
        PTable table = PhoenixRuntime.getTable(conn, indexName+"3");
        assertEquals(table.getTimeStamp(), asyncTimestamp);
        assertFalse(rs.next());
        conn.createStatement().execute("ALTER INDEX "+indexName+"3 ON " + testTable +" DISABLE");
        rs = conn.createStatement().executeQuery(
                "select table_name, " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " +
                        "from \"SYSTEM\".catalog (" + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() + ") " +
                        "where " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " !=0 and table_name like 'ASYNCIND_%' " +
                        "order by table_name" );
        assertFalse(rs.next());
    }

    @Test
    public void testImmutableTableOnlyHasPrimaryKeyIndex() throws Exception {
        helpTestTableOnlyHasPrimaryKeyIndex(false, false);
    }

    @Test
    public void testImmutableLocalTableOnlyHasPrimaryKeyIndex() throws Exception {
        helpTestTableOnlyHasPrimaryKeyIndex(false, true);
    }

    @Test
    public void testMutableTableOnlyHasPrimaryKeyIndex() throws Exception {
        helpTestTableOnlyHasPrimaryKeyIndex(true, false);
    }

    @Test
    public void testMutableLocalTableOnlyHasPrimaryKeyIndex() throws Exception {
        helpTestTableOnlyHasPrimaryKeyIndex(true, true);
    }

    private void helpTestTableOnlyHasPrimaryKeyIndex(boolean mutable,
                                                     boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        conn.createStatement().execute(
                "CREATE TABLE " + dataTableName + " ("
                        + "pk1 VARCHAR not null, "
                        + "pk2 VARCHAR not null, "
                        + "CONSTRAINT PK PRIMARY KEY (pk1, pk2))"
                        + (!mutable ? "IMMUTABLE_ROWS=true" : ""));
        String query = "SELECT * FROM " + dataTableName;
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        conn.createStatement().execute(
                "CREATE " + (localIndex ? "LOCAL" : "EXTERNAL")
                        + " INDEX " + indexName + " ON " + dataTableName + " (pk2, pk1)");
        query = "SELECT * FROM " + indexName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?)");
        stmt.setString(1, "k11");
        stmt.setString(2, "k21");
        stmt.execute();
        conn.commit();

        query = "SELECT * FROM " + indexName;
        rs = conn.createStatement().executeQuery(query);
        List<String> result = new ArrayList<>(2);
        assertTrue(rs.next());
        result.add(rs.getString(1));
        result.add(rs.getString(2));
        assertTrue(result.contains("k21"));
        assertTrue(result.contains("k11"));

        assertFalse(rs.next());
        result.clear();
        query = "SELECT * FROM " + dataTableName + " WHERE pk2='k21'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        result.add(rs.getString(1));
        result.add(rs.getString(2));
        assertTrue(result.contains("k11"));
        assertTrue(result.contains("k21"));
        assertFalse(rs.next());
    }



    @Test
    public void testIndexAlterPhoenixProperty() throws Exception {

        String testTable = generateUniqueName();
        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "IDX_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + testTable  + " (v1) ";
        stmt.execute(ddl);
        conn.createStatement().execute("ALTER INDEX "+indexName+" ON " + testTable +" ACTIVE SET GUIDE_POSTS_WIDTH = 10");

        ResultSet rs = conn.createStatement().executeQuery(
                "select GUIDE_POSTS_WIDTH from SYSTEM.\"CATALOG\" where TABLE_NAME='" + indexName + "'");assertTrue(rs.next());
        assertEquals(10,rs.getInt(1));

        conn.createStatement().execute("ALTER INDEX "+indexName+" ON " + testTable +" ACTIVE SET GUIDE_POSTS_WIDTH = 20");
        rs = conn.createStatement().executeQuery(
                "select GUIDE_POSTS_WIDTH from SYSTEM.\"CATALOG\" where TABLE_NAME='" + indexName + "'");assertTrue(rs.next());
        assertEquals(20,rs.getInt(1));
    }


    @Test
    public void testIndexAlterHBaseProperty() throws Exception {
        String testTable = generateUniqueName();

        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "IDX_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + testTable  + " (v1) ";
        stmt.execute(ddl);

        conn.createStatement().execute("ALTER INDEX "+indexName+" ON " + testTable +" ACTIVE SET DISABLE_WAL=false");
        asssertIsWALDisabled(conn,indexName,false);
        conn.createStatement().execute("ALTER INDEX "+indexName+" ON " + testTable +" ACTIVE SET DISABLE_WAL=true");
        asssertIsWALDisabled(conn,indexName,true);
    }

    private static void asssertIsWALDisabled(PhoenixConnection conn, String fullTableName, boolean expectedValue) throws SQLException {
        assertEquals(expectedValue, conn.getTable(new PTableKey(conn.getTenantId(), fullTableName)).isWALDisabled());
    }

    // 上面来自IndexMetadataIT
    @Test
    public void testExternalIndexProps() throws Exception {
        String testTable = generateUniqueName();

        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "IDX_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + testTable  + " (v1) " +
                "\""+ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop1-child\"=\"propvalue1\"," +
                "\""+ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop2\"=propvalue2";
        try{
            stmt.execute(ddl);
            fail(ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop1-child & "+
                    ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop2 not processed");
        }catch (ElasticsearchStatusException e){
            assertTrue(ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop1-child & "+
                            ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX +"prop2 processed",
                    e.getDetailedMessage().contains("unknown setting [index.prop1-child]") ||
                    e.getDetailedMessage().contains("unknown setting [index.prop2]") );
        }
    }
    @Test
    public void testExternalMappingProps() throws Exception {
        String testTable = generateUniqueName();

        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "IDX_" + generateUniqueName();

        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + testTable  + " (v1) " +
                "\""+ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop1-child\"=\"propvalue1\"," +
                "\""+ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop2\"=propvalue2";
        try{
            stmt.execute(ddl);
            fail(ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop1-child & "+
                    ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop2 not processed");
        }catch (ElasticsearchStatusException e){
            assertTrue(ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop1-child & "+
                            ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX +"prop2 processed",
                    e.getDetailedMessage().contains("Root mapping definition has unsupported parameters:  [prop2 : propvalue2] [prop1-child : propvalue1]") );
        }
    }
}
