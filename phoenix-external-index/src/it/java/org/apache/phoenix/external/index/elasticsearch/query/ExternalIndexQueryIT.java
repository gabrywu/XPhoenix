package org.apache.phoenix.external.index.elasticsearch.query;

import org.apache.phoenix.external.index.elasticsearch.base.BaseTest;
import org.apache.phoenix.external.index.elasticsearch.base.ParallelStatsDisabledIT;
import org.apache.phoenix.external.index.elasticsearch.base.TestUtil;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

public class ExternalIndexQueryIT extends ParallelStatsDisabledIT {
    private static final Logger log = LoggerFactory.getLogger(ExternalIndexQueryIT.class);
    private static final Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    private static final Random random = new Random();
    private PhoenixConnection conn;
    @Before
    public void before() throws SQLException {
        conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    }
    @After
    public void after() throws SQLException {
        conn.setAutoCommit(true);
        ExternalUtil.closeSilent(conn);
    }
    private String createTableName(){
        String tableName = "TBL_" + generateUniqueName();
        return SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    }
    private String createIndexName(){
        String indexName = "IND_" + generateUniqueName();
        return SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    }
    private void createTable(PhoenixConnection conn,String tableName) throws SQLException {
        String ddl ="CREATE TABLE " + tableName + TestUtil.TEST_TABLE_SCHEMA;
        conn.createStatement().execute(ddl);
    }
    private void createIndex(PhoenixConnection conn,String tableName,String indexName) throws SQLException {
        String indexDdl = "create external index "+indexName+" on "+tableName + "(varchar_col1) include(varchar_col2)";
        conn.createStatement().execute(indexDdl);
    }
    @Test
    public void indexFirstColumn() throws SQLException {
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);

        createIndex(conn,tableName,indexName);
        TestUtil.printTableColumns(conn,tableName,tableName+" metadata");
        TestUtil.printTableColumns(conn,indexName,indexName+" metadata");
        ResultSet rs = conn.getMetaData().getColumns ("",
                null,
                indexName,
                "0:EXTERNAL_QUERY");
        assertTrue(rs.next());
        assertEquals("EXTERNAL_QUERY is not the first field in index "+indexName,1,rs.getInt("ORDINAL_POSITION"));
    }
    @Test
    public void fullTextQuery() throws SQLException {
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);
        BaseTest.populateTestTable2(tableName);
        conn.createStatement().executeUpdate("upsert into "+tableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2,varchar_col1,varchar_col2)" +
                "values('varchar4','char4',4,4,4,'2015-01-01 00:00:00',5,'varchar_a col1','varchar_a col2')");
        conn.commit();
        createIndex(conn,tableName,indexName);

        String query = "select /*+ INDEX("+tableName+" "+indexName+") */ * from "+tableName+" where external_query = 'col1'";

        PhoenixResultSet rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("VARCHAR_COL1 value is not right",
                "varchar_a col1",rs.getString("VARCHAR_COL1"));
        assertFalse(rs.next());
    }
    @Test
    public void startWithQuery() throws SQLException {
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);
        BaseTest.populateTestTable2(tableName);
        conn.createStatement().executeUpdate("upsert into "+tableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2,varchar_col1,varchar_col2)" +
                "values('varchar4','char4',4,4,4,'2015-01-01 00:00:00',5,'varchar_ab','varchar_a col2')");
        conn.commit();
        conn.createStatement().executeUpdate("upsert into "+tableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2,varchar_col1,varchar_col2)" +
                "values('varchar5','char5',5,5,5,'2015-01-01 00:00:00',6,'varchar_abc','varchar_ax col2')");
        conn.commit();
        createIndex(conn,tableName,indexName);

        String query = "select /*+ INDEX("+tableName+" "+indexName+") */ * from "+tableName+" where external_query = 'varchar_a?'";
        TestUtil.printResult(conn.createStatement().executeQuery(query),"start with [varchar_a ]");
        PhoenixResultSet rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("VARCHAR_COL1 value is not right",
                "varchar_ab",rs.getString("VARCHAR_COL1"));
        assertEquals("VARCHAR_COL2 value is not right",
                "varchar_a col2",rs.getString("VARCHAR_COL2"));
        assertTrue(rs.next());
        assertEquals("VARCHAR_COL1 value is not right",
                "varchar_abc",rs.getString("VARCHAR_COL1"));
        assertEquals("VARCHAR_COL2 value is not right",
                "varchar_ax col2",rs.getString("VARCHAR_COL2"));
        assertFalse(rs.next());
    }
    @Test
    public void fieldMatchQuery() throws SQLException{
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);
        BaseTest.populateTestTable2(tableName);
        conn.createStatement().executeUpdate("upsert into "+tableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2,varchar_col1,varchar_col2)" +
                "values('varchar4','char4',4,4,4,'2015-01-01 00:00:00',5,'varchar_a col1','varchar_a col2')");

        conn.createStatement().executeUpdate("upsert into "+tableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2,varchar_col1,varchar_col2)" +
                "values('varchar5','char5',5,5,5,'2015-01-01 00:00:00',6,'varchar_a col2','varchar_a col1')");
        conn.commit();
        createIndex(conn,tableName,indexName);

        String query = "select /*+ INDEX("+tableName+" "+indexName+") */ * from "+tableName+" where external_query = 'VARCHAR_COL1_raw:\"varchar_a col1\"'";

        PhoenixResultSet rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("VARCHAR_COL1 value is not right",
                "varchar_a col1",rs.getString("VARCHAR_COL1"));
        assertFalse(rs.next());
    }
    @Test
    public void limitQuery()throws SQLException{
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);
        int totalNum = random.nextInt(200);
        upsertRows(conn,tableName,totalNum);
        conn.commit();
        createIndex(conn,tableName,indexName);
        int limitNum = random.nextInt(totalNum-1) + 1;

        log.info("limitQuery table {}, index {},total num {},limitNum {}",
                tableName,indexName,totalNum,limitNum);

        String query = "select varchar_col1,varchar_col2 from "+tableName+" limit "+limitNum;
        PhoenixResultSet rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        for (int i = 0; i < limitNum; i++) {
            assertTrue(rs.next());
        }
        assertFalse(rs.next());
    }
    @Test
    public void matchManyQuery()throws SQLException{
        String tableName = createTableName();
        String indexName = createIndexName();
        createTable(conn, tableName);
        int totalNum = random.nextInt(20)+10;
        upsertRows(conn,tableName,totalNum);
        conn.commit();
        createIndex(conn,tableName,indexName);

        log.info("limitQuery table {}, index {},totalNum {}",tableName,indexName,totalNum);

        String query = "select varchar_col1,varchar_col2 from "+tableName+" where external_query = 'varchar_a'";
        PhoenixResultSet rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        for (int i = 0; i < totalNum; i++) {
            assertTrue(rs.next());
        }
        assertFalse(rs.next());
    }
}
