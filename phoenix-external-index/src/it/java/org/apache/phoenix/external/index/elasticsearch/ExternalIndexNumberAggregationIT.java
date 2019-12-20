package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.index.elasticsearch.base.BaseTest;
import org.apache.phoenix.external.index.elasticsearch.base.ParallelStatsDisabledIT;
import org.apache.phoenix.external.index.elasticsearch.base.TestUtil;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 * 针对BaseExternalIndexIT新增的测试案例
 */
@RunWith(Parameterized.class)
public class ExternalIndexNumberAggregationIT extends ParallelStatsDisabledIT {
    @Parameterized.Parameters(name="ExternalIndexNumberAggregationIT_mutable={0},columnEncoded={1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false },
                { false, true },
                { true, false },
                { true, true }
        });
    }
    private static final Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    private final String tableDDLOptions;
    private PhoenixConnection conn;
    public ExternalIndexNumberAggregationIT(boolean mutable, boolean columnEncoded){
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("IMMUTABLE_ROWS=true");
            optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME="+ PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Before
    public void init() throws SQLException {
        conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
    }
    @After
    public void close() throws SQLException {
        conn.setAutoCommit(true);
        ExternalUtil.closeSilent(conn);
    }
    @Test
    public void testMultiGroupByMultiAgg()throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        BaseTest.upsertRow( conn, fullTableName, 4, false);
        conn.createStatement().executeUpdate("upsert into "+fullTableName+
                "(VARCHAR_PK,CHAR_PK,INT_PK,LONG_PK,DECIMAL_PK,DATE_PK,INT_COL2)" +
                "values('varchar4','char4',4,4,4,'2015-01-01 00:00:00',5)");
        conn.commit();
        TestUtil.printResult(conn.createStatement().executeQuery("select * from "+fullTableName),fullTableName+" all data");


        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (long_col2,int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;
        Map<String,String> result = new HashMap<>(3);
        String noIndexQuery = "SELECT /*+ NO_INDEX */ long_col2,int_col2,max(int_col1) mx" +
                ",min(int_col1) mi,count(1) cnt FROM " + fullTableName + " GROUP BY long_col2,int_col2";
        TestUtil.printResult(conn.createStatement().executeQuery(
                "SELECT /*+ NO_INDEX */ long_col2,int_col2,max(int_col1) mx,min(int_col1) mi,count(1) cnt " +
                        "FROM " + fullTableName + " GROUP BY long_col2, int_col2 order by long_col2, int_col2"),
                fullTableName+" all agg data");
        rs = (PhoenixResultSet) conn.createStatement().executeQuery(noIndexQuery);
//        TBL_T000001 all agg data
//        3   3   2   2   1
//        4   4   3   3   1
//        5   5   4   4   1
//        6   5   5   5   1
        assertTrue(rs.next());
        result.put(rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2")
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        result.put(rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2")
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        result.put(rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2")
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        result.put(rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2")
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertFalse(rs.next());

        String query = "SELECT long_col2, int_col2,max(int_col1) mx,min(int_col1) mi,count(1) cnt " +
                "FROM " + fullTableName + " GROUP BY long_col2,int_col2 order by int_col2,long_col2 desc";

        TestUtil.printResult(conn.createStatement().executeQuery(query),"index GROUP BY int_col2");

        rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        assertTrue(rs.next());

        String col2 = rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2");
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));

        assertTrue(rs.next());
        col2 = rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2");
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));

        assertTrue(rs.next());
        col2 = rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2");
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        col2 = rs.getInt("LONG_COL2")+"-" + rs.getInt("INT_COL2");
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));

        assertFalse(rs.next());

    }
    @Test
    public void testGroupByMultiAgg()throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable2(fullTableName);
        TestUtil.printResult(conn.createStatement().executeQuery("select * from "+fullTableName),fullTableName+" all data");


        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;
        Map<String,String> result = new HashMap<>(3);
        String noIndexQuery = "SELECT /*+ NO_INDEX */ int_col2,max(int_col1) mx,min(int_col1) mi,count(1) cnt,avg(int_col1) ag FROM " + fullTableName + " GROUP BY int_col2";
        TestUtil.printResult(conn.createStatement().executeQuery(noIndexQuery),
                fullTableName+" all agg data");
        rs = (PhoenixResultSet) conn.createStatement().executeQuery(noIndexQuery);
        assertTrue(rs.next());
        result.put(String.valueOf( rs.getInt("INT_COL2") )
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        result.put(String.valueOf( rs.getInt("INT_COL2") )
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertTrue(rs.next());
        result.put(String.valueOf( rs.getInt("INT_COL2") )
                ,rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertFalse(rs.next());
//        INT_COL2    MX    MI    CNT    AG
//        3   2   2   1   2
//        4   3   3   1   3
//        5   5   4   2   4.5
        String query = "SELECT int_col2,max(int_col1) mx,min(int_col1) mi,count(1) cnt,avg(int_col1) ag  FROM " + fullTableName
                + " GROUP BY int_col2 order by int_col2 desc";

        TestUtil.printResult(conn.createStatement().executeQuery(query),"index GROUP BY int_col2");

        rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        assertTrue(rs.next());

        String col2 = String.valueOf(rs.getInt("INT_COL2"));
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));

        assertTrue(rs.next());
        col2 = String.valueOf(rs.getInt("INT_COL2"));
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));

        assertTrue(rs.next());
        col2 = String.valueOf(rs.getInt("INT_COL2"));
        assertTrue(result.containsKey(col2));
        assertEquals(result.get(col2),rs.getLong("MX")+"-"+rs.getLong("MI")+"-"+rs.getLong("CNT"));
        assertFalse(rs.next());

    }
    @Test
    public void testGroupByMax()throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;
        String query = "SELECT int_col2,max(int_col1) FROM " + fullTableName + " GROUP BY int_col2";
        rs = (PhoenixResultSet) conn.createStatement().executeQuery(query);
        String noIndexQuery = "SELECT /*+ NO_INDEX */ int_col2,max(int_col1) FROM " + fullTableName + " GROUP BY int_col2";
        TestUtil.printResult(conn.createStatement().executeQuery(noIndexQuery),"noIndex GROUP BY int_col2");
        TestUtil.printResult(conn.createStatement().executeQuery(query),"index GROUP BY int_col2");
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(2));
    }
    @Test
    public void testGroupByCount() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;
        String query = "SELECT int_col2,count(int_col1) FROM " + fullTableName + " GROUP BY int_col2";
        rs = (PhoenixResultSet)conn.createStatement().executeQuery(query);
        String noIndexQuery = "SELECT /*+ NO_INDEX */ int_col2,count(int_col1) FROM " + fullTableName + " GROUP BY int_col2";
        TestUtil.printResult(conn.createStatement().executeQuery(noIndexQuery),"noIndex GROUP BY int_col2");
        TestUtil.printResult(conn.createStatement().executeQuery(query),"index GROUP BY int_col2");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(2));

    }
    @Test
    public void testMax() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;

        rs = (PhoenixResultSet)conn.createStatement().executeQuery("SELECT MAX(int_col2) FROM " + fullTableName );
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ MAX(int_col2) FROM " + fullTableName),"GROUP BY null");

        assertTrue(rs.next());
        assertEquals(5,rs.getInt(1));
    }
    @Test
    public void testCount() throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;

        rs = (PhoenixResultSet)conn.createStatement().executeQuery("SELECT COUNT(int_col2) FROM " + fullTableName );
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ COUNT(int_col2) FROM " + fullTableName),"GROUP BY null");

        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
    }
    @Test
    public void testMin() throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable2(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;

        rs = (PhoenixResultSet)conn.createStatement().executeQuery("SELECT min(LONG_COL1) FROM " + fullTableName );
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ * FROM " + fullTableName),"all data "+fullTableName);
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ min(LONG_COL1) FROM " + fullTableName),"GROUP BY null");

        assertTrue(rs.next());
        assertEquals(2,rs.getLong(1));
    }
    @Test
    public void testSum() throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;

        rs = (PhoenixResultSet)conn.createStatement().executeQuery("SELECT sum(int_col2) FROM " + fullTableName );
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ sum(int_col2) FROM " + fullTableName),"GROUP BY null");

        assertTrue(rs.next());
        assertEquals(12,rs.getInt(1));
    }
    @Test
    public void testAvg() throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        PhoenixResultSet rs;

        rs = (PhoenixResultSet)conn.createStatement().executeQuery("SELECT avg (int_col2) FROM " + fullTableName );
        TestUtil.printResult(conn.createStatement().executeQuery("SELECT  /*+ NO_INDEX */ avg(int_col2) FROM " + fullTableName),"GROUP BY null");

        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
    }
    @Test
    public void keyFieldAgg()throws Exception{
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        BaseTest.populateTestTable(fullTableName);
        ddl = "CREATE EXTERNAL INDEX " + indexName + " ON " + fullTableName + " (int_col2,int_col1)";

        stmt.execute(ddl);
        try{
            conn.createStatement().executeQuery("SELECT max (int_pk),max (char_pk) FROM " + fullTableName );
            fail();
        }catch (UnsupportedEsAggregationException e){
            assertEquals("Row key not support Aggregation"
                    ,"Row key not support Aggregation",e.getMessage());
        }
    }
}
