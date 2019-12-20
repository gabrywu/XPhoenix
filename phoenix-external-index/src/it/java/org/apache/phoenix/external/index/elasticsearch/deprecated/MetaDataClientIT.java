package org.apache.phoenix.external.index.elasticsearch.deprecated;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.external.ExternalConstants;
import org.apache.phoenix.external.index.elasticsearch.base.ParallelStatsDisabledIT;
import org.apache.phoenix.external.index.elasticsearch.base.TestUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetaDataClientIT extends ParallelStatsDisabledIT {
    //curl -XDELETE http://server1:9200/index_t000002
    private void printCatalog(PhoenixConnection connection) throws SQLException {
        String query = "SELECT * FROM system.catalog where table_schem is null";
        System.out.println("查询catalog数据 "+query);
        printResult(connection,query);
    }
    private void printPlan(PhoenixConnection connection,String query) throws SQLException{
        System.out.println("执行计划 "+query);
        printResult(connection,"explain "+query);
    }
    private void printResult(PhoenixResultSet rs ) throws SQLException{
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            System.out.print(metaData.getColumnName(i)+"    ");
        }
        System.out.println();
        while (rs.next()){
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.print(rs.getString(i)+"   ");
            }
            System.out.println();
        }
    }
    private void printResult(PhoenixConnection connection,String query) throws SQLException{
        PhoenixResultSet rs = (PhoenixResultSet)connection.createStatement().executeQuery(query);
        printResult(rs);
    }
    @Test
    public void maintainExternalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {
            System.out.println(conn.getQueryServices().getConfiguration().get(
                    ExternalConstants.EXTERNAL_METADATA_CLASS_KEY));

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY1','V1_TEST_1','create by index create')";
            conn.createStatement().execute(query);
            conn.commit();

            query="CREATE EXTERNAL INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY2','V1_TEST_1','create by index maintain')";
            conn.createStatement().execute(query);
            conn.commit();

            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where v1='V1_TEST_1'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY1",rs.getString("K"));
            assertEquals("V1_TEST_1",rs.getString("V1"));
            printPlan(conn,query);
            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where external_query = 'V1_TEST_1'";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            query = "SELECT a.*,b.external_query FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.external_query = 'V1_TEST_1'";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            //printPlan(conn,query);
        }
    }
    @Test
    public void maintainGlobalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {
            System.out.println(conn.getQueryServices().getConfiguration().get(
                    ExternalConstants.EXTERNAL_METADATA_CLASS_KEY));

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY1','V1_TEST_1','V2_TEST_1')";
            conn.createStatement().execute(query);
            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY2','V1_TEST_1','V2_TEST_2')";
            conn.createStatement().execute(query);
            conn.commit();

            query="CREATE INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            query = "SELECT a.*,b.v1 FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.v1 = 'V1_TEST_1'";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            //printPlan(conn,query);
        }
    }

    @Test
    public void maintainExternalIndexWithOffsetLimit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {
            System.out.println(conn.getQueryServices().getConfiguration().get(
                    ExternalConstants.EXTERNAL_METADATA_CLASS_KEY));

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY1','V1_TEST_1','V2_TEST_1')";
            conn.createStatement().execute(query);
            conn.commit();

            query="CREATE EXTERNAL INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY2','V1_TEST_1','V2_TEST_2')";
            conn.createStatement().execute(query);
            conn.commit();

            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where v1='V1_TEST_1' limit 1";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY1",rs.getString("K"));
            assertEquals("V1_TEST_1",rs.getString("V1"));
            assertEquals("V2_TEST_1",rs.getString("V2"));
            printPlan(conn,query);
            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where external_query = 'V1_TEST_1' limit 1 ";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            query = "SELECT a.*,b.external_query FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.external_query = 'V1_TEST_1' limit 1 ";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            //printPlan(conn,query);
        }
    }
    @Test
    public void maintainExternalIndexWithOrderBy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {
            System.out.println(conn.getQueryServices().getConfiguration().get(
                    ExternalConstants.EXTERNAL_METADATA_CLASS_KEY));

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY1','V1_TEST_1','V2_TEST_1')";
            conn.createStatement().execute(query);
            conn.commit();

            query="CREATE EXTERNAL INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY2','V1_TEST_1','V2_TEST_2')";
            conn.createStatement().execute(query);
            conn.commit();

            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where v1='V1_TEST_1' limit 1";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY1",rs.getString("K"));
            assertEquals("V1_TEST_1",rs.getString("V1"));
            assertEquals("V2_TEST_1",rs.getString("V2"));
            printPlan(conn,query);
            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where external_query = 'V1_TEST_1' order by v1 ";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            query = "SELECT a.*,b.external_query FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.external_query = 'V1_TEST_1' order by b.v1 ";
            rs = conn.createStatement().executeQuery(query);
            printResult((PhoenixResultSet) rs);
            //printPlan(conn,query);
        }
    }

    @Test
    public void explainExternalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {
            System.out.println(conn.getQueryServices().getConfiguration().get(
                    ExternalConstants.EXTERNAL_METADATA_CLASS_KEY));

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="CREATE EXTERNAL INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where v1='V1_TEST_1'";

            printPlan(conn,query);
            query = "SELECT /*+ INDEX("+fullTableName+" "+fullIndexName+") */* FROM " + fullTableName + " where external_query = 'V1_TEST_1'";

            printPlan(conn,query);

            query = "SELECT a.*,b.external_query FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.external_query = 'V1_TEST_1'";

            printPlan(conn,query);
        }
    }
}
