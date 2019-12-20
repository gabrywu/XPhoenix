package org.apache.phoenix.external;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

public class ExternalMetaDataClientIT extends ParallelStatsDisabledIT {

    @Test
    public void createExternalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props);
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

            query = "SELECT * FROM " + tableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            query="CREATE EXTERNAL INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            TableName indexTableName = TableName.create(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
            NamedTableNode indexNode = NamedTableNode.create(null, indexTableName, null);
            ColumnResolver resolver = FromCompiler.getResolver(indexNode, conn.unwrap(PhoenixConnection.class));
            PTable indexTable = resolver.getTables().get(0).getTable();
            assertSame("Index type is EXTERNAL",indexTable.getIndexType(), PTable.IndexType.EXTERNAL);
            rs = conn.createStatement().executeQuery("explain "+query);
            assertTrue(rs.next());
            assertEquals("explain " + query, "CREATE EXTERNAL INDEX", rs.getString(1));

            HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(fullIndexName));
            assertEquals("HTableDescriptor has "+ExternalConstants.ATTRIBUTE_INDEX_TYPE +" flag"
                    , PTable.IndexType.EXTERNAL,PTable.IndexType.valueOf(tableDescriptor.getValue(ExternalConstants.ATTRIBUTE_INDEX_TYPE)));
        }
    }

    @Test
    public void maintainGlobalIndexAndInclude() throws Exception{
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props);
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

            query="CREATE INDEX " + fullIndexName + " ON " + fullTableName + " (v1) include(v2) ";
            conn.createStatement().execute(query);

            TableName indexTableName = TableName.create(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
            NamedTableNode indexNode = NamedTableNode.create(null, indexTableName, null);
            ColumnResolver resolver = FromCompiler.getResolver(indexNode, conn.unwrap(PhoenixConnection.class));
            PTable indexTable = resolver.getTables().get(0).getTable();
            assertSame("Index type is global",indexTable.getIndexType(), PTable.IndexType.GLOBAL);
            rs = conn.createStatement().executeQuery("explain "+query);
            assertTrue(rs.next());
            assertEquals("explain " + query, "CREATE GLOBAL INDEX", rs.getString(1));

            HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(fullIndexName));
            assertEquals("HTableDescriptor has "+ExternalConstants.ATTRIBUTE_INDEX_TYPE +" flag"
                    , PTable.IndexType.GLOBAL,PTable.IndexType.valueOf(tableDescriptor.getValue(ExternalConstants.ATTRIBUTE_INDEX_TYPE)));

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY','V1_TEST','V2_TEST')";
            conn.createStatement().execute(query);
            conn.commit();
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY",rs.getString("K"));
        }
    }
    @Test
    public void maintainGlobalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props);
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

            query="CREATE INDEX " + fullIndexName + " ON " + fullTableName + " (v1,v2) ";
            conn.createStatement().execute(query);

            TableName indexTableName = TableName.create(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
            NamedTableNode indexNode = NamedTableNode.create(null, indexTableName, null);
            ColumnResolver resolver = FromCompiler.getResolver(indexNode, conn.unwrap(PhoenixConnection.class));
            PTable indexTable = resolver.getTables().get(0).getTable();
            assertSame("Index type is global",indexTable.getIndexType(), PTable.IndexType.GLOBAL);
            rs = conn.createStatement().executeQuery("explain "+query);
            assertTrue(rs.next());
            assertEquals("explain " + query, "CREATE GLOBAL INDEX", rs.getString(1));

            HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(fullIndexName));
            assertEquals("HTableDescriptor has "+ExternalConstants.ATTRIBUTE_INDEX_TYPE +" flag"
                    , PTable.IndexType.GLOBAL,PTable.IndexType.valueOf(tableDescriptor.getValue(ExternalConstants.ATTRIBUTE_INDEX_TYPE)));

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY','V1_TEST','V2_TEST')";
            conn.createStatement().execute(query);
            conn.commit();
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY",rs.getString("K"));
        }
    }

    @Test
    public void maintainExternalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props);
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

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY','V1_TEST','V2_TEST')";
            conn.createStatement().execute(query);
            conn.commit();
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("KEY",rs.getString("K"));
            assertEquals("V1_TEST",rs.getString("V1"));
            assertEquals("V2_TEST",rs.getString("V2"));
            query = "SELECT * FROM system.catalog where table_schem is null";
            System.out.println("查询catalog数据 "+query);
            rs = conn.createStatement().executeQuery(query);
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
    }

    @Test
    public void selectGlobalIndex() throws Exception{
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TABLE_" + generateUniqueName();
        String indexName = "INDEX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props)) {

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query="CREATE INDEX " + fullIndexName + " ON " + fullTableName + " (v1) ";
            conn.createStatement().execute(query);

            query="UPSERT INTO "+fullTableName+"(K,V1,V2)VALUES('KEY','V1_TEST','V2_TEST')";
            conn.createStatement().execute(query);
            conn.commit();

            query = "SELECT * FROM " + fullTableName + " where v1='V1_TEST'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            query = "SELECT/*+ INDEX("+fullTableName+" "+fullIndexName+") */ * FROM " + fullTableName + " where v1='V1_TEST'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            query = "SELECT k FROM " + fullTableName + " where v1='V1_TEST'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            query = "SELECT a.* FROM " + fullTableName + " a join "+fullTableName+" b on a.k = b.k where b.v1='V1_TEST'";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
        }
    }

    @Test
    public void getTableDescriptorsWhenOneNotExists() throws Exception{
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName1 = "TABLE_" + generateUniqueName();
        String fullTableName1 = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName1);
        String tableName2 = "TABLE_" + generateUniqueName();
        String fullTableName2 = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName2);
        List<String> tables = new ArrayList<>(2);
        tables.add(fullTableName1);
        tables.add(fullTableName2);
        try (PhoenixConnection conn = (PhoenixConnection)DriverManager.getConnection(getUrl(), props);
             HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin()) {

            String ddl ="CREATE TABLE " + fullTableName1
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "COLUMN_ENCODED_BYTES = 0 ";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            HTableDescriptor[] tableDescriptors = admin.getTableDescriptors(tables);
            assertEquals(tableDescriptors.length,1);
            assertEquals(fullTableName1,tableDescriptors[0].getTableName().getQualifierAsString());
            for (HTableDescriptor tableDescriptor:tableDescriptors){
                System.out.println(tableDescriptor.getTableName());
            }

        }
    }
    @Test
    public void testConfigurationIterator(){
        Configuration conf = HBaseConfiguration.create();
        int t = 10000;
        long s = System.currentTimeMillis();
        for (int i = 0; i < t; i++) {
            ExternalUtil.getReadOnlyConfiguration(conf);
        }
        long e = System.currentTimeMillis();
        System.out.println("getReadOnlyConfiguration," + t +" 次耗时 "+(e-s));
        s = System.currentTimeMillis();
        Map<String, String> props = null;
        for (int i = 0; i < t; i++) {
            props = new HashMap<>(conf.size());
            Iterator<Map.Entry<String, String>> it = conf.iterator();
            while (it.hasNext()){
                Map.Entry<String, String> en = it.next();
                props.put(en.getKey(),en.getValue());
            }
        }
        e = System.currentTimeMillis();
        System.out.println("Configuration iterator2map," + t +" 次耗时 "+(e-s));

        s = System.currentTimeMillis();

        for (int i = 0; i < t; i++) {
           new ReadOnlyProps(props);
        }
        e = System.currentTimeMillis();
        System.out.println("ReadOnlyProps(props)," + t+" 次耗时 "+(e-s));
        s = System.currentTimeMillis();

        for (int i = 0; i < t; i++) {
            new ReadOnlyProps(conf.iterator());
        }
        e = System.currentTimeMillis();
        System.out.println("ReadOnlyProps(conf.iterator())," + t +" 次耗时 "+(e-s));
    }
}
