package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.index.elasticsearch.base.ParallelStatsDisabledIT;
import org.apache.phoenix.external.schema.ExternalMetaDataClient;
import org.apache.phoenix.external.utils.ExternalClassFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.external.index.elasticsearch.base.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertNotNull;
public class ExternalMetaDataClientFactoryIT extends ParallelStatsDisabledIT {
    @Test
    public void test() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        ExternalMetaDataClient externalMetaDataClient = ExternalClassFactory.getInstance(
                "org.apache.phoenix.external.index.elasticsearch.ElasticSearchMetaDataClient",
                ExternalMetaDataClient.class);
        assertNotNull(externalMetaDataClient);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props)) {
            MetaDataClient client = new MetaDataClient(conn);
            externalMetaDataClient.initialize(client);
        }
    }
    @Test
    public void testNewElasticsearchMetaDataClient() throws SQLException{
        ElasticSearchMetaDataClient elasticsearchMetaDataClient = new ElasticSearchMetaDataClient();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props)) {
            MetaDataClient client = new MetaDataClient(conn);
            elasticsearchMetaDataClient.initialize(client);
        }
    }
}
