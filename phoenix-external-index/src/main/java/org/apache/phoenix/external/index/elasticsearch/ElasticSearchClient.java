package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchClient {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
    private static ElasticSearchClient elasticSearchClient = null;
    public static ElasticSearchClient getSingleton(ConfigurationReadableOnlyWrapper configuration){
        if(elasticSearchClient == null){
            elasticSearchClient = new ElasticSearchClient(configuration);
        }
        return elasticSearchClient;
    }
    private RestHighLevelClient globalElasticClient;
    private ElasticSearchClient(ConfigurationReadableOnlyWrapper configuration){
        globalElasticClient = EsUtils.createClient(configuration);
    }
    public RestHighLevelClient getSingletonClient(){
        return globalElasticClient;
    }
    public void closeSingletonClient(){
        logger.info("close globalElasticClient");
        ExternalUtil.closeSilent(globalElasticClient);
    }
}
