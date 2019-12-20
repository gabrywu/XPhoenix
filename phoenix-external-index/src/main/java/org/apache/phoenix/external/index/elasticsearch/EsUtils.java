package org.apache.phoenix.external.index.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.store.ExternalIndexStore;
import org.apache.phoenix.jdbc.PhoenixResultSetMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class EsUtils {
    private static Logger logger = LoggerFactory.getLogger(EsUtils.class);
    private static BulkResponse EMPTY_BULK_RESPONSE = new BulkResponse(new BulkItemResponse[]{},0);
    static String getEsIndexName(final PTable indexTable){
        return SchemaUtil.getUnEscapedFullColumnName(getEsIndexName(indexTable.getTableName().getString()));
    }
    static String getEsIndexName(final String tableName){
        return tableName.toLowerCase();
    }

    static RestHighLevelClient createClient( final ConfigurationReadableOnlyWrapper configuration ){
        String[] clusterNodes = StringUtils.getStrings(configuration.get(ElasticSearchConstants.CLUSTER_NODES));
        HttpHost[] clusterHttpHosts = new HttpHost[clusterNodes.length];
        for (int i = 0; i < clusterNodes.length; i++) {
            clusterHttpHosts[i] = HttpHost.create(clusterNodes[i]);
        }
        return new RestHighLevelClient(RestClient.builder(clusterHttpHosts).setHttpClientConfigCallback(httpClientBuilder -> {
            String maxConnection = configuration.get(ElasticSearchConstants.CLIENT_MAX_CONNECTION_PROP);
            if(maxConnection != null){
                httpClientBuilder.setMaxConnTotal(Integer.parseInt(maxConnection));
            }
            String maxConnectionPerRoute = configuration.get(ElasticSearchConstants.CLIENT_MAX_CONNECTION_PER_ROUTE_PROP);
            if(maxConnectionPerRoute != null){
                httpClientBuilder.setMaxConnPerRoute(Integer.parseInt(maxConnectionPerRoute));
            }
            return httpClientBuilder;
        }) );
    }
    static ExternalIndexStore toExternalStore(final SearchHit hit){
        ExternalIndexStore store = new ExternalIndexStore(hit.getId());
        store.setFields(hit.getSourceAsMap());
        return store;
    }
    private static boolean isDefaultPolicy(WriteRequest.RefreshPolicy policy){
        return ElasticSearchConstants.DEFAULT_REFRESH_POLICY == policy;
    }
    static WriteRequest.RefreshPolicy getRefreshPolicy(ConfigurationReadableOnlyWrapper configuration){
        String policyName = configuration.get(ElasticSearchConstants.REFRESH_POLICY_PROP);
        WriteRequest.RefreshPolicy policy = ElasticSearchConstants.DEFAULT_REFRESH_POLICY;
        for (WriteRequest.RefreshPolicy p:WriteRequest.RefreshPolicy.values()){
            if(p.name().equals(policyName)){
                policy = p;
                break;
            }
        }
        if(isDefaultPolicy(policy)){
            logger.warn("Using default policy {}",policy);
        }
        return policy;
    }

    static BulkRequest createBulkRequest(WriteRequest.RefreshPolicy refreshPolicy){
        return new BulkRequest().setRefreshPolicy(refreshPolicy);
    }

    static void logMetadata(String query, PhoenixResultSetMetaData metaData) throws SQLException {
        logger.info("query's meta data: {}",query);
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            sb.append(metaData.getColumnLabel(i)).append("\t");
        }
        sb.append("\n");
        logger.info(sb.toString());
    }
    static void printAggregations(Aggregations aggregations){
        try{
            logger.info("Aggregations {}", JSON.toJSONString(aggregations,true));
        }catch (JSONException ignore){

        }
    }
    static SortOrder toEsSortOrder(org.apache.phoenix.schema.SortOrder sortOrder){
        return sortOrder == org.apache.phoenix.schema.SortOrder.ASC ? SortOrder.ASC: SortOrder.DESC;
    }
    static Future<BulkResponse> bulkRequestAsync(final RestHighLevelClient elasticClient,
                                                 final BulkRequest bulkRequest){
        CompletableFuture<BulkResponse> responseCompleteFuture = new CompletableFuture<>();
        elasticClient.bulkAsync( bulkRequest,RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                responseCompleteFuture.complete(bulkItemResponses);
            }
            @Override
            public void onFailure(Exception e) {
                responseCompleteFuture.complete(EMPTY_BULK_RESPONSE);
            }
        });
        return responseCompleteFuture;
    }
    static BulkResponse bulkRequest(final RestHighLevelClient elasticClient, final BulkRequest bulkRequest) throws IOException {
        return elasticClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
    static SearchSourceBuilder buildSort(SearchSourceBuilder searchSourceBuilder,  LinkedHashMap<String,SortOrder> orderByColumnName){
        if(orderByColumnName != null){
            for (String columnName:orderByColumnName.keySet()){
                searchSourceBuilder.sort(ElasticSearchFieldType.getRawFieldName(columnName),
                        orderByColumnName.get(columnName));
            }
        }
        return searchSourceBuilder;
    }

    static String getUnquotedString(String quotedStr){
        return quotedStr.startsWith("\"")?quotedStr.substring(1,quotedStr.length()-1):quotedStr;
    }
    static Map<String,String> getMappingsParameters(final Map<String,Object> tableProps){
        Map<String, String> parameters = new HashMap<>(ElasticSearchConstants.DEFAULT_MAPPINGS_PARAMETERS);
        for (String prop:tableProps.keySet()){
            String lowerProp = prop.toLowerCase();
            if(lowerProp.startsWith(ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX)){
                parameters.put(lowerProp.replace(ElasticSearchConstants.ES_MAPPING_SETTINGS_PREFIX,""),
                        getUnquotedString(tableProps.get(prop).toString()));
            }
        }
        return parameters;
    }
    static Settings.Builder createIndexSettingsBuilder( final Map<String,Object> tableProps ){
        Settings.Builder builder = Settings.builder();
        for (String defProp:ElasticSearchConstants.DEFAULT_INDEX_SETTINGS.keySet()){
            builder.put(defProp,ElasticSearchConstants.DEFAULT_INDEX_SETTINGS.get(defProp).toString());
        }
        for (String prop:tableProps.keySet()){
            String lowerProp = prop.toLowerCase();
            if(lowerProp.startsWith(ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX)){
                builder.put(lowerProp.replace(ElasticSearchConstants.ES_INDEX_SETTINGS_PREFIX,""),
                        getUnquotedString(tableProps.get(prop).toString()));
            }
        }
        return builder;
    }
    static void waitAllWithNoException(List<Future<?>> futures){
        for (Future future:futures){
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }
    static void waitAllWithException(List<Future<?>> futures) throws ExecutionException, InterruptedException {
        for (Future future:futures){
            future.get();
        }
    }
}
