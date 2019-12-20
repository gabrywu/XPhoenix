package org.apache.phoenix.external.index.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.external.ExternalConstants;
import org.apache.phoenix.external.schema.ExternalMetaDataClient;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixResultSetMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;

public class ElasticSearchMetaDataClient extends ExternalMetaDataClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMetaDataClient.class);
    private WriteRequest.RefreshPolicy refreshPolicy;
    private int maxConnection = 1;
    private List<Future<?>> waitingResult;
    /**
     * 获取当前ExternalMetaDataClient的type
     * 主要用于区分各个ExternalMetaDataClient子类
     *
     * @return 当前ExternalMetaDataClient的type
     */
    @Override
    public String getType() {
        return "ElasticSearch";
    }

    @Override
    public void initialize(MetaDataClient client) {
        if(!initialized.get()){
            super.initialize(client);
            logger.debug("initialize with {}",client);
            refreshPolicy = EsUtils.getRefreshPolicy(configuration);
            String maxConnectionStr = configuration.get(ElasticSearchConstants.CLIENT_MAX_CONNECTION_PROP);
            if(maxConnectionStr != null){
                maxConnection = Integer.parseInt(maxConnectionStr);
            }
            waitingResult = new ArrayList<>(maxConnection);
        }
    }
    @Override
    public void close() {
        logger.info("close elastic search client");
        try {
            super.close();
            ElasticSearchClient.getSingleton(configuration).closeSingletonClient();
        } catch (SQLException e) {
            logger.warn("close elastic search error {}",e.getMessage(),e);
        }
    }

    private void dropEsIndexIfExists(RestHighLevelClient elasticClient ,String esIndexName) throws SQLException{
        dropEsIndex(elasticClient,esIndexName,true);
    }
    private void dropEsIndex(RestHighLevelClient elasticClient ,String esIndexName,boolean ifExists) throws SQLException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(esIndexName);
        try {
            elasticClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
        } catch (Exception e) {
            if(!ifExists){
                throw new SQLException(e.getMessage(),e);
            }
        }
    }

    @Override
    public void createIndex(final PTable indexTable, final Map<String,Object> tableProps) throws SQLException {
        super.createIndex(indexTable,tableProps);
        Settings.Builder settingsBuilder = EsUtils.createIndexSettingsBuilder(tableProps);
        Map<String,String> mappingsParameters = EsUtils.getMappingsParameters(tableProps);

        String indexName = EsUtils.getEsIndexName(indexTable);
        logger.debug("create index [{}] with {}",indexName,indexTable.getTableName().getString());
        RestHighLevelClient elasticClient = ElasticSearchClient.getSingleton(configuration).getSingletonClient();
        if(logger.isDebugEnabled()){
            logger.error("dropEsIndexIfExists just for test");
            dropEsIndexIfExists(elasticClient,EsUtils.getEsIndexName(indexTable));
        }
        JSONObject fields = new JSONObject();
        List<PColumn> columns = indexTable.getColumns();

        for (int i = 0; i < columns.size(); i++) {
            String esName = ExternalUtil.getDataColumnName( columns.get(i) );
            JSONObject esType = ElasticSearchFieldType.from(columns.get(i).getDataType());
            fields.put(esName,esType);
            Pair<String,JSONObject> alias = ElasticSearchFieldType.alias(esName,esType);
            fields.put(alias.getFirst(),alias.getSecond());
        }
        fields.put(ExternalConstants.EXTERNAL_QUERY_FIELD,
                ElasticSearchFieldType.defaultType());

        JSONObject mappings = new JSONObject();
        mappings.put("properties",fields);
        mappings.putAll(mappingsParameters);

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(settingsBuilder);
        request.mapping(mappings);
        logger.info("create index {} for mapping {}",indexName, JSON.toJSON(mappings));
        logger.info("create index {} with settings {}",indexName,settingsBuilder.build().toString());
        try {
            CreateIndexResponse createIndexResponse = elasticClient.indices().create(request, RequestOptions.DEFAULT);
            logger.debug("CreateIndexResponse isAcknowledged: {}",createIndexResponse.isAcknowledged());
        } catch (IOException e) {
            logger.warn(e.getMessage(),e);
            throw new SQLException(e.getMessage(),e);
        }
    }

    @Override
    public void dropIndex(PTable indexTable) throws SQLException {
        dropEsIndexIfExists(ElasticSearchClient.getSingleton(configuration).getSingletonClient(),EsUtils.getEsIndexName(indexTable));
    }

    @Override
    public MutationState buildIndex(PTable indexTable, TableRef dataTableRef) throws SQLException {

        final PTable dataTable = dataTableRef.getTable();
        LinkedHashMap<String,PColumn> columns = ExternalUtil.getColumnNameMap(indexTable);
        LinkedHashMap<String,PColumn> pkColumns =  ExternalUtil.getPkColumnNameMap(indexTable);
        List<String> columnNames = new ArrayList<>(columns.keySet());

        int batchSize = ExternalUtil.getBatchSize(configuration);
        final RestHighLevelClient elasticClient = ElasticSearchClient.getSingleton(configuration).getSingletonClient();
        int counter = 0;

        try(final PhoenixStatement statement = (PhoenixStatement)connection.createStatement();
            final PhoenixResultSet resultSet = this.queryIndex(statement,indexTable,dataTable)){
            BulkRequest bulkRequest = EsUtils.createBulkRequest(refreshPolicy);
            PhoenixResultSetMetaData metaData = (PhoenixResultSetMetaData) resultSet.getMetaData();

            while (resultSet.next()){
                ImmutableBytesPtr ptr = new ImmutableBytesPtr();

                String docId = ExternalUtil.getRowKeyHexString(resultSet,
                        indexTable,
                        pkColumns,
                        ptr);

                Map<String, Object> fields = new HashMap<>(columns.size());
                for (int i = 1; i <= columns.size(); i++) {
                    if(!pkColumns.containsKey(columnNames.get(i-1))){
                        Object convertedValue = ElasticSearchFieldType.toReadableValue(
                                resultSet.getObject(i),metaData.getColumnTypeName(i),
                                columns.get(columnNames.get(i-1)).getDataType());

                        fields.put(columnNames.get(i-1),convertedValue);
                    }
                }
                logger.debug("buildIndex docId {} ,fields {},pkColumns {}",docId,fields,pkColumns);
                IndexRequest indexRequest = new IndexRequest(EsUtils.getEsIndexName(indexTable))
                        .id(docId).source(fields);
                bulkRequest.add(indexRequest);
                counter++;
                if ( counter % batchSize == 0 ){
                    // todo 2019-09-06 18:58:49 将批量提交改成异步批量提交，以节约时间，但要控制并发量，最好有反压机制
                    //  这里并发不涉及多个rowkey相互覆盖的问题，因为查询时候，每一个rowkey只会有一个记录
                    Future<?> bulkResponseFuture = EsUtils.bulkRequestAsync(elasticClient,bulkRequest);
                    waitingResult.add(bulkResponseFuture);
                    bulkRequest = EsUtils.createBulkRequest(refreshPolicy);
                }
                if(waitingResult.size()>maxConnection){
                    EsUtils.waitAllWithNoException(waitingResult);
                    waitingResult.clear();
                }
            }
            if(!bulkRequest.requests().isEmpty()){
                Future<?> bulkResponseFuture = EsUtils.bulkRequestAsync(elasticClient,bulkRequest);
                waitingResult.add(bulkResponseFuture);
                EsUtils.waitAllWithNoException(waitingResult);
                waitingResult.clear();
            }
        }
        enableIndex(indexTable,dataTableRef);
        return new MutationState(0,0,connection,counter);
    }

    @Override
    public ExternalMetaDataClient newInstance() {
        return new ElasticSearchMetaDataClient();
    }

}
