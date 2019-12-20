package org.apache.phoenix.external.index.elasticsearch;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.index.IExternalBatch;
import org.apache.phoenix.external.store.ExternalIndexStore;
import org.apache.phoenix.external.store.ExternalIndexStoreManager;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Future;

public class ElasticSearchBatch implements IExternalBatch<BulkResponse> {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBatch.class);
    private final ConfigurationReadableOnlyWrapper configuration;
    private final PTable indexTable;
    private final boolean usesEncodedColumnNames;
    private boolean opened;
    private RestHighLevelClient elasticClient;
    private final WriteRequest.RefreshPolicy refreshPolicy;
    public ElasticSearchBatch(ConfigurationReadableOnlyWrapper configuration, PTable indexTable){
        this.configuration = configuration;
        this.indexTable = indexTable;
        this.refreshPolicy = EsUtils.getRefreshPolicy(configuration);
        this.usesEncodedColumnNames = EncodedColumnsUtil.usesEncodedColumnNames(indexTable);
        logger.debug("ElasticSearchBatch created with {} for {}",configuration,indexTable);
    }
    @Override
    public void open() {
        if(!opened){
            elasticClient = ElasticSearchClient.getSingleton(configuration).getSingletonClient();
        }
        opened = true;
    }
    private BulkRequest convert2BulkRequest(final List<? extends Row> actions) throws SQLException{
        BulkRequest bulkRequest = EsUtils.createBulkRequest(refreshPolicy);
        String indexName = EsUtils.getEsIndexName(indexTable);
        for (int i = 0; i < actions.size(); i++) {
            Mutation mutation = (Mutation) actions.get(i);
            ExternalIndexStore store = ExternalIndexStoreManager.convert(indexTable,mutation,usesEncodedColumnNames);
            logger.debug("batch store {}",store);
            DocWriteRequest docWriteRequest;
            if(ExternalIndexStore.Type.Delete == store.getType()){
                docWriteRequest = new DeleteRequest(indexName)
                        .id(store.getRowKey());
            }else{
                docWriteRequest = new IndexRequest(indexName)
                        .id(store.getRowKey()).source(store.getFields());
            }
            logger.debug("DocWriteRequest {}",docWriteRequest);
            bulkRequest.add(docWriteRequest);
        }
        return bulkRequest;
    }
    @Override
    public void batch(final List<? extends Row> actions) throws IOException,InterruptedException, SQLException {
        try {
            BulkRequest bulkRequest = convert2BulkRequest(actions);

            BulkResponse response = EsUtils.bulkRequest(elasticClient,bulkRequest);
            logger.info("Bulk {} records took {} micros,ingest took {}",
                    bulkRequest.requests().size(),
                    response.getTook().getMicros(),
                    response.getIngestTook().getMicros());
            bulkRequest.requests().clear();
        } catch (AmbiguousColumnException | ColumnNotFoundException e) {
            throw new IOException(e.getMessage(),e);
        }
    }

    @Override
    public Future<BulkResponse> batchAsync(final List<? extends Row> actions) throws IOException, InterruptedException, SQLException {
        final BulkRequest bulkRequest = convert2BulkRequest(actions);
        return EsUtils.bulkRequestAsync(elasticClient,bulkRequest);
    }

    @Override
    public void close() {
        logger.debug("ElasticSearchBatch close");
    }
}