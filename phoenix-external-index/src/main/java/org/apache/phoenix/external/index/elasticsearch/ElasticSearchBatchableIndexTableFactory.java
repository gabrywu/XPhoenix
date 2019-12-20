package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.index.ExternalIndexTableBatcherFactory;
import org.apache.phoenix.external.index.IExternalBatch;
import org.apache.phoenix.schema.PTable;

public class ElasticSearchBatchableIndexTableFactory extends ExternalIndexTableBatcherFactory {

    @Override
    public IExternalBatch createExternalIndexTableBatch(PTable indexTable) {
        return new ElasticSearchBatch(configuration,indexTable);
    }
}
