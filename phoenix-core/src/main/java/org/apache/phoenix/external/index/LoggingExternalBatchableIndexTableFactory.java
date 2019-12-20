package org.apache.phoenix.external.index;

import org.apache.hadoop.hbase.client.Row;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

public class LoggingExternalBatchableIndexTableFactory extends ExternalIndexTableBatcherFactory {

    @Override
    public IExternalBatch createExternalIndexTableBatch(PTable indexTable) {
        return new LoggingExternalBatch(indexTable);
    }

    static class LoggingExternalBatch implements IExternalBatch<Void>{
        private static final Logger logger = LoggerFactory.getLogger(LoggingExternalBatch.class);
        private final PTable indexTable;
        public LoggingExternalBatch(PTable indexTable){
            this.indexTable = indexTable;
            logger.info("created");
        }
        @Override
        public void open() {
            logger.info("open");
        }

        @Override
        public void batch(List<? extends Row> actions) throws IOException, InterruptedException {
            logger.info("batch actions size {}",actions.size());
            for (Row r:actions){
                logger.info("batch row {}",r);
            }
        }

        @Override
        public Future<Void> batchAsync(List<? extends Row> actions) throws IOException, InterruptedException {
            logger.info("batch actions size {}",actions.size());
            for (Row r:actions){
                logger.info("batch row {}",r);
            }
            return null;
        }

        @Override
        public void close() {
            logger.info("close");
        }
    }
}
