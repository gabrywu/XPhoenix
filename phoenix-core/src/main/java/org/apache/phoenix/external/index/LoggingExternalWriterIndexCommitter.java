package org.apache.phoenix.external.index;

import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExternalWriterIndexCommitter extends ExternalWriterIndexCommitter {
    private static Logger logger = LoggerFactory.getLogger(LoggingExternalWriterIndexCommitter.class);
    @Override
    public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name) {
        logger.info("setup external writer committer for {},{} with {}",name,parent,env);
    }

    /**
     * 批量提交所以数据
     *
     * @param toWrite       待写入数据
     * @param clientVersion 客户端版本
     * @throws IndexWriteException
     */
    @Override
    public void batchWrite(Multimap<HTableInterfaceReference, Mutation> toWrite, int clientVersion) throws IndexWriteException {
        logger.info("batch write client version {}",clientVersion);
        for (HTableInterfaceReference index:toWrite.keySet()){
            logger.info("index table {},mutations {}",index.getTableName(),toWrite.get(index));
        }
    }

    @Override
    public IExternalBatch createBatch(PTable indexTable) {
        return new LoggingExternalBatchableIndexTableFactory.LoggingExternalBatch(indexTable);
    }

}
