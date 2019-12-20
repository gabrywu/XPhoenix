package org.apache.phoenix.external.index.elasticsearch;

import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.external.index.ExternalWriterIndexCommitter;
import org.apache.phoenix.external.index.IExternalBatch;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class ElasticSearchWriterIndexCommitter extends ExternalWriterIndexCommitter {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchWriterIndexCommitter.class);
    private int maxConnection = 1;
    private List<Future<?>> waitingResult;
    @Override
    public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name) {
        logger.debug("setup ElasticSearchWriterIndexCommitter, {},{},{}",parent,env,name);
        super.setup(parent,env,name);
        String maxConnectionStr = configuration.get(ElasticSearchConstants.CLIENT_MAX_CONNECTION_PROP);
        if(maxConnectionStr != null){
            maxConnection = Integer.parseInt(maxConnectionStr);
        }
        waitingResult = new ArrayList<>(maxConnection);
    }

    /**
     * Stop this service.
     * Implementers should favor logging errors over throwing RuntimeExceptions.
     *
     * @param why Why we're stopping.
     */
    @Override
    public void stop(String why) {
        if(isStopped()){
            return;
        }
        ElasticSearchClient.getSingleton(configuration).closeSingletonClient();;
        logger.debug("ElasticSearchWriterIndexCommitter stop {}",why);
        super.stop(why);
    }
    /**
     * 批量提交所以数据
     * @param toWrite 待写入数据
     * @param clientVersion 客户端版本
     * @throws IndexWriteException
     */
    public final void batchWrite(Multimap<HTableInterfaceReference, Mutation> toWrite, int clientVersion)
            throws IndexWriteException{
        logger.debug("ExternalWriterIndexCommitter write {}",toWrite.size());
        List<Throwable> failures = new ArrayList<>(toWrite.keySet().size());
        for (HTableInterfaceReference index:toWrite.keySet()){
            IExternalBatch batch = null;
            try{
                if(waitingResult.size() > maxConnection){
                    EsUtils.waitAllWithNoException(waitingResult);
                    waitingResult.clear();
                }
                // todo 2019-08-05 09:20:05 写入逻辑可以放到线程池，每个表分配一个线程。
                //  同一个表不能分开放到不同的线程，因为需要考虑同一个rowkey的更新覆盖的问题。
                //  比如同一个数据的delete和put，可能会被分到不同的线程
                PTable indexTable = this.getTable(index,clientVersion);
                batch = createBatch(indexTable);
                batch.open();
                // todo 2019-09-06 18:58:49 将批量提交改成异步批量提交，以节约时间，但要控制并发量，最好有反压机制
                Future<?> bulkResponseFuture = batch.batchAsync((List<? extends Row>) toWrite.get(index));
                waitingResult.add(bulkResponseFuture);
            }catch (IOException | SQLException | InterruptedException e){
                failures.add(e);
                logger.error(e.getMessage(),e);
            } finally {
                ExternalUtil.closeSilent(batch);
            }
        }
        if (!failures.isEmpty()){
            throw new IndexWriteException(failures.get(0),false);
        }
    }

    @Override
    public IExternalBatch createBatch(PTable indexTable) {
        return new ElasticSearchBatch(configuration,indexTable);
    }

}
