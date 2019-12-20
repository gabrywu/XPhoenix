package org.apache.phoenix.external.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.hbase.index.write.IndexCommitter;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.*;

/**
 * 外部索引提交类
 * 协处理器中使用
 */
public abstract class ExternalWriterIndexCommitter implements IndexCommitter {
    private static final Logger logger = LoggerFactory.getLogger(ExternalWriterIndexCommitter.class);
    private static final int CACHE_TTL = 3;
    private static final int CACHE_MAX_SIZE = 25;
    private AtomicBoolean stopped = new AtomicBoolean(false);
    protected ConfigurationReadableOnlyWrapper configuration;
    private boolean isNamespaceMapped;

    private HTableFactory retryingFactory;
    private HTableFactory noRetriesfactory;
    private Cache<String,PTable> tableCache;
    @Override
    public void setup(IndexWriter parent, RegionCoprocessorEnvironment env, String name){
        this.configuration = new ConfigurationReadableOnlyWrapper(env.getConfiguration()) ;
        this.retryingFactory = IndexWriterUtils.getDefaultDelegateHTableFactory(env);
        this.noRetriesfactory = IndexWriterUtils.getNoRetriesHTableFactory(env);
        String isNamespaceMappedStr = configuration.get(QueryServices.IS_NAMESPACE_MAPPING_ENABLED);
        this.isNamespaceMapped = isNamespaceMappedStr == null ?
                QueryServicesOptions.DEFAULT_IS_NAMESPACE_MAPPING_ENABLED:
                Boolean.parseBoolean(isNamespaceMappedStr);
        this.tableCache = CacheBuilder.newBuilder().
                expireAfterAccess(CACHE_TTL, TimeUnit.MINUTES).
                maximumSize(CACHE_MAX_SIZE).build();
    }
    private HTableFactory getHTableFactory(final int clientVersion){
        return clientVersion < MetaDataProtocol.MIN_CLIENT_RETRY_INDEX_WRITES ? retryingFactory : noRetriesfactory;
    }
    private PTable getTable(HTableInterface ht,final byte[] tenantId,final byte[] schemaName,final byte[] tableName,final long tableTimestamp ) throws Throwable {
        final Map<byte[], MetaDataProtos.MetaDataResponse> results =
                ht.coprocessorService(MetaDataProtos.MetaDataService.class,
                        null,null,
                        new Batch.Call<MetaDataProtos.MetaDataService, MetaDataProtos.MetaDataResponse>() {
                    @Override
                    public MetaDataProtos.MetaDataResponse call(MetaDataProtos.MetaDataService instance) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<MetaDataProtos.MetaDataResponse> rpcCallback =
                                new BlockingRpcCallback<MetaDataProtos.MetaDataResponse>();
                        MetaDataProtos.GetTableRequest.Builder builder = MetaDataProtos.GetTableRequest.newBuilder();
                        builder.setTenantId(ByteStringer.wrap(tenantId));
                        builder.setSchemaName(ByteStringer.wrap(schemaName));
                        builder.setTableName(ByteStringer.wrap(tableName));
                        builder.setTableTimestamp(tableTimestamp);
                        builder.setClientTimestamp(System.currentTimeMillis());
                        builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        instance.getTable(controller, builder.build(), rpcCallback);
                        if(controller.getFailedOn() != null) {
                            throw controller.getFailedOn();
                        }
                        return rpcCallback.get();
                    }
                });

        assert(results.size() == 1);
        MetaDataProtos.MetaDataResponse result = results.values().iterator().next();
        MetaDataMutationResult metaDataResult =  MetaDataMutationResult.constructFromProto(result);
        return metaDataResult.getTable();
    }

    /**
     * 获取HTableInterfaceReference对应的PTable
     * @param tableInterfaceReference 待查询的tableInterfaceReference
     * @param clientVersion 客户端版本
     * @return 查询到的PTable
     * @throws TableNotFoundException
     */
    protected PTable getTable(HTableInterfaceReference tableInterfaceReference,final int clientVersion) throws TableNotFoundException {
        long start = System.nanoTime();
        String tableNameFullName = tableInterfaceReference.getTableName();
        PTable lastCachedTable = tableCache.getIfPresent(tableNameFullName);
        PTable newerTable;
        try {
            final byte[] tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY ;
            final String schemaName = SchemaUtil.getSchemaNameFromFullName(tableNameFullName);
            final String tableName = SchemaUtil.getTableNameFromFullName (tableNameFullName);

            HTableFactory tableFactory = getHTableFactory(clientVersion);
            HTableInterface catalogHt = tableFactory.getTable(SchemaUtil.getPhysicalHBaseTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA_BYTES
                    ,PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES,isNamespaceMapped).getBytesPtr());

            newerTable = getTable(catalogHt,tenantIdBytes,
                    schemaName ==null? ByteUtil.EMPTY_BYTE_ARRAY :Bytes.toBytes(schemaName),
                    Bytes.toBytes(tableName),lastCachedTable == null ? MetaDataProtocol.MIN_TABLE_TIMESTAMP:lastCachedTable.getTimeStamp());
        } catch (Throwable e) {
            logger.error(e.getMessage(),e);
            throw new TableNotFoundException(tableNameFullName);
        }
        if(newerTable != null){
            tableCache.put(tableNameFullName,newerTable);
        }
        long end = System.nanoTime();
        logger.info("getTable {} took {} us",tableInterfaceReference.getTableName(),(end - start)/1000);
        return newerTable == null ? lastCachedTable : newerTable;
    }
    @Override
    public void stop(String why) {
        this.stopped.compareAndSet(false, true);
    }

   @Override
    public final void write(Multimap<HTableInterfaceReference, Mutation> toWrite, boolean allowLocalUpdates, int clientVersion) throws IndexWriteException{
       long start = System.nanoTime();
       batchWrite(toWrite,clientVersion);
       long end = System.nanoTime();
       logger.info("Batch {} records took time {} us",toWrite.size(),(end - start)/1000);
    }
    /**
     * @return True if {@link #stop(String)} has been closed.
     */
    @Override
    public boolean isStopped() {
        return this.stopped.get();
    }
    /**
     * 批量提交所以数据
     * @param toWrite 待写入数据
     * @param clientVersion 客户端版本
     * @throws IndexWriteException
     */
    public abstract void batchWrite(Multimap<HTableInterfaceReference, Mutation> toWrite, int clientVersion)
            throws IndexWriteException;
    /**
     * 创建IExternalBatch用以批量提交数据
     * @param indexTable 目标表
     * @return IExternalBatch
     */
    public abstract IExternalBatch createBatch(PTable indexTable);
}
