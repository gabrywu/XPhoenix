package org.apache.phoenix.external.schema;

import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 * 默认的外部元数据客户端，只提供打印日志功能
 */
public class LoggingExternalMetaDataClient extends ExternalMetaDataClient {
    private static Logger log = LoggerFactory.getLogger(LoggingExternalMetaDataClient.class);

    /**
     * 获取当前ExternalMetaDataClient的type
     * 主要用于区分各个ExternalMetaDataClient子类
     *
     * @return 当前ExternalMetaDataClient的type
     */
    @Override
    public String getType() {
        return "log";
    }

    @Override
    public void initialize(MetaDataClient client) {
        super.initialize(client);
        PName tenantId = connection.getTenantId();
        log.info("initialize with client {},tenantId {},config {}",client, tenantId ==null?"": tenantId.getString(),configuration);
    }

    @Override
    public void close() {
        log.info("close");
        try {
            super.close();
        } catch (SQLException e) {
            log.error(e.getMessage(),e);
        }
    }

    @Override
    public void createIndex(final PTable indexTable, final Map<String,Object> tableProps) throws SQLException {
        super.createIndex(indexTable,tableProps);
        log.info("create index with {}",indexTable);
    }

    @Override
    public void dropIndex(PTable indexTable) throws SQLException {
        log.info("drop index {}",indexTable);
    }

    @Override
    public MutationState buildIndex(PTable index, TableRef dataTableRef) {
        log.info("build index with {} for {}",index,dataTableRef.getTable().getName());
        return new MutationState(0, 0, connection);
    }

    @Override
    public ExternalMetaDataClient newInstance() {
        return new LoggingExternalMetaDataClient();
    }

    @Override
    public MutationState buildIndexAtTimeStamp(PTable index, NamedTableNode dataTableNode) {
        log.info("build index at timestamp with {} for {}",index,dataTableNode.getName());
        return new MutationState(0, 0, connection);
    }
}
