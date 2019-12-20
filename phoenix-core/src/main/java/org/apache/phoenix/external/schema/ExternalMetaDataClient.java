package org.apache.phoenix.external.schema;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 外部元数据客户端
 */
public abstract class ExternalMetaDataClient {
    private static Logger logger = LoggerFactory.getLogger(ExternalMetaDataClient.class);
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    protected MetaDataClient client;
    protected PhoenixConnection connection;
    protected ConfigurationReadableOnlyWrapper configuration;
    protected AtomicBoolean initialized = new AtomicBoolean(false);
    /**
     * 获取当前ExternalMetaDataClient的type
     * 主要用于区分各个ExternalMetaDataClient子类
     * @return 当前ExternalMetaDataClient的type
     */
    public abstract String getType();
    /**
     * 外部元数据客户端初始化
     * @param client Phoenix元数据客户端
     */
    public void initialize(MetaDataClient client){
        this.client = client;
        this.connection = client.getConnection();
        this.configuration = new ConfigurationReadableOnlyWrapper(connection.getQueryServices().getConfiguration()) ;
        this.initialized.compareAndSet(false, true);
    }

    /**
     * 释放资源
     */
    public void close() throws SQLException {
        connection.close();
    }

    /**
     * 创建索引
     * @param indexTable 当前PTable
     * @throws SQLException 异常信息
     */
    public void createIndex(final PTable indexTable, final Map<String,Object> tableProps) throws SQLException{
    }
    public abstract void dropIndex(PTable indexTable) throws SQLException;
    /**
     * 根据数据表，构建索引数据
     * @param index 索引表
     * @param dataTableRef 数据表
     * @return MutationState
     * @throws SQLException 异常信息
     */
    public abstract MutationState buildIndex(PTable index, TableRef dataTableRef) throws SQLException;

    /**
     * 创建ExternalMetaDataClient子类实例
     * @return ExternalMetaDataClient
     */
    public abstract ExternalMetaDataClient newInstance();
    /**
     * 根据数据表、时间戳，构建索引数据
     * @param index 索引表
     * @param dataTableNode 数据表
     * @return MutationState
     * @throws SQLException 异常信息
     */
    public MutationState buildIndexAtTimeStamp(PTable index, NamedTableNode dataTableNode) throws SQLException{
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, Long.toString(connection.getSCN()+1));
        PhoenixConnection conn = new PhoenixConnection(connection, connection.getQueryServices(), props);
        ExternalMetaDataClient elasticsearchMetaDataClient = newInstance();
        elasticsearchMetaDataClient.initialize(new MetaDataClient(conn));
        conn.setAutoCommit(true);
        ColumnResolver resolver = FromCompiler.getResolver(dataTableNode, conn);
        TableRef tableRef = resolver.getTables().get(0);
        boolean success = false;
        SQLException sqlException = null;
        try {
            MutationState state = elasticsearchMetaDataClient.buildIndex(index, tableRef);
            success = true;
            return state;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                elasticsearchMetaDataClient.close();
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * 将索引设置为ACTIVE
     * @param index 待设置的index
     * @param dataTableRef 带设置的index对应的dataTable
     * @throws SQLException 设置失败时的异常信息
     */
    protected void enableIndex(final PTable index, final TableRef dataTableRef) throws SQLException {
        AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
                TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
        client.alterIndex(indexStatement);
    }

    /**
     * 是否支持该数据表。目前不支持事务表
     * @param dataTable 数据表
     * @return true支持；false不支持
     */
    public boolean isSupported(final PTable dataTable){
        return !dataTable.isTransactional();
    }

    /**
     * 查询索引对应数据表数据
     * @param statement 执行SQL的statement
     * @param indexTable 索引表
     * @param dataTable 索引表对应的源数据表
     * @return 数据表数据
     * @throws SQLException SQL异常
     */
    protected PhoenixResultSet queryIndex(final PhoenixStatement statement, final PTable indexTable, final PTable dataTable) throws SQLException {
        return ExternalUtil.queryIndex(statement,indexTable,dataTable);
    }
}
