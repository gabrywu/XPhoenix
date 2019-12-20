package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.aggregator.ServerDirectAggregators;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

public abstract class AbstractElasticSearchResultIterator<T extends ActionRequest> implements PeekingResultIterator {
    private static Logger logger = LoggerFactory.getLogger(AbstractElasticSearchResultIterator.class);
    public final static String INDEX_DOC_ID_FIELD_NAME = "_id";

    protected final RestHighLevelClient elasticClient;
    protected T actionRequest;
    protected final TupleProjector tupleProjector;
    protected final LinkedHashMap<String, PColumn> allColumns;
    protected final String query;
    protected final String indexName;
    protected final PTable indexTable;
    protected final TupleProjector rowTupleProjector;
    protected final ExpressionCompiler expressionCompiler;
    protected final ServerAggregators serverAggregators;
    protected final  LinkedHashMap<String,SortOrder> orderBy;
    public AbstractElasticSearchResultIterator(final ConfigurationReadableOnlyWrapper configuration,
                                               final ExpressionCompiler expressionCompiler ,
                                               final String indexName,
                                               final String query,
                                               final  LinkedHashMap<String,SortOrder> orderBy,
                                               final TupleProjector tupleProjector,
                                               final PTable indexTable,
                                               final RowProjector rowProjector,
                                               final ServerAggregators serverAggregators){
        this.indexName = indexName;
        this.expressionCompiler = expressionCompiler;
        this.elasticClient = ElasticSearchClient.getSingleton(configuration).getSingletonClient();
        this.query = query;
        this.tupleProjector = tupleProjector;
        this.indexTable = indexTable;
        this.rowTupleProjector = ExternalUtil.getTupleProjectorNonKeyColumn(rowProjector);
        this.allColumns = ExternalUtil.getColumnNameMap(indexTable);
        this.serverAggregators = ServerDirectAggregators.from( serverAggregators );
        this.orderBy = orderBy;
        logger.debug("indexName {},allColumns {},query {},tupleProjector {},rowProjector {},serverAggregators {},expressionCompiler {},orderBy {}",indexName,allColumns,
                query,tupleProjector,rowProjector,serverAggregators,expressionCompiler,orderBy);
    }

    /**
     * 构建ElasticSearch的SearchSourceBuilder
     * @return 构建好的SearchSourceBuilder
     */
    public abstract SearchSourceBuilder buildSearchSourceBuilder();

    /**
     * 初始化
     * @return 当前AbstractElasticSearchResultIterator实例
     * @throws SQLException 异常信息
     */
    public abstract AbstractElasticSearchResultIterator initialize() throws SQLException;
    @Override
    public void explain(List<String> planSteps) {
        logger.info("explain ElasticSearchPeekingResultIterator");
        for (String planStep:planSteps){
            logger.info(planStep);
        }
    }
    @Override
    public void close() throws SQLException {
    }
}
