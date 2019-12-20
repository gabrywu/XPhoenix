package org.apache.phoenix.external.index.elasticsearch;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.*;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.iterate.ExternalResultIterators;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticSearchResultIterators extends ExternalResultIterators {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchResultIterators.class);
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private List<PeekingResultIterator> peekingResultIterators;

    @Override
    public void initialize(ConfigurationReadableOnlyWrapper configuration,
                           ExpressionCompiler expressionCompiler ,
                           boolean isAggregate,
                           List<AliasedNode> select,
                           PTable table,
                           String externalQuery,
                           GroupByCompiler.GroupBy groupBy,
                           OrderByCompiler.OrderBy orderBy,
                           Integer limit,
                           Integer offset,
                           TupleProjector tupleProjector,
                           RowProjector rowProjector,
                           ServerAggregators serverAggregators) throws SQLException {
        if(!this.initialized.compareAndSet(false, true)){
            logger.warn("ElasticSearchResultIterators already initialize");
            return;
        }
        super.initialize(configuration,expressionCompiler,isAggregate,select,
                table,externalQuery,groupBy,orderBy,limit,offset,tupleProjector,
                rowProjector,serverAggregators);

        if(logger.isDebugEnabled()){
            logger.debug("initialize {} for {},group {},order {},limit {},offset {}",
                    select,table,groupBy.getExpressions(),orderBy.getOrderByExpressions(),limit,offset);
            logger.debug("projectedTuple {},type {},rowProjector {},type {}",tupleProjector,tupleProjector==null?null:tupleProjector.getClass().getName()
                    ,rowProjector,rowProjector==null?null:rowProjector.getClass().getName());
            if( rowProjector != null ){
                for (ColumnProjector columnProjector:rowProjector.getColumnProjectors()){
                    logger.debug("columnProjector {},type {}",columnProjector,columnProjector.getExpression().getClass().getName());
                }
            }else{
                logger.warn("rowProjector is null");
            }
        }

        String indexName = EsUtils.getEsIndexName(table);

//        List<Pair<String, SortOrder>> orderByColumnName = new ArrayList<>(orderBy.getOrderByExpressions().size());
        LinkedHashMap<String,SortOrder> orderByColumnName = new LinkedHashMap<>(orderBy.getOrderByExpressions().size());
        for (OrderByExpression expression:orderBy.getOrderByExpressions()){
            String colName = ExternalUtil.getOrderByColumnName(expression);
            SortOrder sortOrder = EsUtils.toEsSortOrder( expression.getExpression().getSortOrder() );
            // orderByColumnName.add(new Pair<>(colName,sortOrder));
            orderByColumnName.put(colName,sortOrder);
        }

        AbstractElasticSearchResultIterator peekingResultIterator = isAggregate ?
                new ElasticSearchAggregatePeekingResultIterator(
                        configuration,
                        expressionCompiler,
                        indexName,
                        externalQuery,
                        groupBy,
                        orderByColumnName,
                        tupleProjector,
                        indexTable,rowProjector,serverAggregators ):
                new ElasticSearchPeekingResultIterator(
                        configuration,
                        expressionCompiler,
                        indexName,
                        externalQuery,
                        orderByColumnName,
                        offset,
                        limit,
                        tupleProjector,
                        indexTable,rowProjector);
        peekingResultIterator.initialize();

        peekingResultIterators = new ArrayList<>(1);
        peekingResultIterators.add(peekingResultIterator);
    }

    @Override
    public int size() {
        return Math.max(getScans().size(), 1);
    }

    @Override
    public List<KeyRange> getSplits() {
        return Collections.emptyList();
    }

    @Override
    public List<List<Scan>> getScans() {
        return Collections.emptyList();
    }

    @Override
    public void explain(List<String> planSteps) {
        logger.debug("explain ElasticSearchResultIterators");
        for (String planStep:planSteps){
            logger.info(planStep);
        }
    }

    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        return peekingResultIterators;
    }

    @Override
    public void close() throws SQLException {
        if(!initialized.get()){
            return;
        }
        for (PeekingResultIterator iterator:peekingResultIterators){
            iterator.close();
        }
        this.initialized.compareAndSet(true, false);
    }
}
