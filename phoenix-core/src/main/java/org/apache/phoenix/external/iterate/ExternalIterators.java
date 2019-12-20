package org.apache.phoenix.external.iterate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.external.ExternalConstants;
import org.apache.phoenix.external.utils.ExternalClassFactory;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

public class ExternalIterators extends BaseResultIterators {
    private static Logger logger = LoggerFactory.getLogger(ExternalIterators.class);
    private ExternalResultIterators delegate;
    private boolean initFirstScanOnly;
    public ExternalIterators(QueryPlan plan, Integer perScanLimit,
                             ParallelScanGrouper scanGrouper,
                             Scan scan,
                             boolean initFirstScanOnly,
                             Map<ImmutableBytesPtr,ServerCacheClient.ServerCache> caches,
                             QueryPlan dataPlan)
            throws SQLException {
        this(plan, perScanLimit, null, scanGrouper, scan,caches, dataPlan);
        this.initFirstScanOnly = initFirstScanOnly;
    }
    private ExternalIterators(QueryPlan plan,
                              Integer perScanLimit,
                              Integer offset,
                              ParallelScanGrouper scanGrouper,
                              Scan scan,
                              Map<ImmutableBytesPtr, ServerCacheClient.ServerCache> caches,
                              QueryPlan dataPlan) throws SQLException {
        super(plan, perScanLimit, offset, scanGrouper, scan, caches, dataPlan);
        Configuration configuration = context.getConnection().getQueryServices().getConfiguration();
        String delegateClassName = configuration.get(ExternalConstants.EXTERNAL_RESULT_ITERATORS_CLASS_KEY,
                ExternalConstants.DEFAULT_EXTERNAL_RESULT_ITERATORS_CLASS);

        TupleProjector tupleProjector = TupleProjector.deserializeProjectorFromScan(context.getScan());

        if(orderBy.getOrderByExpressions().isEmpty()){
            orderBy =  OrderByCompiler.compile(context, (SelectStatement) plan.getStatement(), groupBy, limit, offset, plan.getProjector(),
                    groupBy == GroupByCompiler.GroupBy.EMPTY_GROUP_BY ? tupleProjector: null, false);
        }

        logger.debug("create ExternalResultIterators[{}] with plan {}, table {}, group {}, order {},hint {}, limit {},offset {}, initFirstScanOnly {} ",
                delegateClassName,plan.getStatement(),plan.getTableRef().getTable(),groupBy,
                orderBy.getOrderByExpressions(),plan.getStatement().getHint(),limit,offset,initFirstScanOnly);

        delegate = ExternalClassFactory.getInstance(delegateClassName, ExternalResultIterators.class);
        SelectStatement selectStatement = (SelectStatement) plan.getStatement();
        if( ExternalUtil.isInvalidExternalQuery(selectStatement) ){
            throw new IllegalArgumentException("External Index query invalid " + selectStatement.getWhere().toString());
        }

        ServerAggregators serverAggregators = selectStatement.isAggregate() ?
                ServerAggregators.deserialize(scan .getAttribute(BaseScannerRegionObserver.AGGREGATORS), configuration, null)
                :null;

        logger.debug("Server Aggregator {}",serverAggregators);

        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        delegate.initialize(ExternalUtil.getReadOnlyConfiguration(configuration),
                expressionCompiler,
                selectStatement.isAggregate(),
                selectStatement.getSelect(),
                plan.getTableRef().getTable(),
                ExternalUtil.getExternalQuery(selectStatement),
                groupBy,
                orderBy,
                limit,
                offset,
                tupleProjector,
                plan.getProjector(),
                serverAggregators);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    protected boolean isSerial() {
        return false;
    }

    @Override
    public List<KeyRange> getSplits() {
        return delegate.getSplits();
    }

    @Override
    public List<List<Scan>> getScans() {
        return delegate.getScans();
    }

    @Override
    public void explain(List<String> planSteps) {
        boolean displayChunkCount = context.getConnection().getQueryServices().getProps().getBoolean(
                QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB,
                QueryServicesOptions.DEFAULT_EXPLAIN_CHUNK_COUNT);
        StringBuilder buf = new StringBuilder();
        buf.append("CLIENT ");
        if (displayChunkCount) {
            boolean displayRowCount = context.getConnection().getQueryServices().getProps().getBoolean(
                    QueryServices.EXPLAIN_ROW_COUNT_ATTRIB,
                    QueryServicesOptions.DEFAULT_EXPLAIN_ROW_COUNT);
            buf.append(getSplits().size()).append("-CHUNK ");
            if (displayRowCount && delegate.getEstimatedRows() != null) {
                buf.append(delegate.getEstimatedRows()).append(" ROWS ");
                buf.append(delegate.getEstimatedSize()).append(" BYTES ");
            }
        }
        buf.append(getName()).append(" ").append(size()).append("-WAY ");

        if(this.plan.getStatement().getTableSamplingRate()!=null){
            buf.append(plan.getStatement().getTableSamplingRate()/100D).append("-").append("SAMPLED ");
        }
        try {
            if (plan.useRoundRobinIterator()) {
                buf.append("ROUND ROBIN ");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        explain(buf.toString(),planSteps);
    }

    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        return delegate.getIterators();
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    protected String getName() {
        return "EXTERNAL PARALLEL";
    }

    @Override
    protected void submitWork(List<List<Scan>> nestedScans,
                              List<List<Pair<Scan, Future<PeekingResultIterator>>>> nestedFutures,
                              Queue<PeekingResultIterator> allIterators,
                              int estFlattenedSize,
                              boolean isReverse,
                              ParallelScanGrouper scanGrouper) throws SQLException {
        logger.info("submitWork");
    }
}
