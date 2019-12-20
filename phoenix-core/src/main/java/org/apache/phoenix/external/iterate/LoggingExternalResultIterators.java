package org.apache.phoenix.external.iterate;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LoggingExternalResultIterators extends ExternalResultIterators {
    private static Logger logger = LoggerFactory.getLogger(LoggingExternalResultIterators.class);

    @Override
    public int size() {
        logger.info("size");
        return 0;
    }

    @Override
    public List<KeyRange> getSplits() {
        logger.info("getSplits");
        return Collections.emptyList();
    }

    @Override
    public List<List<Scan>> getScans() {
        logger.info("getScans");
        return Collections.emptyList();
    }

    @Override
    public void explain(List<String> planSteps) {
        logger.info("explain");
        for (String planStep:planSteps){
            logger.info(planStep);
        }
    }

    @Override
    public List<PeekingResultIterator> getIterators() throws SQLException {
        logger.info("getIterators");
        return new ArrayList<>(0);
    }

    @Override
    public void close() throws SQLException {
        logger.info("close");
    }

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
                           ServerAggregators serverAggregators) {
        logger.info("initialize for {},group {},order {},limit {},offset {},rowProjector {}",
                table,groupBy,orderBy,limit,offset,rowProjector);
    }
}
