package org.apache.phoenix.external.iterate;

import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.iterate.ResultIterators;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.schema.PTable;

import java.sql.SQLException;
import java.util.List;

/**
 * 外部结果遍历器集合
 */
public abstract class ExternalResultIterators implements ResultIterators {
    protected ConfigurationReadableOnlyWrapper configuration;
    protected String externalQuery;
    protected Integer limit;
    protected Integer offset;
    protected TupleProjector tupleProjector;
    protected List<AliasedNode> select;
    protected boolean isAggregate;
    protected PTable indexTable;
    protected GroupByCompiler.GroupBy groupBy;
    protected OrderByCompiler.OrderBy orderBy;
    protected ExpressionCompiler expressionCompiler;
    protected RowProjector rowProjector;
    protected ServerAggregators serverAggregators;
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
                           ServerAggregators serverAggregators) throws SQLException{
        this.configuration = configuration;
        this.expressionCompiler = expressionCompiler;
        this.select = select;
        this.indexTable = table;
        this.externalQuery = externalQuery;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
        this.tupleProjector = tupleProjector;
        this.isAggregate = isAggregate;
        this.rowProjector = rowProjector;
        this.serverAggregators = serverAggregators;
    }
    public String getName(){
        return this.getClass().getSimpleName();
    }
    public Long getEstimatedRows(){
        return null;
    }
    public Long getEstimatedSize() {
        return null;
    }
    public void explain(List<String> planSteps){}
}
