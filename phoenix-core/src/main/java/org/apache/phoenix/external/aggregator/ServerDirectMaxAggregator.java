package org.apache.phoenix.external.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.aggregator.MaxAggregator;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;

public class ServerDirectMaxAggregator extends MaxAggregator {
    public static byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_M");
    public static byte[] VALUE_COLUMN_QUALIFIER = QueryConstants.VALUE_COLUMN_QUALIFIER;
    private ServerDirectAggregator serverDirectAggregator;
    private PDataType inputDataType;
    private Integer maxLength;
    public ServerDirectMaxAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }
    public ServerDirectMaxAggregator(SortOrder sortOrder,PDataType inputDataType,Integer maxLength) {
        this(sortOrder);
        this.inputDataType = inputDataType;
        this.maxLength = maxLength;
        serverDirectAggregator = new ServerDirectAggregator(VALUE_COLUMN_FAMILY,VALUE_COLUMN_QUALIFIER,getDataType());
    }
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        Object result = serverDirectAggregator.aggregate(tuple,ptr);
        if( result != null ){
            value.set(ptr.get(),ptr.getOffset(),ptr.getLength());
        }
    }
    /**
     * @return data type of the column
     */
    @Override
    public PDataType getDataType() {
        return inputDataType;
    }
    @Override
    public Integer getMaxLength() {
        return maxLength;
    }
}
