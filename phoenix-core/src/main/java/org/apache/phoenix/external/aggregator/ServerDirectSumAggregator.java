package org.apache.phoenix.external.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.aggregator.NumberSumAggregator;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDirectSumAggregator extends NumberSumAggregator {
    private static PDataType defaultType = PInteger.INSTANCE;
    private static final Logger logger = LoggerFactory.getLogger(ServerDirectSumAggregator.class);
    private PDataType inputDataType;
    public static byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_S");
    public static byte[] VALUE_COLUMN_QUALIFIER = QueryConstants.VALUE_COLUMN_QUALIFIER;
    private ServerDirectAggregator serverDirectAggregator;
    public ServerDirectSumAggregator(PDataType inputDataType,SortOrder sortOrder){
        this(sortOrder);
        this.inputDataType = inputDataType;
        this.serverDirectAggregator =  new ServerDirectAggregator(VALUE_COLUMN_FAMILY,VALUE_COLUMN_QUALIFIER,inputDataType);
    }
    public ServerDirectSumAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    @Override
    protected PDataType getInputDataType() {
        return inputDataType;
    }
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        Object result = serverDirectAggregator.aggregate(tuple,ptr);
        if( result != null ){
            sum = Long.parseLong( result.toString() );
            if (buffer == null) {
                initBuffer();
            }
        }
    }
}
