package org.apache.phoenix.external.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDirectCountAggregator extends CountAggregator {
    private static PDataType defaultType = PInteger.INSTANCE;
    private static final Logger logger = LoggerFactory.getLogger(ServerDirectCountAggregator.class);
    public static byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_C");
    public static byte[] VALUE_COLUMN_QUALIFIER = QueryConstants.VALUE_COLUMN_QUALIFIER;
    private ServerDirectAggregator serverDirectAggregator;
    public ServerDirectCountAggregator(){
        serverDirectAggregator = new ServerDirectAggregator(VALUE_COLUMN_FAMILY,VALUE_COLUMN_QUALIFIER,getDataType());
    }
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        Object countObject = serverDirectAggregator.aggregate(tuple,ptr);
        if( countObject != null ){
            count = Long.parseLong(countObject.toString());
        }
    }
}
