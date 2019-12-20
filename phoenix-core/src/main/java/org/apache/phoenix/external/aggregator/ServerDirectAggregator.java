package org.apache.phoenix.external.aggregator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDirectAggregator {
    private static Logger logger = LoggerFactory.getLogger(ServerDirectAggregator.class);
    private static PDataType defaultType = PInteger.INSTANCE;
    private byte[] columnFamily;
    private byte[] columnQualifier;
    private PDataType inputDataType;
    public ServerDirectAggregator( byte[] columnFamily,byte[] columnQualifier,PDataType inputDataType){
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.inputDataType = inputDataType;
    }
    public Object aggregate(Tuple tuple, ImmutableBytesWritable ptr){
        Cell cell = tuple.getValue(columnFamily,columnQualifier);
        Object object = null;
        if(cell != null){
            byte[] value = CellUtil.cloneValue(cell);
            PDataType valueType = value.length == 4 ? defaultType : inputDataType;
            object = valueType.toObject(value).toString() ;
            ptr.set(value,0,value.length);
            logger.debug("family {},object {},ptr {}", Bytes.toString(columnFamily),object,ptr);
        }
        return object;
    }

}
