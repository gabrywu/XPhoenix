package org.apache.phoenix.external.aggregator;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.util.ByteUtil;

import java.util.HashMap;
import java.util.Map;

public class ServerDirectAggregatorUtil {
    public static Map<String, Pair<byte[],byte[]>> directAggregatorMap;
    static {
        directAggregatorMap = new HashMap<>(4);
        directAggregatorMap.put("COUNT",Pair.newPair(ServerDirectCountAggregator.VALUE_COLUMN_FAMILY,
                ServerDirectCountAggregator.VALUE_COLUMN_QUALIFIER));

        directAggregatorMap.put("SUM",Pair.newPair(ServerDirectSumAggregator.VALUE_COLUMN_FAMILY,
                ServerDirectSumAggregator.VALUE_COLUMN_QUALIFIER));

        directAggregatorMap.put("MAX",Pair.newPair(ServerDirectMaxAggregator.VALUE_COLUMN_FAMILY,
                ServerDirectMaxAggregator.VALUE_COLUMN_QUALIFIER));

        directAggregatorMap.put("MIN",Pair.newPair(ServerDirectMinAggregator.VALUE_COLUMN_FAMILY,
                ServerDirectMinAggregator.VALUE_COLUMN_QUALIFIER));
    }
    public static boolean isDirectAggregator(String aggregatorName){
        return directAggregatorMap.containsKey(aggregatorName);
    }
    public static byte[] getDirectAggregatorFamily(String aggregatorName){
        byte[] family = directAggregatorMap.containsKey(aggregatorName)? directAggregatorMap.get(aggregatorName).getFirst():
                ByteUtil.EMPTY_BYTE_ARRAY;
        return family;
    }
    public static byte[] getDirectAggregatorColumn(String aggregatorName){
        byte[] column = directAggregatorMap.containsKey(aggregatorName)? directAggregatorMap.get(aggregatorName).getSecond():
                ByteUtil.EMPTY_BYTE_ARRAY;
        return column;
    }
}
