package org.apache.phoenix.external.aggregator;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.*;
import org.apache.phoenix.expression.function.SingleAggregateFunction;

public class ServerDirectAggregators extends NonSizeTrackingServerAggregators {
    public static ServerDirectAggregators from(ServerAggregators serverAggregators){
        if(serverAggregators == null){
            return null;
        }
        Aggregator[] aggregatorList = new Aggregator[serverAggregators.getAggregatorCount()];
        for (int i = 0; i < serverAggregators.getAggregatorCount(); i++) {
            if(serverAggregators.getAggregators()[i] instanceof CountAggregator){
                aggregatorList[i] = new ServerDirectCountAggregator();
            }else if (serverAggregators.getAggregators()[i] instanceof NumberSumAggregator){
                aggregatorList[i] = new ServerDirectSumAggregator(serverAggregators.getAggregators()[i].getDataType(),
                        serverAggregators.getAggregators()[i].getSortOrder());
            }else if (serverAggregators.getAggregators()[i] instanceof MaxAggregator){
                aggregatorList[i] = new ServerDirectMaxAggregator(serverAggregators.getAggregators()[i].getSortOrder(),
                        serverAggregators.getAggregators()[i].getDataType(),
                        serverAggregators.getAggregators()[i].getMaxLength());
            }else if (serverAggregators.getAggregators()[i] instanceof MinAggregator){
                aggregatorList[i] = new ServerDirectMinAggregator(serverAggregators.getAggregators()[i].getSortOrder(),
                        serverAggregators.getAggregators()[i].getDataType(),
                        serverAggregators.getAggregators()[i].getMaxLength());
            }else{
                aggregatorList[i] = serverAggregators.getAggregators()[i];
            }
        }
        return new ServerDirectAggregators(serverAggregators.getFunctions(),
                aggregatorList,
                serverAggregators.getExpressions(),
                serverAggregators.getMinNullableIndex());
    }
    public ServerDirectAggregators(SingleAggregateFunction[] functions, Aggregator[] aggregators, Expression[] expressions, int minNullableIndex) {
        super(functions, aggregators, expressions, minNullableIndex);
    }
}
