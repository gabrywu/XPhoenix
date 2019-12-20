package org.apache.phoenix.external.index.elasticsearch;

import java.sql.SQLException;

public class UnsupportedEsAggregationException extends SQLException {
    public UnsupportedEsAggregationException(String reason){
        super(reason);
    }
}
