package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.ExternalConstants;
import org.elasticsearch.action.support.WriteRequest;

import java.util.HashMap;
import java.util.Map;

public interface ElasticSearchConstants {
    String ES_INDEX_SETTINGS_PREFIX = ExternalConstants.EXTERNAL_PROP_PREFIX + ".es.index.";
    String ES_MAPPING_SETTINGS_PREFIX = ExternalConstants.EXTERNAL_PROP_PREFIX + ".es.mapping.";
    String ES_CLIENT_SETTINGS_PREFIX = ExternalConstants.EXTERNAL_PROP_PREFIX + ".es.client.";

    String CLUSTER_NODES = ES_CLIENT_SETTINGS_PREFIX + "cluster-nodes";


    Map<String,Object> DEFAULT_INDEX_SETTINGS = new HashMap<String,Object>(){{
        put("max_result_window",MAX_RESULT_WINDOW);
    }};
    Map<String,String> DEFAULT_MAPPINGS_PARAMETERS = new HashMap<String,String>(){{
        put("dynamic",DEFAULT_MAPPINGS_DYNAMIC);
    }};
    WriteRequest.RefreshPolicy DEFAULT_REFRESH_POLICY = WriteRequest.RefreshPolicy.WAIT_UNTIL;
    int MAX_RESULT_WINDOW = Integer.MAX_VALUE - 1;
    String REFRESH_POLICY_PROP = ES_INDEX_SETTINGS_PREFIX + "refresh.policy";
    String DEFAULT_MAPPINGS_DYNAMIC = "strict";
    String CLIENT_MAX_CONNECTION_PROP = ES_CLIENT_SETTINGS_PREFIX + "max-connection";
    String CLIENT_MAX_CONNECTION_PER_ROUTE_PROP = ES_CLIENT_SETTINGS_PREFIX + "max-connection-per-route";
}
