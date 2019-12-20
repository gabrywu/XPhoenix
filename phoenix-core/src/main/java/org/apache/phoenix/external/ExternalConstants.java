package org.apache.phoenix.external;

public class ExternalConstants {
    public static final String EXTERNAL_PROP_PREFIX = "external";
    public static final String EXTERNAL_METADATA_CLASS_KEY = EXTERNAL_PROP_PREFIX + ".index.metadata.class";
    public static final String EXTERNAL_RESULT_ITERATORS_CLASS_KEY = EXTERNAL_PROP_PREFIX + ".index.result.iterators.class" ;
    public static final String EXTERNAL_INDEX_BATCH_FACTORY_CLASS_KEY = EXTERNAL_PROP_PREFIX + ".index.batch.factory.class" ;
    public static final String EXTERNAL_INDEX_COMMITTER_CONF_KEY = EXTERNAL_PROP_PREFIX + ".index.committer.class";

    public static final String DEFAULT_EXTERNAL_RESULT_ITERATORS_CLASS = "org.apache.phoenix.external.iterate.LoggingExternalResultIterators";
    public static final String DEFAULT_EXTERNAL_METADATA_CLASS = "org.apache.phoenix.external.schema.LoggingExternalMetaDataClient";
    public static final String DEFAULT_EXTERNAL_INDEX_BATCH_FACTORY_CLASS = "org.apache.phoenix.external.index.LoggingExternalBatchableIndexTableFactory";

    public static final String ATTRIBUTE_INDEX_TYPE = "INDEX_TYPE";
    public static final String EXTERNAL_QUERY_FIELD = "EXTERNAL_QUERY";
}
