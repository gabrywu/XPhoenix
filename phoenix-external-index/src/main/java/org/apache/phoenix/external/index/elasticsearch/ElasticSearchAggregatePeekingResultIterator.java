package org.apache.phoenix.external.index.elasticsearch;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.aggregator.ServerDirectAggregatorUtil;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.apache.phoenix.query.QueryConstants.*;

public class ElasticSearchAggregatePeekingResultIterator extends AbstractElasticSearchResultIterator<SearchRequest>{
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchAggregatePeekingResultIterator.class);
    private static final String AGGREGATOR_SUFFIX = "@";
    private static final String LITERAL_FIELD = "1";
    private static final String LITERAL_AGGREGATOR_SUFFIX = AGGREGATOR_SUFFIX + "COUNT";
    private static final String SUM_AGGREGATOR_SUFFIX = AGGREGATOR_SUFFIX + "SUM";
    private static final String LITERAL_AGGREGATOR = LITERAL_FIELD + LITERAL_AGGREGATOR_SUFFIX;
    private static final PDataType DEFAULT_AGGREGATOR_DATA_TYPE = PInteger.INSTANCE;
    private static final String DEFAULT_AGGREGATOR_NAME = "_index";
    private GroupByCompiler.GroupBy groupBys;
    private int currentPosition;
    private List<Tuple> resultTuples;

    public enum SupportedEsAggregationType {
        COUNT,MAX,MIN,SUM,// AVG 被转化成了COUNT/SUM
        FIRST_VALUE,LAST_VALUE,FIRST_VALUES,LAST_VALUES,
        NTH_VALUE,STDDEV_POP,STDDEV_SAMP,PERCENT_RANK,
        APPROX_COUNT_DISTINCT,PERCENTILE_CONT,PERCENTILE_DISC
    }

    private Map<String,PDataType> aggregatorTypes ;
    private Map<String,Expression> groupByKeyExpressions;

    private boolean isDefaultAggregation(Aggregation aggregation){
        return DEFAULT_AGGREGATOR_NAME.equals(aggregation.getName());
    }
    private TermsAggregationBuilder buildDefaultTermsAggregationBuilder() throws SQLException{
        return buildTermsAggregationBuilder(DEFAULT_AGGREGATOR_NAME,true);
    }
    private TermsAggregationBuilder buildTermsAggregationBuilder(String groupFiledName,boolean withAggregator) throws SQLException {
        TermsAggregationBuilder groupBy = AggregationBuilders.terms(groupFiledName)
                .field(ElasticSearchFieldType.getRawFieldName(groupFiledName))
                .size(Integer.MAX_VALUE);
        if(orderBy.containsKey(groupFiledName)){
            groupBy.order(BucketOrder.key(SortOrder.ASC == orderBy.get(groupFiledName)));
        }
        if(withAggregator){
            for (SingleAggregateFunction aggregateFunction:this.serverAggregators.getFunctions()){
                Expression aggExpression = aggregateFunction.getAggregatorExpression();
                String alias;
                String aggFieldName;
                if(aggExpression instanceof KeyValueColumnExpression){
                    KeyValueColumnExpression aggFuncExpression = (KeyValueColumnExpression)aggExpression;

                    String aggFieldFamilyName = ExternalUtil.getAggregateFunctionFamilyName(aggFuncExpression);
                    aggFieldName = ExternalUtil.getAggregateFunctionColumnName(indexTable, aggFuncExpression);
                    alias = IndexUtil.getIndexColumnName (aggFieldFamilyName,aggFieldName) + AGGREGATOR_SUFFIX + aggregateFunction.getName();

                }else if(aggExpression instanceof RowKeyColumnExpression){
                    // todo 2019-09-10 09:17:19 如何支持
                    throw new UnsupportedEsAggregationException("Row key not support Aggregation");
                }else{
                    aggFieldName = aggExpression.toString();
                    alias = aggExpression.toString() + AGGREGATOR_SUFFIX + aggregateFunction.getName();
                }
                logger.debug("aggFieldName {},alias {}",aggFieldName,alias);
                if(!aggregatorTypes.containsKey(alias)){
                    aggregatorTypes.put(alias,aggExpression.getDataType());
                }

                ValuesSourceAggregationBuilder aggregationBuilder = buildValuesSourceAggregationBuilder(aggregateFunction.getName(),alias,aggFieldName);
                groupBy.subAggregation(aggregationBuilder);
            }
        }
        return groupBy;
    }

    private ValuesSourceAggregationBuilder buildValuesSourceAggregationBuilder(String aggTypeName,String alias,String aggFieldName) throws UnsupportedEsAggregationException {
        SupportedEsAggregationType aggregationType = SupportedEsAggregationType.valueOf(aggTypeName);
        String rawFieldName = ElasticSearchFieldType.getRawFieldName( aggFieldName.equals("1")?INDEX_DOC_ID_FIELD_NAME:aggFieldName);
        ValuesSourceAggregationBuilder aggregationBuilder;
        switch (aggregationType){
            case COUNT:
                aggregationBuilder = AggregationBuilders.count(alias).field( rawFieldName );
                break;
            case MAX:
                aggregationBuilder = AggregationBuilders.max(alias).field( rawFieldName );
                break;
            case MIN:
                aggregationBuilder = AggregationBuilders.min(alias).field( rawFieldName );
                break;
            case SUM:
                aggregationBuilder =  AggregationBuilders.sum(alias).field( rawFieldName );
                break;
            default:
                throw new UnsupportedEsAggregationException("Unexpected value: " + aggregationType);
        }
        return aggregationBuilder;
    }

    private TermsAggregationBuilder buildTermsAggregationBuilder() throws SQLException {
        TermsAggregationBuilder termsAggregationBuilders = null;
        if(groupBys.getExpressions().isEmpty()){
            termsAggregationBuilders = buildDefaultTermsAggregationBuilder();
        }else{
            List<Expression> groupByExpressions = groupBys.getExpressions();
            for (int i = 0; i < groupByExpressions.size(); i++) {
                Expression group = groupByExpressions.get(i);
                KeyValueColumnExpression kvGroup = ((KeyValueColumnExpression) group);
                PColumn column = ExternalUtil.convert2Column(indexTable,kvGroup);
                String columnName = column.getName().getString();
                String groupName = IndexUtil.getDataColumnName(columnName) ;
                TermsAggregationBuilder groupBy = buildTermsAggregationBuilder(groupName,
                        i==groupByExpressions.size()-1);

                logger.debug("expression {},columnName {},groupName {}",group,columnName,groupName);
                if(termsAggregationBuilders == null){
                    termsAggregationBuilders = groupBy;
                }else{
                    termsAggregationBuilders.subAggregation(groupBy);
                }
            }
        }
        return termsAggregationBuilders;
    }

    public ElasticSearchAggregatePeekingResultIterator(final ConfigurationReadableOnlyWrapper configuration,
                                                       final ExpressionCompiler expressionCompiler ,
                                                       final String indexName,
                                                       final String query,
                                                       final GroupByCompiler.GroupBy groupBys,
                                                       final  LinkedHashMap<String,SortOrder> orderByColumnName ,
                                                       final TupleProjector tupleProjector,
                                                       final PTable indexTable,
                                                       final RowProjector rowProjector,
                                                       final ServerAggregators serverAggregators) throws SQLException {
        super(configuration,expressionCompiler, indexName,query,orderByColumnName, tupleProjector,indexTable,rowProjector,serverAggregators);
        this.groupBys = groupBys;
        this.currentPosition = -1;
        aggregatorTypes = new HashMap<>();
        actionRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = buildTermsAggregationBuilder();
        logger.debug("termsAggregationBuilder {}",termsAggregationBuilder);

        searchSourceBuilder.aggregation(termsAggregationBuilder);

      //  searchSourceBuilder = EsUtils.buildSort(searchSourceBuilder,orderByColumnName);

        actionRequest.source(searchSourceBuilder);

        logger.debug("group by {},searchSourceBuilder {},termsAggregationBuilders {},rowProjector {}",
                groupBys,searchSourceBuilder,termsAggregationBuilder,rowProjector);

        if(logger.isDebugEnabled()){
            for (Expression expression:groupBys.getKeyExpressions()){
                logger.info("key expression: {}, type {}",expression.toString(),expression.getClass().getName());
            }
            for (Expression expression:groupBys.getExpressions()){
                logger.info("group expression: {}, type {}",expression.toString(),expression.getClass().getName());
            }
        }

        groupByKeyExpressions = ExternalUtil.getGroupByKeyExpressionMap(groupBys);
    }
    @Override
    public SearchSourceBuilder buildSearchSourceBuilder() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(query!=null && !query.isEmpty()){
            searchSourceBuilder.query(QueryBuilders.queryStringQuery(query));
        }else{
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }

        return searchSourceBuilder;
    }

    private boolean isLiteralCountAggregation(Aggregation aggregation){
        return LITERAL_AGGREGATOR.equals(aggregation.getName());
    }
    private String getAggregatorName(String aggregationName){
        return aggregationName.substring(aggregationName.indexOf(AGGREGATOR_SUFFIX)+1);
    }
    private String getColumnFullName(Aggregation aggregation){
        String aggregatorName = aggregation.getName();
        return aggregatorName.substring(0,aggregatorName.indexOf(AGGREGATOR_SUFFIX));
    }
    private void retrieveFrom( final Aggregations aggregations ) throws SQLException{
        retrieveFrom(aggregations,ByteUtil.EMPTY_BYTE_ARRAY);
    }
    private void retrieveFrom( final Aggregations aggregations, final byte[] lastAggKeyBytes ) throws SQLException {
        List<Aggregation> aggregationList = aggregations.asList();
        Aggregation firstAgg = aggregationList.get(0);
        if(firstAgg instanceof Terms){
            logger.debug("aggregations {}",firstAgg.getName());
            for (Terms.Bucket bucket:((Terms) firstAgg).getBuckets()){
                logger.debug("aggregations {},bucket {}",bucket,bucket.getKeyAsString());
                byte[] nextAggKeyBytes ;
                if( !isDefaultAggregation(firstAgg) ){
                    Expression keyExpression = groupByKeyExpressions.get(firstAgg.getName());
                    PDataType keyType = keyExpression == null ? DEFAULT_AGGREGATOR_DATA_TYPE : keyExpression.getDataType();
                    byte[] currentAggKeyBytes = keyType.toBytes( keyType.toObject(bucket.getKeyAsString()));
                    nextAggKeyBytes = lastAggKeyBytes.length == 0 ? currentAggKeyBytes:
                            ByteUtil.concat(lastAggKeyBytes,QueryConstants.SEPARATOR_BYTE_ARRAY,currentAggKeyBytes);
                }else{
                    nextAggKeyBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                }

                logger.debug("aggregations {}, bucket {},nextAggKeyBytes {}",firstAgg.getName(),bucket.getKeyAsString(),Bytes.toHex(nextAggKeyBytes));
                Aggregations bucketAgg = bucket.getAggregations();
                retrieveFrom(bucketAgg,nextAggKeyBytes);
            }
        }else {
            for (Aggregation agg:aggregationList){
                String aggregatorName = getAggregatorName(agg.getName());
                boolean isLiteralAggregation = isLiteralCountAggregation(agg);

                String columnFullName;
                String aggregatorFieldName;
                byte[] aggregatorFieldFamily;
                PDataType aggregatorFieldType;
                if(isLiteralAggregation){
                    columnFullName = Bytes.toString( VALUE_COLUMN_QUALIFIER );
                    aggregatorFieldFamily = VALUE_COLUMN_FAMILY;
                    aggregatorFieldName = Bytes.toString(VALUE_COLUMN_QUALIFIER) ;
                    aggregatorFieldType = PInteger.INSTANCE;
                }else{
                    columnFullName = getColumnFullName(agg);
                    aggregatorFieldFamily = Bytes.toBytes( IndexUtil.getDataColumnFamilyName(columnFullName) );
                    aggregatorFieldName = IndexUtil.getDataColumnName(columnFullName);
                    aggregatorFieldType = allColumns.get(aggregatorFieldName).getDataType();
                }

                logger.debug("agg Name {},bulkKey {} isLiteralAggregation {},aggregator name {},aggregatorFieldFamily {},aggregatorFieldName {}" +
                                ",aggregatorFieldNameType {},aggregatorFieldValue {},columnFullName {}",
                        agg.getName(), Bytes.toHex(lastAggKeyBytes),isLiteralAggregation,
                        aggregatorName,aggregatorFieldFamily,aggregatorFieldName,aggregatorFieldType,
                        agg instanceof NumericMetricsAggregation.SingleValue ?
                                ((NumericMetricsAggregation.SingleValue) agg).getValueAsString():agg
                        ,columnFullName);

                Cell cell;

                if(agg instanceof NumericMetricsAggregation.SingleValue){
                    cell = CellUtil.createCell(lastAggKeyBytes,
                            aggregatorFieldFamily,
                            EncodedColumnsUtil.usesEncodedColumnNames(indexTable) && !isLiteralAggregation?
                                    indexTable.getColumnForColumnName(columnFullName).getColumnQualifierBytes():
                                    Bytes.toBytes(columnFullName),
                            AGG_TIMESTAMP, KeyValue.Type.Put.getCode(),
                            aggregatorFieldType.toBytes( ((NumericMetricsAggregation.SingleValue) agg).value() ));
                }else{
                    // todo 2019-08-16 14:33:54 要适配字符串类型的聚合
                    throw new UnsupportedEsAggregationException("UnSupported aggregator "+agg.getName()+",type "+agg.getClass().getName());
                }

                ResultTuple resultTuple;
                if(ServerDirectAggregatorUtil.isDirectAggregator(aggregatorName)){
                    Cell literalCell = CellUtil.createCell(lastAggKeyBytes,
                            ServerDirectAggregatorUtil.getDirectAggregatorFamily(aggregatorName),
                            ServerDirectAggregatorUtil.getDirectAggregatorColumn(aggregatorName),
                            AGG_TIMESTAMP, KeyValue.Type.Put.getCode(),
                            aggregatorFieldType.toBytes( ((NumericMetricsAggregation.SingleValue) agg).value() ));
                    List<Cell> cells = Arrays.asList(cell,literalCell);
                    Collections.sort(cells,KeyValue.COMPARATOR);
                    resultTuple = new ResultTuple(Result.create(cells));
                }else{
                    resultTuple = new ResultTuple(Result.create(Collections.singletonList(cell)));
                }
                serverAggregators.aggregate(serverAggregators.getAggregators(),resultTuple);
            }

            Cell resultCell = CellUtil.createCell(lastAggKeyBytes,
                    SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN,
                    AGG_TIMESTAMP, KeyValue.Type.Put.getCode(),
                    serverAggregators.toBytes(serverAggregators.getAggregators()));
            serverAggregators.reset(serverAggregators.getAggregators());
            SingleKeyValueTuple resultTuple = new SingleKeyValueTuple(resultCell);

            resultTuples.add(resultTuple);
        }
    }

    @Override
    public AbstractElasticSearchResultIterator initialize() throws SQLException {
        try {
            resultTuples = new ArrayList<>();
            SearchResponse searchResponse = elasticClient.search(actionRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();

            EsUtils.printAggregations(aggregations);

            retrieveFrom(aggregations);
            logger.debug("resultTuples size {}",resultTuples.size());
        } catch (IOException e) {
            throw new SQLException(e.getMessage(),e);
        }
        return this;
    }

    /**
     * Returns the next result without advancing the iterator
     *
     * @throws SQLException
     */
    @Override
    public Tuple peek() throws SQLException {
        logger.debug("peek currentPosition {}",currentPosition);
        return currentPosition+1<resultTuples.size()? resultTuples.get(currentPosition+1):null;
    }

    /**
     * Grab the next row's worth of values. The iterator will return a Tuple.
     *
     * @return Tuple object if there is another row, null if the scanner is
     * exhausted.
     * @throws SQLException e
     */
    @Override
    public Tuple next() throws SQLException {
        Tuple tuple = peek();
        currentPosition++;
        return tuple;
    }
}
