package org.apache.phoenix.external.index.elasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.search.TotalHits;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.store.ExternalIndexStore;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticSearchPeekingResultIterator extends AbstractElasticSearchResultIterator<SearchRequest> {
    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchPeekingResultIterator.class);
    private SearchHits hits;
    private long hitSize;
    private int currentPosition;
    private Integer offset;
    private Integer limit;
    public ElasticSearchPeekingResultIterator(final ConfigurationReadableOnlyWrapper configuration,
                                              final ExpressionCompiler expressionCompiler ,
                                              final String indexName,
                                              final String query,
                                              final LinkedHashMap<String,SortOrder> orderBy,
                                              final Integer offset,
                                              final Integer limit,
                                              final TupleProjector tupleProjector,
                                              final PTable indexTable,
                                              final RowProjector rowProjector){
        super(configuration,expressionCompiler,indexName,query,orderBy,tupleProjector,indexTable,rowProjector,null);
        this.offset = offset;
        this.limit = limit;
        this.actionRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = buildSearchSourceBuilder();
        actionRequest.source(searchSourceBuilder);
        currentPosition = -1;
        hitSize = 0;
        logger.debug("create ElasticSearchPeekingResultIterator with query [{}] for index [{}],searchSourceBuilder {}",query,indexName,searchSourceBuilder);
    }

    @Override
    public SearchSourceBuilder buildSearchSourceBuilder() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(query!=null && !query.isEmpty()){
            searchSourceBuilder.query(QueryBuilders.queryStringQuery(query).defaultOperator(Operator.AND));
        }else{
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }
        if(offset!=null){
            searchSourceBuilder.from(offset);
        }
        if(limit!=null){
            searchSourceBuilder.size(limit);
        }else{
            searchSourceBuilder.size(ElasticSearchConstants.MAX_RESULT_WINDOW);
        }
        searchSourceBuilder = EsUtils.buildSort(searchSourceBuilder,orderBy);
        searchSourceBuilder.sort(new FieldSortBuilder(INDEX_DOC_ID_FIELD_NAME).order(SortOrder.ASC));
        if(logger.isDebugEnabled()){
            searchSourceBuilder.profile(true);
        }
        return searchSourceBuilder;
    }

    public ElasticSearchPeekingResultIterator initialize() throws SQLException {
        try {
            SearchResponse searchResponse = elasticClient.search(actionRequest, RequestOptions.DEFAULT);
            hits = searchResponse.getHits();
            // 文档总记录条数
            TotalHits totalHits = hits.getTotalHits();
            hitSize = hits.getHits().length;
            if(logger.isDebugEnabled()){
                logger.debug("Action request {}", JSON.toJSONString(actionRequest,true));
                logger.debug("Hit size {}",hitSize);
                for (int i = 0; i < hitSize; i++) {
                    logger.debug("The {} hit {}",i,hits.getAt(i));
                }
                Map<String, ProfileShardResult> profilingResults =
                        searchResponse.getProfileResults();
                logger.debug("profilingResults {}",JSON.toJSONString(profilingResults,true));
            }
        } catch (IOException e) {
            throw new SQLException(e.getMessage(),e);
        }
        return this;
    }
    private Tuple toTuple(SearchHit hit) throws ColumnNotFoundException, AmbiguousColumnException {
        ExternalIndexStore store = EsUtils.toExternalStore(hit);

        ResultTuple resultTuple = new ResultTuple(store.convert2Result(allColumns,tupleProjector,indexTable));

        Tuple tuple = tupleProjector == null ? resultTuple:tupleProjector.projectResults(resultTuple) ;

        TupleProjector.ProjectedValueTuple rowProjectedTuple = rowTupleProjector.projectResults(tuple) ;

        logger.debug("tuple result key {}", Bytes.toHex( rowProjectedTuple.getKeyPtr().get() ));
        return rowProjectedTuple;
    }
    /**
     * Returns the next result without advancing the iterator
     *
     * @throws SQLException
     */
    @Override
    public Tuple peek() throws SQLException {
        return currentPosition + 1 < hitSize ? toTuple(hits.getAt(currentPosition + 1)):null;
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
        currentPosition ++;
        return tuple;
    }

}
