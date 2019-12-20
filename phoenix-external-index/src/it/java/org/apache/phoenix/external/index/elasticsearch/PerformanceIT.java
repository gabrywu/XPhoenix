package org.apache.phoenix.external.index.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PerformanceIT {
    private static ConfigurationReadableOnlyWrapper config;
    private static int FIELD_NUM = 2;
    private static long sum = 0;
    private static long sumCount = 0;
    @Before
    public void start(){
        sum = 0;
        sumCount = 0;
    }
    @BeforeClass
    public static void startup(){
        config = new ConfigurationReadableOnlyWrapper(HBaseConfiguration.create());
    }
    private String randomString(int length ){
        String letter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < length ;i++){
            int number = random.nextInt(letter.length());
            sb.append(letter.charAt(number));
        }
        return sb.toString();
    }
    @Test
    public void testGetRefreshPolicy(){
        int num = 10000;
        long s = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            EsUtils.getRefreshPolicy(config);
        }
        long e = System.currentTimeMillis();
        System.out.println(num+" 次耗时 "+(e-s)+" 毫秒");

        s = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            EsUtils.getRefreshPolicy(config);
        }
        e = System.currentTimeMillis();
        System.out.println(num+" 次耗时 "+(e-s)+" 毫秒");
    }
    private String getFieldName(int index){
        return "f"+index;
    }
    private void createIndex(RestHighLevelClient elasticClient,String indexName) throws IOException{
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        JSONObject mappings = new JSONObject();
        JSONObject fields = new JSONObject();
        for (int i = 1; i <= FIELD_NUM; i++) {
            fields.put(getFieldName(i),ElasticSearchFieldType.defaultType());
        }
        mappings.put("properties",fields);
        mappings.put("dynamic","strict");
        Settings.Builder builder = Settings.builder()
                .put("index.max_result_window",ElasticSearchConstants.MAX_RESULT_WINDOW)
                .put("refresh_interval","6s");
        request.mapping(mappings);
        request.settings(builder);
      //  System.out.println("create index "+indexName+" with mappings "+mappings+",settings "+builder.build().toString());
        elasticClient.indices().create(request, RequestOptions.DEFAULT);
    }
    private void dropIndex(RestHighLevelClient elasticClient,String indexName)throws IOException{
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        elasticClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
    }
    private long testBatchTime(RestHighLevelClient elasticClient,String indexName, int dataNum,int batchSize)throws IOException{
        createIndex(elasticClient,indexName);
        long start = System.nanoTime();
        BulkRequest bulkRequest = EsUtils.createBulkRequest(WriteRequest.RefreshPolicy.NONE);
        int counter = 1;

        for (int i = 0; i < dataNum; i++) {
            Map<String, Object> fields = new HashMap<>(FIELD_NUM);
            for (int j = 1; j <= FIELD_NUM; j++) {
                fields.put(getFieldName(j),randomString(8)+" "+randomString(4)+" "+randomString(8));
            }
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .id(String.valueOf(i)).source(fields);
            long s = System.nanoTime();
            bulkRequest.add(indexRequest);
            sumCount += 1;
            sum += System.nanoTime() - s;
            if ( counter % batchSize == 0 ){
                EsUtils.bulkRequest(elasticClient,bulkRequest);
                bulkRequest = EsUtils.createBulkRequest(WriteRequest.RefreshPolicy.NONE);
            }
            counter++;
        }
        if(!bulkRequest.requests().isEmpty()){
            EsUtils.bulkRequest(elasticClient,bulkRequest);
        }
        long end = System.nanoTime();

        dropIndex(elasticClient,indexName);

        return end - start;
    }

    @Test
    public void testEsClient() throws IOException {
        RestHighLevelClient elasticClient = ElasticSearchClient.getSingleton(config).getSingletonClient();
        String indexName = randomString(4).toLowerCase();
        int dataNum = 100*10000;
        long elapsed ;
        for (int i = 1; i <= 10; i++) {
            int batchSize = i*1000;
            elapsed = testBatchTime(elasticClient,indexName,dataNum,batchSize);
            System.out.println("Index " + indexName + " ,"+dataNum+" records ,batch "+batchSize
                    +", elapsed time "+elapsed/1000000+" ms, avg "+elapsed*batchSize/1000000/dataNum+" ms/"+batchSize);
        }
        System.out.println("bulk add " + sum/sumCount);
    }
    private long testBatchTimeAsync(RestHighLevelClient elasticClient,String indexName, int dataNum,int batchSize,int parallel) throws IOException, InterruptedException {
        createIndex(elasticClient,indexName);
        long start = System.nanoTime();
        BulkRequest bulkRequest = EsUtils.createBulkRequest(WriteRequest.RefreshPolicy.NONE);
        int counter = 1;

        for (int i = 0; i < dataNum; i++) {
            Map<String, Object> fields = new HashMap<>(FIELD_NUM);
            for (int j = 1; j <= FIELD_NUM; j++) {
                fields.put(getFieldName(j),randomString(8)+" "+randomString(4)+" "+randomString(8));
            }
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .id(String.valueOf(i)).source(fields);
            bulkRequest.add(indexRequest);
            if ( counter % batchSize == 0 ){
                EsUtils.bulkRequestAsync(elasticClient, bulkRequest);
                bulkRequest = EsUtils.createBulkRequest(WriteRequest.RefreshPolicy.NONE);
            }
            counter++;
        }
        if(!bulkRequest.requests().isEmpty()){
            EsUtils.bulkRequest(elasticClient,bulkRequest);
        }
        long end = System.nanoTime();

        dropIndex(elasticClient,indexName);

        return end - start;
    }
    @Test
    public void testAsyncEsClient() throws IOException, InterruptedException {
        RestHighLevelClient elasticClient = ElasticSearchClient.getSingleton(config).getSingletonClient();
        String indexName = randomString(4).toLowerCase();
        int parallel = 10;
        int dataNum = 100*10000;
        long elapsed ;
        for (int i = 1; i <= 10; i++) {
            int batchSize = i*1000;
            elapsed = testBatchTimeAsync(elasticClient,indexName,dataNum,batchSize,parallel);
            System.out.println("Index " + indexName + " , async bulk "+dataNum+" records ,batch "+batchSize+",parallel "+parallel
                    +", elapsed time "+elapsed/1000000+" ms, avg "+elapsed*batchSize/1000000/dataNum+" ms/"+batchSize);
        }
    }

    @Test
    public void testBulkRequest(){
        int dataNum = 100*10000;
        int batchSize = 1000;
        BulkRequest bulkRequest = EsUtils.createBulkRequest(ElasticSearchConstants.DEFAULT_REFRESH_POLICY);

        long start = System.nanoTime();
        for (int i = 1; i <= dataNum; i++) {
            Map<String,Object> fields = new HashMap<>();
            fields.put("f1","test_field1"+i);
            fields.put("f2",i);
            DocWriteRequest indexRequest = i%2 == 0 ?
                    new DeleteRequest(String.valueOf(i)):
                    new IndexRequest("testIndex")
                    .id(String.valueOf(i)).source(fields);
            bulkRequest.add(indexRequest);
            if( i % batchSize == 0){
                bulkRequest = EsUtils.createBulkRequest(ElasticSearchConstants.DEFAULT_REFRESH_POLICY);
            }
        }

        long elapsed = System.nanoTime() - start;
        System.out.println(dataNum+" records,"+batchSize+" batch size ," + elapsed/1000/1000 +" elapsed ms");
    }
}
