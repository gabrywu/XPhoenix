package org.apache.phoenix.external.index.elasticsearch.benchmark;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Benchmark {
    private static Logger logger = LoggerFactory.getLogger(Benchmark.class);
    private static String upsertSql( String table,int fieldNum,int dataPos ){
        StringBuilder sb = new StringBuilder();
        sb.append("upsert into ").append(table).append(" values");
        sb.append("(").append(dataPos );
        for (int j = 0; j < fieldNum; j++) {
            sb.append(",'").append(BenchmarkUtil.getFieldRandomValue()).append("'");
        }
        sb.append(")");

        return sb.toString();
    }
    private static void batch(String zk,String table,int fieldNum,int dataNum) throws SQLException{

        System.out.println("zk "+zk+" table "+table+" field num "+fieldNum+" data num "+dataNum);
        try (PhoenixConnection connection = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:"+zk);
             PhoenixStatement statement = (PhoenixStatement) connection.createStatement()){
            int batchSize = Integer.parseInt( connection.getQueryServices().getConfiguration().get("phoenix.mutate.batchSize",
                    "1000") );
            System.out.println("begin batch size "+batchSize);
            long start = System.currentTimeMillis();
            for (int i = 1; i <= dataNum; i++) {
                String sql = upsertSql(table,fieldNum,i);
                statement.addBatch(sql);
                if(i%batchSize==0){
                    statement.executeBatch();
                    connection.commit();
                    logger.info("commit "+ i );
                }
            }
            statement.executeBatch();
            connection.commit();
            long end = System.currentTimeMillis();
            logger.warn("end time "+(end - start)+" ms");
        }
    }
    public static void main(String[] args) throws SQLException {
        String zk = args[0];
        String table = args[1];
        int fieldNum = Integer.parseInt(args[2]);
        int dataNum = Integer.parseInt(args[3]);
        batch(zk,table,fieldNum,dataNum);
        batch(zk,table+"_raw",fieldNum,dataNum);
    }
}
