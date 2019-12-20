package org.apache.phoenix.external.index.elasticsearch.benchmark;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Benchmark2 {
    private static Logger logger = LoggerFactory.getLogger(Benchmark2.class);
    private static void batch(String zk,String table,int fieldNum,int dataNum) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("upsert into ").append(table).append(" values(?");
        for (int i = 0; i < fieldNum; i++) {
            sb.append(",?");
        }
        sb.append(")");
        System.out.println("zk "+zk+" table "+table+" field num "+fieldNum+" data num "+dataNum+" sql "+sb);
        try (PhoenixConnection connection = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:"+zk);
             PhoenixPreparedStatement statement = (PhoenixPreparedStatement) connection.prepareStatement(sb.toString())){
            int batchSize = Integer.parseInt( connection.getQueryServices().getConfiguration().get("phoenix.mutate.batchSize",
                    "1000") );
            System.out.println("begin batch size "+batchSize);
            long start = System.currentTimeMillis();
            for (int i = 1; i <= dataNum; i++) {
                statement.setInt(1,i);
                for (int j = 1; j <= fieldNum; j++) {
                    statement.setString(j+1,BenchmarkUtil.getFieldRandomValue());
                }
                statement.addBatch();
                if(i%batchSize==0){
                    statement.executeBatch();
                    connection.commit();
                    logger.info("commit "+i);
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
