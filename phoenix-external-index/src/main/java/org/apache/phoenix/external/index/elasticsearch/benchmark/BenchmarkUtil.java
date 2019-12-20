package org.apache.phoenix.external.index.elasticsearch.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BenchmarkUtil {
    private static Logger logger = LoggerFactory.getLogger(BenchmarkUtil.class);
    private static Map<String,MethodExecInfo> methodElapsedMap = new HashMap<>(1024);
    public static String randomString(int length,boolean digitOnly){
        String letter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        String digit = "0123456789";
        String str = digitOnly ? digit : letter + digit;
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < length ;i++){
            int number = random.nextInt(str.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
    public static String getFieldRandomValue(){
        return randomString(8,false)+" "+randomString(4,true)+" "+randomString(8,false);
    }
    public static void logMethodElapsed(String methodName,long elapsed){
        if(!methodElapsedMap.containsKey(methodName)){
            methodElapsedMap.put(methodName,new MethodExecInfo());
        }
        MethodExecInfo info = methodElapsedMap.get(methodName);
        info.addElapsed(elapsed);
        info.addExecCount();
        logger.info("{} elapsed {},avg elapsed {}",methodName,elapsed,info.getAvgElapsed());
    }
    static class MethodExecInfo{
        private long elapsed;
        private long execCount;
        public MethodExecInfo(){
            this.execCount = 0;
            this.elapsed = 0;
        }
        public MethodExecInfo(long elapsed,long execCount){
            this.elapsed = elapsed;
            this.execCount = execCount;
        }
        public long getElapsed() {
            return elapsed;
        }

        public void addElapsed(long elapsed) {
            this.elapsed += elapsed;
        }

        public long getExecCount() {
            return execCount;
        }

        public void addExecCount() {
            this.execCount += 1;
        }
        public long getAvgElapsed(){
            return execCount ==0 ? 0: elapsed/execCount;
        }
    }
}
