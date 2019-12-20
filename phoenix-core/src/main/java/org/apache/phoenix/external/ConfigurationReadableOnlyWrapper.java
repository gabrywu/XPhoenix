package org.apache.phoenix.external;

import org.apache.hadoop.conf.Configuration;

/**
 * 封装Configuration的读取操作，以避免客户端修改Configuration
 */
public class ConfigurationReadableOnlyWrapper {
    private final Configuration configuration;
    public ConfigurationReadableOnlyWrapper(Configuration configuration){
        this.configuration = configuration;
    }
    public String get(String name,String defaultValue ){
        return configuration.get(name,defaultValue);
    }
    public String get(String name){
        return configuration.get(name);
    }

    @Override
    public String toString() {
        return configuration.toString();
    }
}
