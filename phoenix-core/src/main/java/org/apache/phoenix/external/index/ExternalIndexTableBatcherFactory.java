package org.apache.phoenix.external.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.schema.PTable;

/**
 * 外部索引表批量工厂
 */
public abstract class ExternalIndexTableBatcherFactory {
    protected ConfigurationReadableOnlyWrapper configuration;

    /**
     * 初始化
     * @param configuration 当前配置
     */
    public void initialize(Configuration configuration){
        this.configuration = ExternalUtil.getReadOnlyConfiguration(configuration);
    }

    /**
     * 关闭并释放资源
     */
    public void close(){}

    /**
     * 创建外部提交器
     * @param indexTable 外部索引表
     * @return indexTable对应的IExternalBatch
     */
    public abstract IExternalBatch createExternalIndexTableBatch(PTable indexTable);
}
