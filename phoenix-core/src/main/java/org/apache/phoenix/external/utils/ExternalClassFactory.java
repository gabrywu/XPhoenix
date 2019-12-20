package org.apache.phoenix.external.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalClassFactory {
    private static final Logger logger = LoggerFactory.getLogger(ExternalClassFactory.class);
    public static <T> T getInstance(String className,Class<T> targetClass){
        T instance = null;
        try {
            Class clazz = Class.forName(className,false,Thread.currentThread().getContextClassLoader());
            instance = (T) clazz.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            logger.warn("{} not found ,{} will be used",className,targetClass.getName());
            logger.error(e.getMessage(),e);
        }
        return instance;
    }
}
