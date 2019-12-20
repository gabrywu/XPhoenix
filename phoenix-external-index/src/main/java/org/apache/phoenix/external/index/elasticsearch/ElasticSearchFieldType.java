package org.apache.phoenix.external.index.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchFieldType {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchFieldType.class);
    private static final String KEYWORD_FIELD_NAME = "raw";
    private static final String KEYWORD_FIELD_NAME_SUFFIX = "."+ KEYWORD_FIELD_NAME;
    private static final String FIELD_NAME_ALIAS_SUFFIX = "_"+ KEYWORD_FIELD_NAME;

    public static String getRawFieldName(String name){
        return name.startsWith("_") ?name: name + FIELD_NAME_ALIAS_SUFFIX;
    }
    public static JSONObject from(PDataType pDataType){
        if(pDataType instanceof PNumericType){
            JSONObject number = new JSONObject();
            if(pDataType instanceof PWholeNumber){
                number.put("type","long");
            }else if(pDataType instanceof PRealNumber){
                number.put("type","double");
            }
            return number;
        }else{
            return TEXT();
        }
    }
    public static Pair<String,JSONObject> alias(String field, JSONObject fieldType){
        JSONObject alias = new JSONObject();
        alias.put("type","alias");
        if("text".equals(fieldType.getString("type"))){
            alias.put("path",field + KEYWORD_FIELD_NAME_SUFFIX);
        }else{
            alias.put("path",field);
        }
        return new Pair<>(field + FIELD_NAME_ALIAS_SUFFIX,alias);
    }
    public static JSONObject defaultType(){
        return TEXT();
    }
    private static JSONObject TEXT() {
        JSONObject text = new JSONObject();
        text.put("type","text");
        JSONObject raw = new JSONObject();
        raw.put("type","keyword");
        JSONObject fields = new JSONObject();
        fields.put(KEYWORD_FIELD_NAME,raw);
        text.put("fields",fields);
        return text;
    }

    public static String toReadableValue(Object dataValue,String dataTypeName,PDataType indexType){
        if(dataValue == null){
            return null;
        }
        PDataType dataValueType =  PDataType.fromSqlTypeName(dataTypeName);
        Object convertedValue = indexType.toObject(dataValue,dataValueType);
        if(PDate.INSTANCE.getSqlTypeName().equals(indexType.getSqlTypeName())){
            String valueWithLiteral = indexType.toStringLiteral(dataValue);
            convertedValue = valueWithLiteral.substring(1,valueWithLiteral.length()-1);
        }
        logger.debug("dataType {},indexType {},value {},valueClass {},convertedValue {}",
                dataTypeName,indexType.getSqlTypeName(),
                dataValue,dataValue.getClass().getName(),
                convertedValue.toString());

        return convertedValue.toString();
    }
}
