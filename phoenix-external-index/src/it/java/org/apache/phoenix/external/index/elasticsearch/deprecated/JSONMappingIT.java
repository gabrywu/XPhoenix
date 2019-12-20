package org.apache.phoenix.external.index.elasticsearch.deprecated;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.external.index.elasticsearch.ElasticSearchFieldType;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
public class JSONMappingIT {
    @Test
    public void dateAndDecimal(){
        byte[] key = SchemaUtil.getTableKey(ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY, Bytes.toBytes("IND_T000002"));
        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
        System.out.println(Bytes.toHex(cacheKey.get()));
    }
    @Test
    public void testType(){
        PDataType pDataType = PDecimal.INSTANCE;
        Object val = pDataType.toObject("2.1");
        byte[] bytes = pDataType.toBytes(val);
        System.out.println(Bytes.toString(bytes));
        System.out.println(Bytes.toHex (bytes));
        byte[] v2=Bytes.fromHex(Bytes.toHex (bytes));
        System.out.println(v2);
    }
    @Test
    public void fieldTest(){
        JSONObject fields = new JSONObject();
        fields.put("title", ElasticSearchFieldType.defaultType());
        fields.put("name",ElasticSearchFieldType.defaultType());
        JSONObject properties = new JSONObject();
        properties.put("properties",fields);
        assertEquals("{\"properties\":{\"name\":{\"type\":\"text\"},\"title\":{\"type\":\"text\"}}}",JSON.toJSONString(properties));
    }
    @Test
    public void testByte(){
        byte[] bytes =Bytes.add( Bytes.add( Bytes.add(Bytes.toBytes("V1_TEST"),new byte[]{0}),
                Bytes.add(Bytes.toBytes("V2_TEST"),new byte[]{0})),Bytes.toBytes("KEY2"));
        List<Integer> split = new ArrayList<>();
        split.add(0);

        for (int i = 0; i < bytes.length; i++) {
           // System.out.println(bytes[i]);
            if(bytes[i]==0){
              //  System.out.println(bytes[i]+" "+i);
                split.add(i);
            }
        }
        split.add(bytes.length);
        for (int i = 0; i < split.size()-1; i++) {
            System.out.println(Bytes.toString(bytes,split.get(i),split.get(i+1)-split.get(i)));
        }
    }
}
