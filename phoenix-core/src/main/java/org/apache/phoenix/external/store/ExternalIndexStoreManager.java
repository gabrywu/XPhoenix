package org.apache.phoenix.external.store;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class ExternalIndexStoreManager {
    private final static Logger logger = LoggerFactory.getLogger(ExternalIndexStoreManager.class);
    public static List<ExternalIndexStore> convert(PTable indexTable,List<Mutation> mutations) throws ColumnNotFoundException, AmbiguousColumnException {
        boolean usesEncodedColumnNames = EncodedColumnsUtil.usesEncodedColumnNames(indexTable);
        List<ExternalIndexStore> stores = new ArrayList<>(mutations.size());
        for (Mutation mutation:mutations){
            stores.add(convert(indexTable,mutation,usesEncodedColumnNames));
        }
        return stores;
    }
    public static ExternalIndexStore convert(byte[] keyBytes,Map<String,Object> fields){
        String rowKey =  Bytes.toHex(keyBytes);
        ExternalIndexStore store = new ExternalIndexStore(rowKey);
        store.setFields(fields);
        return store;
    }
    public static ExternalIndexStore convert(PTable indexTable, Mutation mutation,boolean usesEncodedColumnNames) throws AmbiguousColumnException, ColumnNotFoundException{
        String rowKey =  Bytes.toHex(mutation.getRow());
        ExternalIndexStore store = new ExternalIndexStore(rowKey);
        if(mutation instanceof Delete){
            store.setType(ExternalIndexStore.Type.Delete);
        }else{
            NavigableMap<byte [], List<Cell>> map = mutation.getFamilyCellMap();
            for (Map.Entry<byte [], List<Cell>> entry:map.entrySet()){
                for (Cell cell:entry.getValue()){
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    String columnName = Bytes.toString( qualifier );
                    if (QueryConstants.EMPTY_COLUMN_NAME.equals(columnName) ||
                            ( usesEncodedColumnNames &&
                                    indexTable.getEncodingScheme().decode(qualifier) <
                                            QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE ) ){
                        continue;
                    }
                    PColumn pColumn = indexTable.getColumnForColumnQualifier(family,qualifier);
                    columnName = SchemaUtil.getColumnName(
                            pColumn.getFamilyName() == null ? null : pColumn.getFamilyName().getString()
                            ,pColumn.getName().getString());
                    logger.debug("column {},value : {},type {}",columnName,Bytes.toString(value),pColumn.getDataType());
                    store.addField(IndexUtil.getDataColumnName(columnName),
                            pColumn.getDataType().toObject(value));
                }
            }
        }
        return store;
    }
}
