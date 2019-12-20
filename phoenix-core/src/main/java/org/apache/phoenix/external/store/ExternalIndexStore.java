package org.apache.phoenix.external.store;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.external.utils.ExternalUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExternalIndexStore {
    private static Logger logger = LoggerFactory.getLogger(ExternalIndexStore.class);
    private ExternalIndexStore.Type type;
    private final String rowKey;
    private Map<String,Object> fields = Collections.emptyMap();

    public ExternalIndexStore(String rowKey){
        this.rowKey = rowKey;
        this.type = Type.Put;
    }
    public ExternalIndexStore.Type getType() {
        return type;
    }

    public void setType(ExternalIndexStore.Type type) {
        this.type = type;
    }

    public String getRowKey() {
        return rowKey;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }
    public void addField(String k,Object v){
        if(this.fields.equals(Collections.emptyMap())){
            this.fields = new HashMap<>();
        }
        this.fields.put(k,v);
    }
    public enum Type{
        Delete,Put
    }

    @Override
    public String toString() {
        return String.format("ExternalIndexStore[ row key = %s,field = %s,type = %s]",rowKey,fields,type);
    }
    public Result convert2Result(final Map<String, PColumn> allColumns,
                                 final TupleProjector tupleProjector,
                                 final PTable indexTable) throws ColumnNotFoundException, AmbiguousColumnException {
        return ExternalIndexStore.convert2Result(this,allColumns,tupleProjector,indexTable);
    }
    private static Result convert2Result(final ExternalIndexStore store,
                                         final Map<String, PColumn> allColumns,
                                         final TupleProjector tupleProjector,
                                         final PTable indexTable) throws AmbiguousColumnException, ColumnNotFoundException {
        byte[] rowKey = Bytes.fromHex(store.getRowKey());
        Map<String, Object> fieldMap = store.getFields();
        List<Cell> cells = new ArrayList<>(fieldMap.keySet().size());
        boolean usesEncodedColumnNames = EncodedColumnsUtil.usesEncodedColumnNames(indexTable);
        if(tupleProjector!=null && tupleProjector.getExpressions().length>0){
            Expression[] expressions = tupleProjector.getExpressions();
            for (Expression expr:expressions){
                String exprStr = expr.toString();
                KeyValueColumnExpression kvce = expr instanceof SingleCellColumnExpression ?
                        ((SingleCellColumnExpression) expr).getKeyValueExpression()
                        :(KeyValueColumnExpression) expr;

                if(EncodedColumnsUtil.usesEncodedColumnNames(indexTable)){
                    PColumn pColumn = indexTable.getColumnForColumnQualifier(kvce.getColumnFamily(),kvce.getColumnQualifier());

                    exprStr = SchemaUtil.getColumnName(pColumn.getFamilyName().getString(),pColumn.getName().getString());
                }
                String colName = IndexUtil.getDataColumnName(exprStr);

                logger.debug("exprStr {},expr {},type {}",exprStr,expr,expr.getClass().getName());

                byte[] value = fieldMap.get(colName)==null? ByteUtil.EMPTY_BYTE_ARRAY:
                        ExternalUtil.fromReadableValue(fieldMap.get(colName).toString(),allColumns.get(colName));

                logger.debug("fromReadableValue {} actual value {},actual type {},converted value {},converted type {}",
                        colName,fieldMap.get(colName),fieldMap.get(colName)==null?null:fieldMap.get(colName).getClass().getName(),
                        Bytes.toHex(value),allColumns.get(colName)==null?null:allColumns.get(colName).getDataType().getSqlTypeName());

                Cell cell;
                if(expr instanceof SingleCellColumnExpression){
                    cell = CellUtil.createCell(rowKey,
                            ((SingleCellColumnExpression) expr).getColumnFamily(),
                            ((SingleCellColumnExpression) expr).getColumnQualifier(),
                            HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(),value);
                }else {
                    cell = CellUtil.createCell(rowKey,
                            kvce.getColumnFamily(),
                            kvce.getColumnQualifier(),
                            HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(),value);
                }
                if(logger.isDebugEnabled()){
                    logger.debug("return cell cf {},cq {}",Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell)));
                }

                cells.add(cell);
            }
        }else{
            for (String fieldName:fieldMap.keySet()){
                PDataType pDataType = allColumns.get(fieldName).getDataType();
                Object value = fieldMap.get(fieldName) == null ? null : pDataType.toObject(fieldMap.get(fieldName).toString());

                Cell cell = CellUtil.createCell(rowKey,null, Bytes.toBytes(fieldName),
                        HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(),pDataType.toBytes(value));
                cells.add(cell);
            }
        }

        if( usesEncodedColumnNames ){
            Collections.sort(cells,KeyValue.COMPARATOR);
        }
        if(cells.isEmpty()){
            cells.add(CellUtil.createCell(rowKey, QueryConstants.VALUE_COLUMN_FAMILY, QueryConstants.VALUE_COLUMN_QUALIFIER,
                    HConstants.LATEST_TIMESTAMP, KeyValue.Type.Put.getCode(),ByteUtil.EMPTY_BYTE_ARRAY));
        }
        return Result.create(cells);
    }
}
