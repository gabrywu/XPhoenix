package org.apache.phoenix.external.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.GroupByCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.*;
import org.apache.phoenix.external.ConfigurationReadableOnlyWrapper;
import org.apache.phoenix.external.ExternalConstants;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixResultSetMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

public class ExternalUtil {
    private static final Logger logger = LoggerFactory.getLogger(ExternalUtil.class);
    public static boolean canUseExternalIndex(FilterableStatement statement){
        boolean canUse = statement.getWhere() == null ||
                (statement.getWhere().getChildren().size() == 2 &&
                        ExternalConstants.EXTERNAL_QUERY_FIELD.equals(
                                IndexUtil.getDataColumnName(SchemaUtil.getUnEscapedFullColumnName(statement.getWhere().getChildren().get(0).toString())))
                );
        logger.debug("Can use external index {}, sql {}",canUse,statement);
        if(!canUse){
            logger.debug("where is null {}",statement.getWhere());
            if(statement.getWhere()!=null){
                logger.debug("where size {}",statement.getWhere().getChildren().size());
                logger.debug("where field [{}]",IndexUtil.getDataColumnName(SchemaUtil.getUnEscapedFullColumnName(statement.getWhere().getChildren().get(0).toString())));
            }
        }
        return canUse;
    }
    public static boolean isExternalIndex(PTable table){
        return PTableType.INDEX == table.getType() && PTable.IndexType.EXTERNAL == table.getIndexType();
    }
    public static LinkedHashMap<String, Expression> getGroupByKeyExpressionMap(GroupByCompiler.GroupBy groupBy){
        LinkedHashMap<String, Expression> keyExpressions = new LinkedHashMap<>();
        for (Expression expression:groupBy.getKeyExpressions()){
            if( expression instanceof CoerceExpression){
                keyExpressions.put(SchemaUtil.getTableNameFromFullName(
                        SchemaUtil.getUnEscapedFullColumnName(expression.getChildren().get(0).toString()) )
                        ,expression);
            }else{
                keyExpressions.put(SchemaUtil.getTableNameFromFullName(
                        SchemaUtil.getUnEscapedFullColumnName( expression.toString()) )
                        ,expression);
            }
        }
        return keyExpressions;
    }
    public static TupleProjector getTupleProjectorNonKeyColumn(RowProjector rowProjector){
        LinkedHashMap<String, Expression> noPkExpressions = new LinkedHashMap<>();
        for (ColumnProjector columnProjector:rowProjector.getColumnProjectors()){
            if(! (columnProjector.getExpression() instanceof RowKeyColumnExpression) ){
                noPkExpressions.put(columnProjector.getExpression().toString(),columnProjector.getExpression());
            }
        }
        return new TupleProjector(noPkExpressions.values().toArray(new Expression[0]));
    }
    public static void closeSilent(Closeable closeable){
        try {
            if(closeable != null){
                closeable.close();
            }
        } catch (IOException ignore) { }
    }
    public static void closeSilent(SQLCloseable closeable){
        try {
            if(closeable != null){
                closeable.close();
            }
        } catch (SQLException ignore) { }
    }
    public static PColumn convert2Column(PTable indexTable, KeyValueColumnExpression expression)
            throws AmbiguousColumnException, ColumnNotFoundException {
        return indexTable.getColumnForColumnQualifier(expression.getColumnFamily(),expression.getColumnQualifier());
    }
    private static ColumnDef EXTERNAL_QUERY_FIELD_DEF = null;
    public static ColumnDefInPkConstraint getExternalQueryField(String familyName){
        return new ColumnDefInPkConstraint(ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(familyName, ExternalConstants.EXTERNAL_QUERY_FIELD))
                ,SortOrder.ASC,false);
    }
    public static ColumnDef getExternalQueryField(ParseNodeFactory factory, String familyName){
        if(EXTERNAL_QUERY_FIELD_DEF==null){
            EXTERNAL_QUERY_FIELD_DEF = factory.columnDef(ColumnName.newColumnName(NamedNode.caseSensitiveNamedNode(familyName+":"+ExternalConstants.EXTERNAL_QUERY_FIELD)),
                    PVarchar.INSTANCE.getSqlTypeName(),
                    true,null,null,false,
                    SortOrder.ASC,ExternalConstants.EXTERNAL_QUERY_FIELD,false);
        }
        return EXTERNAL_QUERY_FIELD_DEF;
    }
    public static byte[] fromReadableValue(String value, PColumn column){
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Object convertValue = column.getDataType().toObject(value);
        ptr.set(value == null? ByteUtil.EMPTY_BYTE_ARRAY:Bytes.toBytes(value));
        column.getDataType().coerceBytes(ptr, convertValue, column.getDataType(),
                column.getDataType().getMaxLength(convertValue), column.getDataType().getScale(convertValue), SortOrder.getDefault(),
                column.getMaxLength(), column.getScale(), column.getSortOrder(),
                false);

        byte[] convertedBytes = column.getDataType().toBytes(column.getDataType().toObject(value));
        logger.debug("value {},convertValue {},convertType {},dataMaxLen {},dataScale {},maxLen {},scale {} ,ptr {},convertedBytes {}",value,
                convertValue,convertValue.getClass().getName(),
                column.getDataType().getMaxLength(convertValue),
                column.getDataType().getScale(convertValue),
                column.getMaxLength(),column.getScale(),
                Bytes.toHex(ptr.get()),Bytes.toHex(convertedBytes));
        // TODO: 2019年8月17日15:15:29 研究此处逻辑，使其更加适配
        if(column.getDataType().getSqlType() == PChar.INSTANCE.getSqlType()){
            convertedBytes = ptr.get();
        }
        return convertedBytes;
    }
    public static LinkedHashMap<String, PColumn> getPkColumnNameMap(PTable table){
        return getColumnNameMap(table.getPKColumns());
    }
    public static LinkedHashMap<String, PColumn> getColumnNameMap(PTable table){
        return getColumnNameMap(table.getColumns());
    }

    public static String getDataColumnName(PColumn column){
        return IndexUtil.getDataColumnName(column.getName().getString());
    }
    private static LinkedHashMap<String, PColumn> getColumnNameMap(List<PColumn> columns){
        LinkedHashMap<String, PColumn> columnMap = new LinkedHashMap<>(columns.size());
        for (PColumn column:columns){
            columnMap.put(getDataColumnName(column),column);
        }
        return columnMap;
    }
    public static boolean isInvalidExternalQuery(SelectStatement selectStatement){
        return selectStatement.getWhere() != null && selectStatement.getWhere().getChildren().size() != 2;
    }
    private static String getUnSingleQuote(String text){
        int start = text.startsWith("'")?1:0;
        int end = text.endsWith("'")?text.length()-1:text.length();
        return text.substring(start,end);
    }
    public static String getExternalQuery(SelectStatement selectStatement){
        return selectStatement.getWhere() == null ? null : getUnSingleQuote(selectStatement.getWhere().getChildren().get(1).toString());
    }
    public static int getBatchSize(ConfigurationReadableOnlyWrapper configuration){
        String batchSize = configuration.get( PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB);
        if(batchSize == null){
            batchSize = configuration.get(QueryServices.MUTATE_BATCH_SIZE_ATTRIB);
        }
        return batchSize == null ?
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE:
                Integer.parseInt(batchSize);
    }
    private static byte[][] getRowKeyBytes(final PhoenixResultSet resultSet,
                                           final PTable index,
                                           final LinkedHashMap<String,PColumn> pkColumns) throws SQLException {
        byte[][] values = new byte[index.getPKColumns().size()][];
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int i = 0;
        PhoenixResultSetMetaData metaData = (PhoenixResultSetMetaData) resultSet.getMetaData();
        for (String pkColumn:pkColumns.keySet()){
            PColumn column = pkColumns.get(pkColumn);
            int colIndex = resultSet.findColumn(pkColumn);
            byte[] bytes = resultSet.getBytes(pkColumn);
            ptr.set(bytes == null ? ByteUtil.EMPTY_BYTE_ARRAY : bytes);
            Object value = resultSet.getObject(pkColumn);
            int rsPrecision = metaData.getPrecision(colIndex);
            Integer precision = rsPrecision == 0 ? null : rsPrecision;
            int rsScale =metaData.getScale(colIndex);
            Integer scale = rsScale == 0 ? null : rsScale;
            if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(), SortOrder.getDefault(), precision,
                    scale, column.getMaxLength(), column.getScale())) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(column.getName().getString())
                        .setMessage("value=" + column.getDataType().toStringLiteral(ptr, null)).build()
                        .buildException();
            }
            column.getDataType().coerceBytes(ptr, value, column.getDataType(),
                    precision, scale, SortOrder.getDefault(),
                    column.getMaxLength(), column.getScale(), column.getSortOrder(),
                    index.rowKeyOrderOptimizable());
            values[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
            i++;
        }
        return values;
    }
    public static String getRowKeyHexString(final PhoenixResultSet resultSet,
                                            final PTable indexTable,
                                            final LinkedHashMap<String,PColumn> pkColumns,
                                            ImmutableBytesPtr ptr) throws SQLException {
        byte[][] pkValues = getRowKeyBytes(resultSet,indexTable,pkColumns);
        indexTable.newKey(ptr, pkValues);
        return Bytes.toHex(ptr.get(),0,ptr.getLength());
    }
    private static String buildIndexQuerySql(PTable indexTable,PTable dataTable){
        LinkedHashMap<String,PColumn> columns =  getColumnNameMap(indexTable);
        String schemaName = dataTable.getSchemaName().getString();
        String tableName = dataTable.getTableName().getString();
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (String columnName:columns.keySet()){

            if( ExternalConstants.EXTERNAL_QUERY_FIELD.equals(columnName) ){
                sb.append( SchemaUtil.getQuotedFullColumnName(null,columnName) ).append(",");
            }else{
                PColumn dataColumn = IndexUtil.getDataColumn(dataTable,columns.get(columnName).getName().getString());
                sb.append( SchemaUtil.getQuotedFullColumnName(dataColumn) ).append(",");
            }
        }
        sb.deleteCharAt(sb.length()-1);

        sb.append(" from ").append(SchemaUtil.getQualifiedTableName ( schemaName,tableName));
        logger.debug("build indexTable sql {}",sb);
        return sb.toString();
    }

    public static PhoenixResultSet queryIndex(final PhoenixStatement statement,final PTable indexTable,final PTable dataTable) throws SQLException {
        String selectSql = buildIndexQuerySql(indexTable,dataTable);
        return (PhoenixResultSet)statement.executeQuery(selectSql);
    }

    public static String getOrderByColumnName(OrderByExpression orderByExpression){
        return SchemaUtil.getTableNameFromFullName( SchemaUtil.getUnEscapedFullColumnName(orderByExpression.getExpression().toString()) );
    }
    public static String getAggregateFunctionFamilyName(Expression expression){
        return SchemaUtil.getSchemaNameFromFullName ( SchemaUtil.getUnEscapedFullColumnName(expression.toString())
                , QueryConstants.NAME_SEPARATOR);
    }
    public static String getAggregateFunctionColumnName(PTable indexTable, KeyValueColumnExpression kvce) throws AmbiguousColumnException, ColumnNotFoundException {
        String columnNameStr;
        if(EncodedColumnsUtil.usesEncodedColumnNames(indexTable)){
            PColumn pColumn = indexTable.getColumnForColumnQualifier(kvce.getColumnFamily(),kvce.getColumnQualifier());
            columnNameStr = SchemaUtil.getColumnName(pColumn.getFamilyName().getString(),pColumn.getName().getString());
        }else{
            columnNameStr = kvce.toString();
        }
        return IndexUtil.getDataColumnName(columnNameStr);
    }
    public static ConfigurationReadableOnlyWrapper getReadOnlyConfiguration(Configuration configuration){
        return new ConfigurationReadableOnlyWrapper(configuration);
    }
}
