package org.apache.phoenix.external.index;

import org.apache.hadoop.hbase.client.Row;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Future;

public interface IExternalBatch<T> extends Closeable {
    void open();
    void batch(final List<? extends Row> actions) throws IOException,InterruptedException, SQLException;
    Future<T> batchAsync(final List<? extends Row> actions) throws IOException,InterruptedException, SQLException;
    void close() throws IOException;
}
