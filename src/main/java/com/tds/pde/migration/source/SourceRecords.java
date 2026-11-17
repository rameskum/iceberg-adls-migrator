package com.tds.pde.migration.source;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * The live rows of one data file, streamed with deletes applied. Also counts the physical rows read before
 * deletes, so deleted rows can be reported and the read checked against the file's metadata.
 */
public final class SourceRecords implements CloseableIterable<Record> {

    private final Schema schema;
    private final CloseableIterable<Record> liveRows;
    private long physicalRowsRead;

    SourceRecords(Schema schema, CloseableIterable<Record> physicalRows, DeleteFilter<Record> deletes) {
        this.schema = schema;
        this.liveRows = deletes.filter(CloseableIterable.transform(physicalRows, this::countPhysicalRow));
    }

    /** Schema of the records returned; may include metadata columns needed to apply deletes. */
    public Schema schema() {
        return schema;
    }

    public long physicalRowsRead() {
        return physicalRowsRead;
    }

    @Override
    public CloseableIterator<Record> iterator() {
        return liveRows.iterator();
    }

    @Override
    public void close() throws IOException {
        liveRows.close();
    }

    private Record countPhysicalRow(Record row) {
        physicalRowsRead++;
        return row;
    }
}
