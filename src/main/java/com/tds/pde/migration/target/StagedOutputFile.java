package com.tds.pde.migration.target;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.jspecify.annotations.Nullable;

/**
 * Lets Iceberg's Parquet writer write into a {@link StagedUpload}. Closing the writer stages the last bytes;
 * publishing stays a separate {@link StagedUpload#commit} so the writer can be verified first.
 */
final class StagedOutputFile implements OutputFile {

    private final StagedUpload upload;
    private @Nullable CountingStream stream;

    StagedOutputFile(StagedUpload upload) {
        this.upload = upload;
    }

    /** Bytes the writer has handed to the upload so far; unlike the writer's own length, it never counts buffers. */
    long bytesWritten() {
        return stream == null ? 0 : stream.getPos();
    }

    @Override
    public PositionOutputStream create() {
        stream = new CountingStream(upload.outputStream());
        return stream;
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        return create();
    }

    @Override
    public String location() {
        return upload.blobName();
    }

    @Override
    public InputFile toInputFile() {
        throw new UnsupportedOperationException("Staged content of " + upload.blobName() + " cannot be read");
    }

    private static final class CountingStream extends PositionOutputStream {

        private final OutputStream delegate;
        private long position;

        CountingStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
