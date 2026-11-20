package com.tds.pde.migration.target.hadoop;

import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * A file written under a temporary name and renamed to its final name only once it is complete: the stream is closed,
 * the temporary file holds exactly the bytes written, and nothing exists under the final name yet. After the rename
 * the published file is checked to hold those bytes too.
 *
 * <p>On a flat namespace the rename is a server-side copy of the file and a delete of the temporary one, so the file
 * under its final name is complete once the copy is, but a crash between the two leaves the temporary file behind,
 * for the next clean to delete. A failed or abandoned upload deletes its temporary file. Nothing is ever written under
 * the final name except by the rename.
 *
 * <p>The file system has no blob metadata: the proof that a file is done is its file row, recorded as completed only
 * after the rename and the size check.
 */
@Slf4j
final class HadoopStagedUpload implements StagedUpload {

    private final HadoopFileSystem fileSystem;
    private final String blobName;
    private final URI target;
    private final URI temporary;
    private @Nullable CountingStream stream;
    private boolean published;

    HadoopStagedUpload(HadoopFileSystem fileSystem, String blobName, URI target, URI temporary) {
        this.fileSystem = fileSystem;
        this.blobName = blobName;
        this.target = target;
        this.temporary = temporary;
    }

    @Override
    public String blobName() {
        return blobName;
    }

    /** The content, written to the temporary file. */
    @Override
    public OutputStream outputStream() {
        if (stream != null) {
            throw new IllegalStateException("Output stream already opened for " + blobName);
        }
        try {
            stream = new CountingStream(fileSystem.create(temporary));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create " + temporary + ": " + e.getMessage(), e);
        }
        return stream;
    }

    /**
     * Renames the checked temporary file to the final name. The metadata is not stored: the file system has none.
     *
     * @throws UncheckedIOException when the file cannot be published; nothing is then left under the final name
     */
    @Override
    public void commit(Map<String, String> metadata) {
        CountingStream written = stream;
        if (written == null || !written.closed) {
            throw new IllegalStateException("Cannot commit " + blobName + " before its output stream is closed");
        }
        try {
            long length = fileSystem.length(temporary).orElseThrow(() -> new IOException(
                    "the temporary file " + temporary + " is gone"));
            if (length != written.count) {
                throw new IOException("the temporary file " + temporary + " holds " + length + " bytes, but "
                        + written.count + " were written");
            }
            if (fileSystem.length(target).isPresent()) {
                throw new IOException(target + " already exists, and is not replaced");
            }
            if (!fileSystem.rename(temporary, target)) {
                throw new IOException("the file system refused to rename " + temporary + " to " + target);
            }
            published = true;
            long publishedLength = fileSystem.length(target).orElseThrow(() -> new IOException(
                    target + " is missing after the rename"));
            if (publishedLength != written.count) {
                deletePublished();
                throw new IOException(target + " holds " + publishedLength + " bytes after the rename, but "
                        + written.count + " were written; it was deleted");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot publish " + blobName + ": " + e.getMessage(), e);
        }
        log.debug("File published by rename: from={} to={} bytes={}", temporary, target, written.count);
    }

    /** Closes the stream if it is still open and deletes the temporary file. Never throws. */
    @Override
    public void abort() {
        CountingStream open = stream;
        if (open != null && !open.closeAttempted) {
            try {
                open.close();
            } catch (IOException | RuntimeException e) {
                log.debug("Closing the abandoned upload failed: file={} reason={}", temporary, e.toString());
            }
        }
        if (published) {
            return;
        }
        try {
            fileSystem.delete(temporary, false);
        } catch (IOException | RuntimeException e) {
            log.warn("Temporary file of a failed upload not deleted; the next clean deletes it: file={} reason={}",
                    temporary, e.toString());
        }
    }

    /** Takes back a published file that does not hold what was written, so no wrong file stays under its name. */
    private void deletePublished() {
        try {
            fileSystem.delete(target, false);
            published = false;
        } catch (IOException | RuntimeException e) {
            log.error("A published file of the wrong size could not be deleted; the run fails, and the next clean "
                    + "deletes it: file={} reason={}", target, e.toString());
        }
    }

    private static final class CountingStream extends OutputStream {

        private final OutputStream delegate;
        private long count;
        private boolean closeAttempted;
        private boolean closed;

        CountingStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            count++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            delegate.write(bytes, offset, length);
            count += length;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        /** Closing writes what the file system still buffers, so a failed close fails the file. */
        @Override
        public void close() throws IOException {
            if (closeAttempted) {
                return;
            }
            closeAttempted = true;
            delegate.close();
            closed = true;
        }
    }
}
