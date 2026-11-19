package com.tds.pde.migration.target.azure;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * A block blob built from staged blocks (Put Block) and published in one atomic Put Block List.
 * Staged blocks are invisible to readers, and Azure discards uncommitted blocks after seven days, so an
 * interrupted upload leaves nothing behind that could pass for a finished file.
 *
 * <p>Staging overlaps with writing: a full buffer is handed to a background thread while the writer fills the
 * other of two buffers, so the writer only waits when staging is slower than producing. At most one block is
 * in flight per upload. A failed block is reported by the next write or close, never lost, and
 * {@link #commit} is refused unless every block was staged.
 */
@Slf4j
final class AzureStagedUpload implements StagedUpload {

    /** Azure's limit on committed blocks per blob. */
    static final int MAX_BLOCKS = 50_000;

    private static final String CONTENT_TYPE = "application/vnd.apache.parquet";

    private final BlockBlobClient blob;
    private final int blockSize;
    private final ExecutorService stagingThreads;
    // Unique per attempt, so blocks left over from an earlier failed upload of the same blob are never committed.
    private final String uploadId = UUID.randomUUID().toString();
    private final List<String> blockIds = new ArrayList<>();
    private boolean streamOpened;
    private boolean streamClosed;
    private @Nullable Future<?> staging;
    private @Nullable IOException failure;
    private int buffersAllocated;

    AzureStagedUpload(BlockBlobClient blob, int blockSize, ExecutorService stagingThreads) {
        this.blob = blob;
        this.blockSize = blockSize;
        this.stagingThreads = stagingThreads;
    }

    @Override
    public String blobName() {
        return blob.getBlobName();
    }

    @Override
    public OutputStream outputStream() {
        if (streamOpened) {
            throw new IllegalStateException("Output stream already opened for " + blobName());
        }
        streamOpened = true;
        return new BlockStagingStream();
    }

    @Override
    public void commit(Map<String, String> metadata) {
        if (!streamClosed) {
            throw new IllegalStateException("Cannot commit " + blobName() + " before its output stream is closed");
        }
        blob.commitBlockListWithResponse(new BlockBlobCommitBlockListOptions(blockIds)
                        .setMetadata(metadata)
                        .setHeaders(new BlobHttpHeaders().setContentType(CONTENT_TYPE)),
                null, Context.NONE);
    }

    @Override
    public void abort() {
        Future<?> inFlight = staging;
        if (inFlight != null) {
            inFlight.cancel(true);
        }
        log.debug("Upload abandoned, {} staged blocks left for Azure to discard: blob={}", blockIds.size(), blobName());
    }

    int buffersAllocated() {
        return buffersAllocated;
    }

    private String nextBlockId() {
        // Block ids must be Base64 and equally long within a blob.
        String raw = uploadId + "-" + String.format("%06d", blockIds.size());
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.US_ASCII));
    }

    private final class BlockStagingStream extends OutputStream {

        /** The buffer being filled. Allocated when the next byte needs it, so a one-block file uses one buffer. */
        private byte @Nullable [] filling;
        /** The buffer not being filled: free, or holding the block being staged. */
        private byte @Nullable [] other;
        private int buffered;

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (streamClosed) {
                throw new IOException("Stream closed: " + blobName());
            }
            while (length > 0) {
                if (buffered == blockSize) {
                    stageBufferedBlock();
                }
                if (filling == null) {
                    filling = new byte[blockSize];
                    buffersAllocated++;
                }
                int chunk = Math.min(length, blockSize - buffered);
                System.arraycopy(bytes, offset, filling, buffered, chunk);
                buffered += chunk;
                offset += chunk;
                length -= chunk;
            }
        }

        /** Stages what is buffered and waits for every block; publishing remains {@link #commit}'s job. */
        @Override
        public void close() throws IOException {
            if (streamClosed) {
                return;
            }
            if (buffered > 0) {
                stageBufferedBlock();
            }
            awaitStaging();
            streamClosed = true;
        }

        /**
         * Hands the full buffer to a staging thread and carries on in the other one. Waits first for the block
         * staged before it, which also reports that block's failure.
         */
        private void stageBufferedBlock() throws IOException {
            awaitStaging();
            if (blockIds.size() == MAX_BLOCKS) {
                throw new IOException(blobName() + " would exceed Azure's " + MAX_BLOCKS
                        + " blocks per blob; increase migration.target.block-size");
            }
            int index = blockIds.size();
            String blockId = nextBlockId();
            byte[] block = Objects.requireNonNull(filling);
            int length = buffered;
            blockIds.add(blockId);
            staging = stagingThreads.submit(() -> {
                // Wraps the block without copying; it is not touched again until staging has finished.
                blob.stageBlock(blockId, BinaryData.fromByteBuffer(ByteBuffer.wrap(block, 0, length)));
                log.trace("Block staged: blob={} block={} bytes={}", blobName(), index, length);
            });
            filling = other;
            other = block;
            buffered = 0;
        }

        /** Waits for the block being staged; once any block has failed, every call fails. */
        private void awaitStaging() throws IOException {
            if (failure != null) {
                throw failure;
            }
            Future<?> inFlight = staging;
            if (inFlight == null) {
                return;
            }
            try {
                inFlight.get();
            } catch (ExecutionException e) {
                failure = new IOException("Failed to stage block " + (blockIds.size() - 1) + " of " + blobName(),
                        e.getCause());
                throw failure;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted while staging a block of " + blobName());
            }
            staging = null;
        }
    }
}
