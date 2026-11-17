package com.tds.pde.migration.target.azure;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * A block blob built from staged blocks (Put Block) and published in one atomic Put Block List.
 * Staged blocks are invisible to readers, and Azure discards uncommitted blocks after seven days, so an
 * interrupted upload leaves nothing behind that could pass for a finished file.
 */
@Slf4j
final class AzureStagedUpload implements StagedUpload {

    /** Azure's limit on committed blocks per blob. */
    static final int MAX_BLOCKS = 50_000;

    private static final String CONTENT_TYPE = "application/vnd.apache.parquet";

    private final BlockBlobClient blob;
    private final int blockSize;
    // Unique per attempt, so blocks left over from an earlier failed upload of the same blob are never committed.
    private final String uploadId = UUID.randomUUID().toString();
    private final List<String> blockIds = new ArrayList<>();
    private boolean streamOpened;
    private boolean streamClosed;

    AzureStagedUpload(BlockBlobClient blob, int blockSize) {
        this.blob = blob;
        this.blockSize = blockSize;
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
        log.debug("Upload abandoned, {} staged blocks left for Azure to discard: blob={}", blockIds.size(), blobName());
    }

    private String nextBlockId() {
        // Block ids must be Base64 and equally long within a blob.
        String raw = uploadId + "-" + String.format("%06d", blockIds.size());
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.US_ASCII));
    }

    private final class BlockStagingStream extends OutputStream {

        private final byte[] buffer = new byte[blockSize];
        private int buffered;

        @Override
        public void write(int b) throws IOException {
            if (buffered == buffer.length) {
                stageBufferedBlock();
            }
            buffer[buffered++] = (byte) b;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (streamClosed) {
                throw new IOException("Stream closed: " + blobName());
            }
            while (length > 0) {
                if (buffered == buffer.length) {
                    stageBufferedBlock();
                }
                int chunk = Math.min(length, buffer.length - buffered);
                System.arraycopy(bytes, offset, buffer, buffered, chunk);
                buffered += chunk;
                offset += chunk;
                length -= chunk;
            }
        }

        /** Stages what is buffered; publishing remains {@link #commit}'s job. */
        @Override
        public void close() throws IOException {
            if (!streamClosed) {
                if (buffered > 0) {
                    stageBufferedBlock();
                }
                streamClosed = true;
            }
        }

        private void stageBufferedBlock() throws IOException {
            if (blockIds.size() == MAX_BLOCKS) {
                throw new IOException(blobName() + " would exceed Azure's " + MAX_BLOCKS
                        + " blocks per blob; increase migration.target.block-size");
            }
            String blockId = nextBlockId();
            try {
                // Wraps the buffer without copying; staging is synchronous, so the buffer is free again afterwards.
                blob.stageBlock(blockId, BinaryData.fromByteBuffer(ByteBuffer.wrap(buffer, 0, buffered)));
            } catch (RuntimeException e) {
                throw new IOException("Failed to stage block " + blockIds.size() + " of " + blobName(), e);
            }
            blockIds.add(blockId);
            buffered = 0;
        }
    }
}
