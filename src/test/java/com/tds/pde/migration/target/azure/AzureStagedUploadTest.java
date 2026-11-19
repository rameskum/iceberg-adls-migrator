package com.tds.pde.migration.target.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The staging logic against a stand-in for the Blob API, so the timing of Put Block calls can be controlled. */
class AzureStagedUploadTest {

    private static final int BLOCK_SIZE = 100;

    private final ExecutorService stagingThreads = Executors.newCachedThreadPool();
    private final ExecutorService writer = Executors.newSingleThreadExecutor();
    private final BlockBlobClient blob = mock(BlockBlobClient.class);
    private final List<String> stagedIds = Collections.synchronizedList(new ArrayList<>());
    private final ByteArrayOutputStream stagedBytes = new ByteArrayOutputStream();
    private final AtomicInteger stageCalls = new AtomicInteger();

    @AfterEach
    void shutDown() {
        writer.shutdownNow();
        stagingThreads.shutdownNow();
    }

    private AzureStagedUpload upload() {
        when(blob.getBlobName()).thenReturn("part.parquet");
        return new AzureStagedUpload(blob, BLOCK_SIZE, stagingThreads);
    }

    private Future<?> writeInBackground(OutputStream out, int bytes) {
        return writer.submit(() -> {
            out.write(new byte[bytes]);
            return null;
        });
    }

    /** Records each block as the stand-in service receives it, after {@code gate} lets it through. */
    private void stageThrough(CountDownLatch gate) {
        doAnswer(invocation -> {
            stageCalls.incrementAndGet();
            byte[] content = invocation.<BinaryData>getArgument(1).toBytes();
            gate.await(10, TimeUnit.SECONDS);
            synchronized (stagedBytes) {
                stagedBytes.write(content);
            }
            stagedIds.add(invocation.getArgument(0));
            return null;
        }).when(blob).stageBlock(anyString(), any(BinaryData.class));
    }

    @Test
    void keepsWritingWhileTheFullBlockIsStaged() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        stageThrough(release);
        OutputStream out = upload().outputStream();

        // 150 bytes: the first block is full and handed off, the rest fills the other buffer, nobody waits.
        Future<?> firstWrite = writeInBackground(out, 150);
        firstWrite.get(5, TimeUnit.SECONDS);
        assertThat(stagedIds).as("the first block has not been staged yet").isEmpty();

        // The second block is full too, but only one block is in flight: the writer waits for the first.
        Future<?> secondWrite = writeInBackground(out, 100);
        assertThatThrownBy(() -> secondWrite.get(300, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

        release.countDown();
        secondWrite.get(5, TimeUnit.SECONDS);
    }

    @Test
    void stagesEveryBlockInOrderAndCommitsTheirIdsInTheSameOrder() throws Exception {
        stageThrough(new CountDownLatch(0));
        AzureStagedUpload upload = upload();
        byte[] content = new byte[BLOCK_SIZE * 7 + 33];
        new Random(1).nextBytes(content);

        try (OutputStream out = upload.outputStream()) {
            // Odd write sizes that straddle block boundaries.
            int position = 0;
            for (int size = 1; position < content.length; size = size * 3 + 1) {
                int chunk = Math.min(size, content.length - position);
                out.write(content, position, chunk);
                position += chunk;
            }
        }
        upload.commit(Map.of("records_written", "7"));

        assertThat(stagedBytes.toByteArray()).isEqualTo(content);
        assertThat(stagedIds).hasSize(8).doesNotHaveDuplicates();
        ArgumentCaptor<BlockBlobCommitBlockListOptions> commit =
                ArgumentCaptor.forClass(BlockBlobCommitBlockListOptions.class);
        verify(blob).commitBlockListWithResponse(commit.capture(), any(), any());
        assertThat(commit.getValue().getBase64BlockIds()).containsExactlyElementsOf(stagedIds);
        assertThat(commit.getValue().getMetadata()).containsEntry("records_written", "7");
    }

    @Test
    void waitsForTheLastBlockWhenTheStreamIsClosed() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        stageThrough(release);
        AzureStagedUpload upload = upload();
        OutputStream out = upload.outputStream();
        out.write(new byte[30]);

        Future<?> close = writer.submit(() -> {
            out.close();
            return null;
        });
        assertThatThrownBy(() -> close.get(300, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        assertThatThrownBy(() -> upload.commit(Map.of())).isInstanceOf(IllegalStateException.class);

        release.countDown();
        close.get(5, TimeUnit.SECONDS);
        upload.commit(Map.of());
        assertThat(stagedBytes.size()).isEqualTo(30);
    }

    @Test
    void reportsAFailedBlockOnTheNextWriteAndRefusesToCommit() throws Exception {
        doAnswer(invocation -> {
            throw new IllegalStateException("service unavailable");
        }).when(blob).stageBlock(anyString(), any(BinaryData.class));
        AzureStagedUpload upload = upload();
        OutputStream out = upload.outputStream();

        out.write(new byte[BLOCK_SIZE + 1]);
        // The failed first block is reported when the writer needs the staging thread again.
        assertThatThrownBy(() -> out.write(new byte[BLOCK_SIZE]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to stage block 0 of part.parquet")
                .hasRootCauseMessage("service unavailable");
        assertThatThrownBy(out::close).isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to stage block 0");
        assertThatThrownBy(() -> upload.commit(Map.of())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before its output stream is closed");
    }

    @Test
    void reportsAFailureOfTheLastBlockWhenTheStreamIsClosed() throws Exception {
        doAnswer(invocation -> {
            throw new IllegalStateException("service unavailable");
        }).when(blob).stageBlock(anyString(), any(BinaryData.class));
        OutputStream out = upload().outputStream();
        out.write(new byte[10]);

        assertThatThrownBy(out::close).isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to stage block 0 of part.parquet");
    }

    @Test
    void usesNoSecondBufferForAFileThatFitsInOneBlock() throws Exception {
        stageThrough(new CountDownLatch(0));
        AzureStagedUpload upload = upload();

        try (OutputStream out = upload.outputStream()) {
            out.write(new byte[BLOCK_SIZE]);
        }

        assertThat(stageCalls).hasValue(1);
        assertThat(upload.buffersAllocated()).isEqualTo(1);
    }

    @Test
    void reusesTwoBuffersForAFileOfManyBlocks() throws Exception {
        stageThrough(new CountDownLatch(0));
        AzureStagedUpload upload = upload();

        try (OutputStream out = upload.outputStream()) {
            for (int i = 0; i < 10; i++) {
                out.write(new byte[BLOCK_SIZE]);
            }
        }

        assertThat(stageCalls).hasValue(10);
        assertThat(upload.buffersAllocated()).isEqualTo(2);
    }
}
