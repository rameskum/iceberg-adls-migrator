package com.tds.pde.migration.target.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.target.BlobStore.BlobInfo;
import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Hadoop store against Hadoop's own local file system: publishing by rename, and what it does when the temporary
 * file, the final name or the rename is not as it must be.
 */
class HadoopBlobStoreTest {

    private static final String RUN_ID = "20261120-090000-a1b2c3";
    private static final String NAME = "sales/orders/region=EU__part-00000-9f3e1c2a.parquet";
    private static final byte[] CONTENT = {1, 2, 3};

    @TempDir
    Path tempDir;

    private final RefusingLocalFileSystem local = new RefusingLocalFileSystem();
    private HadoopBlobStore store;

    @BeforeEach
    void openStore() throws IOException {
        local.initialize(URI.create("file:///"), new Configuration());
        store = new HadoopBlobStore(HadoopFileSystem.wrap(local), tempDir.toUri());
    }

    @Test
    void publishesUnderTheFinalNameOnlyByRenamingTheCheckedTemporaryFile() throws IOException {
        StagedUpload upload = store.startUpload(NAME, RUN_ID);
        Path temporary = tempDir.resolve("sales/orders/_migration/tmp/" + RUN_ID
                + "/region=EU__part-00000-9f3e1c2a.parquet");
        try (OutputStream out = upload.outputStream()) {
            out.write(CONTENT);
        }

        assertThat(temporary).hasBinaryContent(CONTENT);
        assertThat(file(NAME)).doesNotExist();

        upload.commit(Map.of("migration_run_id", RUN_ID));

        assertThat(file(NAME)).hasBinaryContent(CONTENT);
        assertThat(temporary).doesNotExist();
        assertThat(store.list("sales/orders/")).containsExactly(new BlobInfo(NAME, CONTENT.length));
    }

    @Test
    void abortDeletesTheTemporaryFileAndPublishesNothing() throws IOException {
        StagedUpload writing = store.startUpload(NAME, RUN_ID);
        writing.outputStream().write(CONTENT);
        StagedUpload written = store.startUpload("sales/orders/b.parquet", RUN_ID);
        try (OutputStream out = written.outputStream()) {
            out.write(CONTENT);
        }

        writing.abort();
        written.abort();
        written.abort();

        assertThat(store.list("sales/orders/")).isEmpty();
    }

    @Test
    void refusesATemporaryFileThatDoesNotHoldWhatWasWritten() throws IOException {
        StagedUpload upload = store.startUpload(NAME, RUN_ID);
        try (OutputStream out = upload.outputStream()) {
            out.write(CONTENT);
        }
        Files.write(tempDir.resolve("sales/orders/_migration/tmp/" + RUN_ID + "/" + NAME.substring(13)),
                new byte[] {4}, StandardOpenOption.APPEND);

        assertThatThrownBy(() -> upload.commit(Map.of()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("holds 4 bytes, but 3 were written");
        upload.abort();

        assertThat(file(NAME)).doesNotExist();
        assertThat(store.list("sales/orders/")).isEmpty();
    }

    @Test
    void neverReplacesAFileUnderTheFinalName() throws IOException {
        Files.createDirectories(file(NAME).getParent());
        Files.write(file(NAME), new byte[] {9});
        StagedUpload upload = store.startUpload(NAME, RUN_ID);
        try (OutputStream out = upload.outputStream()) {
            out.write(CONTENT);
        }

        assertThatThrownBy(() -> upload.commit(Map.of()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("already exists, and is not replaced");
        upload.abort();

        assertThat(file(NAME)).hasBinaryContent(new byte[] {9});
        assertThat(store.list("sales/orders/")).containsExactly(new BlobInfo(NAME, 1));
    }

    @Test
    void publishesNothingWhenTheFileSystemRefusesTheRename() throws IOException {
        local.refuseRenames = true;
        StagedUpload upload = store.startUpload(NAME, RUN_ID);
        try (OutputStream out = upload.outputStream()) {
            out.write(CONTENT);
        }

        assertThatThrownBy(() -> upload.commit(Map.of()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("the file system refused to rename");
        upload.abort();

        assertThat(store.list("sales/orders/")).isEmpty();
    }

    @Test
    void refusesToCommitBeforeTheStreamIsClosed() throws IOException {
        StagedUpload upload = store.startUpload(NAME, RUN_ID);
        assertThatThrownBy(() -> upload.commit(Map.of())).isInstanceOf(IllegalStateException.class);
        upload.outputStream().write(CONTENT);
        assertThatThrownBy(() -> upload.commit(Map.of())).isInstanceOf(IllegalStateException.class);
        upload.abort();

        assertThat(file(NAME)).doesNotExist();
    }

    @Test
    void listsEveryFileUnderThePrefixWithItsSizeIncludingTemporaryFiles() throws IOException {
        publish("sales/orders/a.parquet", 1);
        publish("sales/orders-archive/a.parquet", 2);
        publish("sales/orders/b.parquet", 3);
        StagedUpload unfinished = store.startUpload("sales/orders/c.parquet", RUN_ID);
        unfinished.outputStream().close();

        assertThat(store.list("sales/orders/")).containsExactly(
                new BlobInfo("sales/orders/_migration/tmp/" + RUN_ID + "/c.parquet", 0),
                new BlobInfo("sales/orders/a.parquet", 1),
                new BlobInfo("sales/orders/b.parquet", 3));
        assertThat(store.list("sales/orders")).as("a name prefix, not only a folder").hasSize(4);
        assertThat(store.list("sales/missing/")).isEmpty();
        assertThat(store.list("")).hasSize(4);
    }

    @Test
    void deletesFilesAndTheFolderOfTemporaryFiles() throws IOException {
        publish("sales/orders/a.parquet", 1);
        StagedUpload unfinished = store.startUpload("sales/orders/b.parquet", RUN_ID);
        unfinished.outputStream().close();

        store.delete("sales/orders/_migration/tmp/" + RUN_ID + "/b.parquet");
        store.delete("sales/orders/missing.parquet");
        store.deleteUploadLeftovers("sales/orders/");
        store.deleteUploadLeftovers("sales/orders/");

        assertThat(tempDir.resolve("sales/orders/_migration")).doesNotExist();
        try (Stream<Path> files = Files.list(tempDir.resolve("sales/orders"))) {
            assertThat(files).containsExactly(file("sales/orders/a.parquet"));
        }

        store.delete("sales/orders/a.parquet");

        assertThat(store.list("sales/orders/")).isEmpty();
    }

    private void publish(String name, int length) throws IOException {
        StagedUpload upload = store.startUpload(name, RUN_ID);
        try (OutputStream out = upload.outputStream()) {
            out.write(new byte[length]);
        }
        upload.commit(Map.of());
    }

    private Path file(String name) {
        return tempDir.resolve(name);
    }

    /** Hadoop's local file system, without checksum files, that can refuse renames as a storage service might. */
    static final class RefusingLocalFileSystem extends RawLocalFileSystem {

        volatile boolean refuseRenames;

        @Override
        public boolean rename(org.apache.hadoop.fs.Path source, org.apache.hadoop.fs.Path destination)
                throws IOException {
            return !refuseRenames && super.rename(source, destination);
        }
    }
}
