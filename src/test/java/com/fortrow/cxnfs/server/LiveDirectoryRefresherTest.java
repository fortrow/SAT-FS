package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.Chunker;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.transport.PayloadStagingConfig;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkPayloadPatternCodec;
import com.fortrow.cxnfs.transport.StreamingVerifiedEntryPublisher;
import com.fortrow.cxnfs.transport.TransportBinding;
import com.fortrow.cxnfs.transport.TransportRole;
import com.fortrow.cxnfs.transport.TransportSecurityConfig;
import com.fortrow.cxnfs.transport.VerifiedEntryPublisher;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferChecksum;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LiveDirectoryRefresherTest {
    @TempDir
    Path tempDir;

    @Test
    void serverNamespaceReadsRefreshAddedChangedAndRemovedSourceFiles() throws Exception {
        Path source = tempDir.resolve("share");
        Files.createDirectories(source);
        Files.writeString(source.resolve("hello.txt"), "hello", StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, repository, Duration.ZERO);
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(config(roots), repository, refresher);
        assertEquals(0, repository.containerCount());
        assertEquals(0, repository.chunkCount());

        Files.writeString(source.resolve("later.txt"), "later", StandardCharsets.UTF_8);
        assertEquals(
                List.of("hello.txt", "later.txt"),
                server.list("/share").entries().stream().map(FileMetadata::name).toList()
        );

        Files.writeString(source.resolve("later.txt"), "changed", StandardCharsets.UTF_8);
        assertEquals("changed".length(), server.stat("/share/later.txt").orElseThrow().size());

        Files.delete(source.resolve("hello.txt"));
        assertEquals(
                List.of("later.txt"),
                server.list("/share").entries().stream().map(FileMetadata::name).toList()
        );
        assertTrue(server.stat("/share/hello.txt").isEmpty());
    }

    @Test
    void refreshIndexesMetadataAndHydratesManifestOnlyOnDemand() throws Exception {
        Path source = tempDir.resolve("lazy-share");
        Files.createDirectories(source);
        byte[] data = "lazy manifest content\n".getBytes(StandardCharsets.UTF_8);
        Files.write(source.resolve("model.bin"), data);
        CountingChunker chunker = new CountingChunker();
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, chunker, Duration.ZERO);

        DirectoryLoadResult loaded = refresher.refreshNow();

        assertEquals(1, loaded.filesLoaded());
        assertEquals(data.length, loaded.bytesLoaded());
        assertEquals(0, chunker.calls());
        FileMetadata metadata = refresher.stat("/share/model.bin").orElseThrow();
        assertEquals(data.length, metadata.size());
        assertFalse(metadata.contentHash().isPresent());
        assertFalse(refresher.list("/share").entries().getFirst().contentHash().isPresent());
        assertEquals(0, chunker.calls());

        FileManifest manifest = refresher.manifest("/share/model.bin").orElseThrow();

        assertEquals(1, chunker.calls());
        assertEquals(Hashing.file(
                manifest.chunks().stream().map(ChunkRef::hash).toList(),
                manifest.chunks().stream().map(ChunkRef::size).toList()
        ), manifest.fileHash());
        assertEquals(manifest.fileHash(), refresher.stat("/share/model.bin").orElseThrow().contentHash().orElseThrow());

        refresher.readPlan("/share/model.bin", 0, data.length);
        assertEquals(1, chunker.calls());
    }

    @Test
    void serverMutationsWriteThroughToServedSourceDirectory() throws Exception {
        Path source = tempDir.resolve("write-through-share");
        Files.createDirectories(source);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, repository, Duration.ZERO);
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(config(roots), repository, refresher);

        publish(server, "/share/written.txt", "written".getBytes(StandardCharsets.UTF_8));
        assertEquals("written", Files.readString(source.resolve("written.txt"), StandardCharsets.UTF_8));

        publish(server, "/share/empty.txt", new byte[0]);
        assertEquals(0L, Files.size(source.resolve("empty.txt")));

        publish(server, "/share/current.txt", "old".getBytes(StandardCharsets.UTF_8));
        publish(server, "/share/.current.tmp", "new".getBytes(StandardCharsets.UTF_8));
        server.renamePath("/share/.current.tmp", "/share/current.txt");
        assertTrue(Files.notExists(source.resolve(".current.tmp")));
        assertEquals("new", Files.readString(source.resolve("current.txt"), StandardCharsets.UTF_8));

        server.renamePath("/share/written.txt", "/share/renamed.txt");
        assertTrue(Files.notExists(source.resolve("written.txt")));
        assertEquals("written", Files.readString(source.resolve("renamed.txt"), StandardCharsets.UTF_8));

        server.deletePath("/share/renamed.txt", false);
        assertTrue(Files.notExists(source.resolve("renamed.txt")));
    }

    @Test
    void servedRootWritePublishesSourceBackedFileWithoutRepositoryContainers() throws Exception {
        Path source = tempDir.resolve("source-backed-share");
        Files.createDirectories(source);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, repository, Duration.ZERO);
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(config(roots), repository, refresher);
        byte[] first = "first live source chunk\n".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second live source chunk\n".getBytes(StandardCharsets.UTF_8);
        ContentHash firstHash = Hashing.chunk(first);
        ContentHash secondHash = Hashing.chunk(second);
        ContentHash fileHash = Hashing.file(List.of(firstHash, secondHash), List.of(first.length, second.length));
        FileManifest manifest = new FileManifest(
                "/share/model.bin",
                fileHash,
                first.length + second.length,
                List.of(new ChunkRef(firstHash, first.length), new ChunkRef(secondHash, second.length))
        );

        VerifiedEntryPublisher publisher = server.verifiedEntryPublisher("/share/model.bin");
        publisher.acceptChunk(firstHash, 0L, first);
        publisher.acceptChunk(secondHash, first.length, second);
        FileManifest published = publisher.publishManifest(manifest);

        assertEquals(fileHash, published.fileHash());
        assertEquals(0, repository.containerCount());
        assertEquals(0, repository.chunkCount());
        assertEquals(
                "first live source chunk\nsecond live source chunk\n",
                Files.readString(source.resolve("model.bin"), StandardCharsets.UTF_8)
        );
        assertEquals(
                "first live source chunk\nsecond live source chunk\n",
                new String(
                        server.readRange("/share/model.bin", 0, first.length + second.length),
                        StandardCharsets.UTF_8
                )
        );

        ReadPlan plan = server.readPlan(fileHash, 0, first.length + second.length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        server.readVerifiedPlanChunks(plan, (chunk, payload) ->
                out.write(payload, chunk.offsetInChunk(), chunk.bytesToRead()));
        assertEquals(
                "first live source chunk\nsecond live source chunk\n",
                out.toString(StandardCharsets.UTF_8)
        );
    }

    @Test
    void servedRootStreamingWriteAcceptsOutOfOrderFramesAndSparseZeroRuns() throws Exception {
        Path source = tempDir.resolve("positional-share");
        Files.createDirectories(source);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, repository, Duration.ZERO);
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(config(roots), repository, refresher);
        UUID transferId = UUID.randomUUID();
        int zeroLength = 4096;
        byte[] tail = "tail after sparse zero run".getBytes(StandardCharsets.UTF_8);
        ContentHash zeroHash = Hashing.chunkRun((byte) 0, zeroLength);
        ContentHash tailHash = Hashing.chunk(tail);
        ContentHash fileHash = Hashing.file(List.of(zeroHash, tailHash), List.of(zeroLength, tail.length));
        StreamingVerifiedEntryPublisher publisher = (StreamingVerifiedEntryPublisher) server.verifiedEntryPublisher(
                "/share/zeros.bin",
                zeroLength + tail.length
        );

        publisher.acceptFrame(new BulkDataFrame(
                transferId,
                1,
                0,
                1,
                zeroLength,
                true,
                tail
        ));
        publisher.acceptFrame(new BulkDataFrame(
                transferId,
                0,
                0,
                0,
                0,
                true,
                new BulkPayloadPatternCodec.DecodedPayload(
                        zeroLength,
                        null,
                        Optional.of(new BulkPayloadPatternCodec.PayloadPattern(
                                BulkPayloadPatternCodec.PayloadPatternKind.RUN,
                                zeroLength,
                                (byte) 0,
                                new byte[0]
                        ))
                )
        ));
        assertEquals(2, publisher.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                zeroLength + tail.length,
                List.of(zeroHash, tailHash)
        )));
        publisher.publishEntry(new TransferEntryCommitRequest(
                transferId,
                0,
                "/share/zeros.bin",
                fileHash,
                zeroLength + tail.length
        ));

        Path target = source.resolve("zeros.bin");
        assertEquals(zeroLength + tail.length, Files.size(target));
        byte[] prefix = server.readRange("/share/zeros.bin", 0, zeroLength);
        assertEquals(zeroLength, prefix.length);
        for (byte value : prefix) {
            assertEquals(0, value);
        }
        assertEquals(
                "tail after sparse zero run",
                new String(server.readRange("/share/zeros.bin", zeroLength, tail.length), StandardCharsets.UTF_8)
        );
        assertTrue(Files.notExists(source.resolve("zeros.bin.tmp")));
        assertTrue(Files.notExists(source.resolve("zeros.bin.tmp.cxnfs-state")));
        assertEquals(0, repository.containerCount());
        assertEquals(0, repository.chunkCount());
    }

    @Test
    void servedRootStreamingWriteCanUseChunkFileFallbackMode() throws Exception {
        Path source = tempDir.resolve("chunk-fallback-share");
        Files.createDirectories(source);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(
                roots,
                GearHashChunker.defaults(),
                Duration.ZERO,
                LiveWriteMode.CHUNK_FILES
        );
        refresher.refreshNow();
        CxnFileServer server = new CxnFileServer(config(roots), repository, refresher);
        UUID transferId = UUID.randomUUID();
        byte[] first = "first chunk from fallback\n".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second chunk from fallback\n".getBytes(StandardCharsets.UTF_8);
        ContentHash firstHash = Hashing.chunk(first);
        ContentHash secondHash = Hashing.chunk(second);
        ContentHash fileHash = Hashing.file(List.of(firstHash, secondHash), List.of(first.length, second.length));
        StreamingVerifiedEntryPublisher publisher = (StreamingVerifiedEntryPublisher) server.verifiedEntryPublisher(
                "/share/fallback.bin",
                first.length + second.length
        );

        publisher.acceptFrame(new BulkDataFrame(
                transferId,
                1,
                0,
                1,
                first.length,
                true,
                second
        ));
        publisher.acceptFrame(new BulkDataFrame(
                transferId,
                0,
                0,
                0,
                0,
                true,
                first
        ));
        assertEquals(2, publisher.acceptMetadata(new TransferMetadataUpdate(
                transferId,
                0,
                0,
                true,
                first.length + second.length,
                List.of(firstHash, secondHash)
        )));

        Path chunkDirectory = source.resolve("fallback.bin.tmp.chunks");
        waitForPath(chunkDirectory.resolve("00000000000000000000.chunk"));
        waitForPath(chunkDirectory.resolve("00000000000000000001.chunk"));

        publisher.publishEntry(new TransferEntryCommitRequest(
                transferId,
                0,
                "/share/fallback.bin",
                fileHash,
                first.length + second.length,
                crc32c(first, second)
        ));

        assertEquals(
                "first chunk from fallback\nsecond chunk from fallback\n",
                Files.readString(source.resolve("fallback.bin"), StandardCharsets.UTF_8)
        );
        assertTrue(Files.notExists(source.resolve("fallback.bin.tmp")));
        assertTrue(Files.notExists(source.resolve("fallback.bin.tmp.cxnfs-state")));
        assertTrue(Files.notExists(chunkDirectory));
        assertEquals(0, repository.containerCount());
        assertEquals(0, repository.chunkCount());
    }

    @Test
    void periodicRefreshDoesNotBlockForegroundReads() throws Exception {
        Path source = tempDir.resolve("background-refresh-share");
        Files.createDirectories(source);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        List<ServedRoot> roots = List.of(new ServedRoot("/share", source));
        CountingChunker chunker = new CountingChunker();
        LiveDirectoryRefresher refresher = new LiveDirectoryRefresher(roots, chunker, Duration.ofMillis(1));
        refresher.refreshNow();

        Files.writeString(source.resolve("later.txt"), "later", StandardCharsets.UTF_8);
        Thread.sleep(5);

        long start = System.nanoTime();
        refresher.refreshIfDue();
        long elapsedNanos = System.nanoTime() - start;

        assertTrue(elapsedNanos < Duration.ofMillis(100).toNanos(), "refreshIfDue blocked foreground path");
        waitForVisible(refresher, "/share/later.txt");
        assertEquals(0, chunker.calls());
    }

    private CxnFileServerConfig config(List<ServedRoot> roots) {
        Path keyStore = tempDir.resolve("server.p12");
        return new CxnFileServerConfig(
                roots,
                AuthenticationPolicy.requireUsernamePassword(Map.of("trainer", "secret")),
                new TransportBinding(
                        TransportRole.FILE_SERVER_CONTROL,
                        new InetSocketAddress("127.0.0.1", 0),
                        TransportSecurityConfig.tls13ForTcp(keyStore)
                ),
                new TransportBinding(
                        TransportRole.CONTAINER_TRANSFER,
                        new InetSocketAddress("127.0.0.1", 0),
                        TransportSecurityConfig.quicTls13ForUdp(keyStore)
                ),
                PayloadStagingConfig.serverDefault()
        );
    }

    private static void publish(CxnFileServer server, String path, byte[] data) throws Exception {
        List<ChunkRef> chunks = List.of();
        List<ContentHash> chunkHashes = List.of();
        List<Integer> chunkSizes = List.of();
        if (data.length > 0) {
            ContentHash chunkHash = Hashing.chunk(data);
            chunks = List.of(new ChunkRef(chunkHash, data.length));
            chunkHashes = List.of(chunkHash);
            chunkSizes = List.of(data.length);
            server.putVerifiedChunk(chunkHash, 0, data);
        }
        ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
        server.publishManifest(new FileManifest(
                path,
                fileHash,
                data.length,
                chunks
        ));
    }

    private static void waitForVisible(LiveDirectoryRefresher refresher, String path) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (refresher.stat(path).isPresent()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for refreshed file: " + path);
    }

    private static void waitForPath(Path path) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(path)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for path: " + path);
    }

    private static TransferChecksum crc32c(byte[]... parts) {
        CRC32C checksum = new CRC32C();
        for (byte[] part : parts) {
            checksum.update(part, 0, part.length);
        }
        return TransferChecksum.crc32c(checksum.getValue());
    }

    private static final class CountingChunker implements Chunker {
        private final Chunker delegate = GearHashChunker.defaults();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public List<Chunk> split(byte[] data) {
            calls.incrementAndGet();
            return delegate.split(data);
        }

        private int calls() {
            return calls.get();
        }
    }
}
