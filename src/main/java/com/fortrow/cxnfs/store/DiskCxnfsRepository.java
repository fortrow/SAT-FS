package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.Chunker;
import com.fortrow.cxnfs.codec.CompressionCodec;
import com.fortrow.cxnfs.codec.CompressionCodecRegistry;
import com.fortrow.cxnfs.codec.IdentityCompressionCodec;
import com.fortrow.cxnfs.container.Container;
import com.fortrow.cxnfs.container.ContainerBuilder;
import com.fortrow.cxnfs.container.StoredChunk;
import com.fortrow.cxnfs.format.ContainerCodec;
import com.fortrow.cxnfs.format.ManifestCodec;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.io.WriteMode;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.UUID;
import java.util.stream.Stream;

public final class DiskCxnfsRepository implements CxnfsStore {
    private static final String CONTAINER_SUFFIX = ".cxnfc";
    private static final String MANIFEST_SUFFIX = ".cxnfm";
    private static final String JOURNAL_SUFFIX = ".cxnfj";

    private final Path root;
    private final Path containersRoot;
    private final Path manifestsRoot;
    private final Path journalsRoot;
    private final Path tempRoot;
    private final CompressionCodec codec;
    private final CompressionCodecRegistry codecRegistry;
    private final ContainerBuilder containerBuilder;
    private final CxnfsRepository delegate;
    private final MutationFaults mutationFaults;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public DiskCxnfsRepository(Path root, Chunker chunker) throws IOException {
        this(root, chunker, new IdentityCompressionCodec());
    }

    public DiskCxnfsRepository(Path root, Chunker chunker, CompressionCodec codec) throws IOException {
        this(root, chunker, codec, CompressionCodecRegistry.defaults());
    }

    DiskCxnfsRepository(
            Path root,
            Chunker chunker,
            CompressionCodec codec,
            CompressionCodecRegistry codecRegistry
    ) throws IOException {
        this(root, chunker, codec, codecRegistry, MutationFaults.NONE);
    }

    DiskCxnfsRepository(
            Path root,
            Chunker chunker,
            CompressionCodec codec,
            CompressionCodecRegistry codecRegistry,
            MutationFaults mutationFaults
    ) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("root is required");
        }
        if (chunker == null) {
            throw new IllegalArgumentException("chunker is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        if (codecRegistry == null) {
            throw new IllegalArgumentException("codecRegistry is required");
        }
        if (mutationFaults == null) {
            throw new IllegalArgumentException("mutationFaults is required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.containersRoot = this.root.resolve("containers");
        this.manifestsRoot = this.root.resolve("manifests");
        this.journalsRoot = this.root.resolve("journals");
        this.tempRoot = this.root.resolve("tmp");
        this.codec = codec;
        this.codecRegistry = codecRegistry;
        this.containerBuilder = new ContainerBuilder(codec);
        this.delegate = new CxnfsRepository(chunker, codec, codecRegistry);
        this.mutationFaults = mutationFaults;
        Files.createDirectories(containersRoot);
        Files.createDirectories(manifestsRoot);
        Files.createDirectories(journalsRoot);
        Files.createDirectories(tempRoot);
        replayJournals();
        loadExistingState();
    }

    public Path root() {
        return root;
    }

    public static DiskRepositoryMaintenanceResult compact(Path root) throws IOException {
        return compact(root, DiskRepositoryMaintenancePolicy.IMMEDIATE);
    }

    public static DiskRepositoryMaintenanceResult compact(
            Path root,
            DiskRepositoryMaintenancePolicy policy
    ) throws IOException {
        return maintain(root, false, policy, Clock.systemUTC());
    }

    public static DiskRepositoryMaintenanceResult inspect(Path root) throws IOException {
        return inspect(root, DiskRepositoryMaintenancePolicy.IMMEDIATE);
    }

    public static DiskRepositoryMaintenanceResult inspect(
            Path root,
            DiskRepositoryMaintenancePolicy policy
    ) throws IOException {
        return maintain(root, true, policy, Clock.systemUTC());
    }

    public static DiskRepositoryAuditResult audit(Path root) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("root is required");
        }
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path containersRoot = normalizedRoot.resolve("containers");
        Path manifestsRoot = normalizedRoot.resolve("manifests");
        Path journalsRoot = normalizedRoot.resolve("journals");
        Path tempRoot = normalizedRoot.resolve("tmp");
        Files.createDirectories(containersRoot);
        Files.createDirectories(manifestsRoot);
        Files.createDirectories(journalsRoot);
        Files.createDirectories(tempRoot);

        AuditAccumulator audit = new AuditAccumulator(normalizedRoot);
        Map<ContentHash, Boolean> containerReferencedByHash = new LinkedHashMap<>();
        Map<String, FileManifest> manifestsByPath = new LinkedHashMap<>();

        for (Path manifestPath : listFiles(manifestsRoot, MANIFEST_SUFFIX)) {
            audit.manifestFiles++;
            try {
                FileManifest manifest = ManifestCodec.decode(Files.readAllBytes(manifestPath));
                String path = normalizeNamespacePath(manifest.path());
                if (manifestsByPath.put(path, manifest) != null) {
                    audit.namespaceConflicts++;
                }
                if (!manifestPath(manifestsRoot, path).equals(manifestPath)) {
                    audit.invalidManifestFiles++;
                }
                if (!validManifest(manifest)) {
                    audit.invalidManifestFiles++;
                }
                for (ChunkRef chunk : manifest.chunks()) {
                    audit.referencedChunks.add(chunk.hash());
                }
            } catch (RuntimeException | IOException e) {
                audit.corruptManifestFiles++;
            }
        }
        audit.namespaceConflicts += namespaceConflictCount(manifestsByPath.keySet());

        for (Path containerPath : listFiles(containersRoot, CONTAINER_SUFFIX)) {
            audit.containerFiles++;
            try {
                byte[] bytes = Files.readAllBytes(containerPath);
                Container container = ContainerCodec.decode(bytes);
                if (!validContainerPath(containerPath, container)) {
                    audit.containerNameMismatches++;
                }
                boolean referenced = false;
                for (StoredChunk chunk : container.chunks()) {
                    byte[] data = audit.codecRegistry.require(chunk.compressionType())
                            .decompress(chunk.payload(), chunk.originalSize());
                    if (data.length != chunk.originalSize()) {
                        throw new IOException("container chunk decompression size mismatch");
                    }
                    if (!Hashing.chunk(data).equals(chunk.chunkHash())) {
                        throw new IOException("container chunk hash mismatch");
                    }
                    audit.storedChunks.add(chunk.chunkHash());
                    referenced = referenced || audit.referencedChunks.contains(chunk.chunkHash());
                }
                containerReferencedByHash.put(container.hash(), referenced);
            } catch (RuntimeException | IOException e) {
                audit.corruptContainerFiles++;
            }
        }

        for (Path journalPath : listFiles(journalsRoot, JOURNAL_SUFFIX)) {
            audit.journalFiles++;
            try {
                DiskRepositoryJournalEntry entry = DiskRepositoryJournalCodec.decode(Files.readAllBytes(journalPath));
                if (journalEntryReady(entry, containersRoot)) {
                    audit.replayableJournalFiles++;
                } else {
                    audit.incompleteJournalFiles++;
                }
            } catch (RuntimeException | IOException e) {
                audit.corruptJournalFiles++;
            }
        }

        audit.tempFiles = listFiles(tempRoot, ".tmp").size();
        audit.missingChunks = (int) audit.referencedChunks.stream()
                .filter(chunk -> !audit.storedChunks.contains(chunk))
                .count();
        audit.orphanContainers = (int) containerReferencedByHash.values().stream()
                .filter(referenced -> !referenced)
                .count();
        return audit.result();
    }

    private static DiskRepositoryMaintenanceResult maintain(
            Path root,
            boolean dryRun,
            DiskRepositoryMaintenancePolicy policy,
            Clock clock
    ) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("root is required");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path containersRoot = normalizedRoot.resolve("containers");
        Path manifestsRoot = normalizedRoot.resolve("manifests");
        Path journalsRoot = normalizedRoot.resolve("journals");
        Path tempRoot = normalizedRoot.resolve("tmp");
        Files.createDirectories(containersRoot);
        Files.createDirectories(manifestsRoot);
        Files.createDirectories(journalsRoot);
        Files.createDirectories(tempRoot);
        List<Path> journalFiles = listFiles(journalsRoot, JOURNAL_SUFFIX);
        if (!journalFiles.isEmpty()) {
            throw new IOException("repository has pending journals; start the CXNFS service to replay them before maintenance");
        }

        Set<ContentHash> referencedChunks = referencedChunksFromManifestFiles(manifestsRoot);
        Instant now = clock.instant();
        int scannedContainers = 0;
        int removedContainers = 0;
        long removedContainerBytes = 0;
        int retainedYoungContainers = 0;
        for (Path containerPath : listFiles(containersRoot, CONTAINER_SUFFIX)) {
            scannedContainers++;
            Container container = ContainerCodec.decode(Files.readAllBytes(containerPath));
            boolean referenced = container.chunks().stream()
                    .map(StoredChunk::chunkHash)
                    .anyMatch(referencedChunks::contains);
            if (!referenced) {
                if (!oldEnoughForMaintenance(containerPath, policy.orphanContainerMinAge(), now)) {
                    retainedYoungContainers++;
                    continue;
                }
                long bytes = Files.size(containerPath);
                if (!dryRun) {
                    Files.delete(containerPath);
                }
                removedContainers++;
                removedContainerBytes += bytes;
            }
        }
        if (!dryRun && removedContainers > 0) {
            forceDirectory(containersRoot);
        }

        TempCleanupResult tempCleanup = cleanupTempFiles(tempRoot, dryRun, policy.tempFileMinAge(), now);
        return new DiskRepositoryMaintenanceResult(
                normalizedRoot,
                dryRun,
                listFiles(manifestsRoot, MANIFEST_SUFFIX).size(),
                referencedChunks.size(),
                scannedContainers,
                removedContainers,
                removedContainerBytes,
                retainedYoungContainers,
                policy.orphanContainerMinAge().toMillis(),
                tempCleanup.filesRemoved(),
                tempCleanup.bytesRemoved(),
                tempCleanup.retainedYoungFiles(),
                policy.tempFileMinAge().toMillis()
        );
    }

    public DiskRepositoryMaintenanceResult compact() throws IOException {
        return writeLockedIo(() -> compact(root));
    }

    public DiskRepositoryMaintenanceResult compact(DiskRepositoryMaintenancePolicy policy) throws IOException {
        return writeLockedIo(() -> compact(root, policy));
    }

    public DiskRepositoryMaintenanceResult inspect() throws IOException {
        return readLockedIo(() -> inspect(root));
    }

    public DiskRepositoryMaintenanceResult inspect(DiskRepositoryMaintenancePolicy policy) throws IOException {
        return readLockedIo(() -> inspect(root, policy));
    }

    public DiskRepositoryAuditResult audit() throws IOException {
        return readLockedIo(() -> audit(root));
    }

    @Override
    public FileManifest put(String path, byte[] data) throws IOException {
        return writeLockedIo(() -> {
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            FileManifest manifest = delegate.put(path, data);
            persistChunks(manifest, data);
            persistManifestMutation(new DiskRepositoryJournalEntry(List.of(), List.of(manifest)));
            return manifest;
        });
    }

    @Override
    public FileManifest putStream(String path, InputStream data) throws IOException {
        return writeLockedIo(() -> {
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            Path temp = tempRoot.resolve("stream-put-" + UUID.randomUUID() + ".tmp");
            boolean deleteTemp = true;
            try {
                Files.copy(data, temp);
                FileManifest manifest;
                try (InputStream input = Files.newInputStream(temp)) {
                    manifest = delegate.putStream(path, input);
                }
                try (InputStream input = Files.newInputStream(temp)) {
                    persistChunks(manifest, input);
                }
                persistManifestMutation(new DiskRepositoryJournalEntry(List.of(), List.of(manifest)));
                return manifest;
            } finally {
                if (deleteTemp) {
                    Files.deleteIfExists(temp);
                }
            }
        });
    }

    @Override
    public FileManifest commit(StagedWrite stagedWrite) throws IOException {
        return writeLockedIo(() -> {
            if (stagedWrite == null) {
                throw new IllegalArgumentException("stagedWrite is required");
            }
            hydrateCurrentManifestForWrite(stagedWrite);
            FileManifest manifest = delegate.commit(stagedWrite);
            persistMissingChunks(manifest);
            persistManifestMutation(new DiskRepositoryJournalEntry(List.of(), List.of(manifest)));
            return manifest;
        });
    }

    @Override
    public void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
        writeLockedIo(() -> {
            delegate.putVerifiedChunk(chunkHash, sourceOffset, data);
            persistChunk(chunkHash, sourceOffset, data);
        });
    }

    @Override
    public FileManifest publishManifest(FileManifest manifest) throws IOException {
        return writeLockedIo(() -> {
            ensureManifestChunksLoaded(manifest);
            FileManifest published = delegate.publishManifest(manifest);
            persistManifestMutation(new DiskRepositoryJournalEntry(List.of(), List.of(published)));
            return published;
        });
    }

    @Override
    public PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        return writeLockedIo(() -> {
            String normalizedPath = normalizeNamespacePath(path);
            Map<String, Path> manifestFiles = manifestFilesMatching(normalizedPath);
            PathDeleteResult result = delegate.deletePath(normalizedPath, recursive);
            persistManifestMutation(new DiskRepositoryJournalEntry(List.copyOf(manifestFiles.keySet()), List.of()));
            return result;
        });
    }

    @Override
    public PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        return writeLockedIo(() -> {
            String normalizedSource = normalizeNamespacePath(sourcePath);
            String normalizedTarget = normalizeNamespacePath(targetPath);
            if (normalizedSource.equals(normalizedTarget)) {
                return delegate.renamePath(normalizedSource, normalizedTarget);
            }

            Map<String, Path> sourceManifestFiles = manifestFilesMatching(normalizedSource);
            PathRenameResult result = delegate.renamePath(normalizedSource, normalizedTarget);
            persistManifestMutation(new DiskRepositoryJournalEntry(
                    List.copyOf(sourceManifestFiles.keySet()),
                    result.updatedManifests()
            ));
            return result;
        });
    }

    @Override
    public void pruneExcept(Set<String> retainedPaths) throws IOException {
        writeLockedIo(() -> {
            if (retainedPaths == null) {
                throw new IllegalArgumentException("retainedPaths is required");
            }
            Set<String> normalizedRetainedPaths = retainedPaths.stream()
                    .map(DiskCxnfsRepository::normalizeNamespacePath)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
            List<String> stalePaths = new java.util.ArrayList<>();
            for (Path manifestPath : listFiles(manifestsRoot, MANIFEST_SUFFIX)) {
                FileManifest manifest = ManifestCodec.decode(Files.readAllBytes(manifestPath));
                if (!normalizedRetainedPaths.contains(normalizeNamespacePath(manifest.path()))) {
                    stalePaths.add(normalizeNamespacePath(manifest.path()));
                }
            }
            persistManifestMutation(new DiskRepositoryJournalEntry(stalePaths, List.of()));
            delegate.pruneExcept(normalizedRetainedPaths);
        });
    }

    @Override
    public DirectoryListing list(String path) {
        return delegate.list(path);
    }

    @Override
    public Optional<FileMetadata> stat(String path) {
        return delegate.stat(path);
    }

    @Override
    public Optional<FileManifest> manifest(ContentHash fileHash) {
        return delegate.manifest(fileHash);
    }

    @Override
    public Optional<FileManifest> manifest(String path) {
        return delegate.manifest(path);
    }

    @Override
    public List<ContentHash> missingChunks(List<ChunkRef> chunks) {
        if (chunks == null) {
            throw new IllegalArgumentException("chunks are required");
        }
        Set<ContentHash> missing = new LinkedHashSet<>();
        for (ChunkRef chunk : chunks) {
            if (!delegate.containsChunk(chunk.hash()) && !Files.exists(chunkPath(chunk.hash()))) {
                missing.add(chunk.hash());
            }
        }
        return List.copyOf(missing);
    }

    @Override
    public ReconstructionPlan plan(ContentHash fileHash) {
        try {
            ensureManifestChunksLoaded(fileHash);
            return delegate.plan(fileHash);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load durable chunks for reconstruction plan: " + fileHash, e);
        }
    }

    @Override
    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        return delegate.readPlan(fileHash, offset, length);
    }

    @Override
    public byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        ReadPlan plan = delegate.readPlan(fileHash, offset, length);
        ensurePlanChunksLoaded(plan);
        return delegate.readRange(fileHash, offset, length);
    }

    @Override
    public byte[] reconstruct(ContentHash fileHash) throws IOException {
        ensureManifestChunksLoaded(fileHash);
        return delegate.reconstruct(fileHash);
    }

    public int containerCount() {
        return delegate.containerCount();
    }

    public int chunkCount() {
        return delegate.chunkCount();
    }

    private void loadExistingState() throws IOException {
        for (Path manifestPath : listFiles(manifestsRoot, MANIFEST_SUFFIX)) {
            delegate.publishManifestUnchecked(ManifestCodec.decode(Files.readAllBytes(manifestPath)));
        }
    }

    private void replayJournals() throws IOException {
        for (Path journalPath : listFiles(journalsRoot, JOURNAL_SUFFIX)) {
            DiskRepositoryJournalEntry entry;
            try {
                entry = DiskRepositoryJournalCodec.decode(Files.readAllBytes(journalPath));
            } catch (RuntimeException | IOException e) {
                deleteIfExistsAndForce(journalPath);
                continue;
            }
            if (journalEntryReady(entry, containersRoot)) {
                applyManifestMutation(entry);
            }
            deleteIfExistsAndForce(journalPath);
        }
    }

    private static Set<ContentHash> referencedChunksFromManifestFiles(Path manifestsRoot) throws IOException {
        Set<ContentHash> referenced = new LinkedHashSet<>();
        for (Path manifestPath : listFiles(manifestsRoot, MANIFEST_SUFFIX)) {
            FileManifest manifest = ManifestCodec.decode(Files.readAllBytes(manifestPath));
            for (ChunkRef chunk : manifest.chunks()) {
                referenced.add(chunk.hash());
            }
        }
        return referenced;
    }

    private static TempCleanupResult cleanupTempFiles(
            Path tempRoot,
            boolean dryRun,
            Duration minAge,
            Instant now
    ) throws IOException {
        int filesRemoved = 0;
        long bytesRemoved = 0;
        int retainedYoungFiles = 0;
        for (Path tempPath : listFiles(tempRoot, ".tmp")) {
            if (!oldEnoughForMaintenance(tempPath, minAge, now)) {
                retainedYoungFiles++;
                continue;
            }
            long bytes = Files.size(tempPath);
            if (!dryRun) {
                Files.delete(tempPath);
            }
            filesRemoved++;
            bytesRemoved += bytes;
        }
        if (!dryRun && filesRemoved > 0) {
            forceDirectory(tempRoot);
        }
        return new TempCleanupResult(filesRemoved, bytesRemoved, retainedYoungFiles);
    }

    private static boolean oldEnoughForMaintenance(Path path, Duration minAge, Instant now) throws IOException {
        if (minAge.isZero()) {
            return true;
        }
        Instant modifiedAt = Files.getLastModifiedTime(path).toInstant();
        return !modifiedAt.isAfter(now.minus(minAge));
    }

    private void loadContainer(Path path) throws IOException {
        Container container = ContainerCodec.decode(Files.readAllBytes(path));
        for (StoredChunk chunk : container.chunks()) {
            byte[] data = codecRegistry.require(chunk.compressionType())
                    .decompress(chunk.payload(), chunk.originalSize());
            if (!Hashing.chunk(data).equals(chunk.chunkHash())) {
                throw new IOException("stored container chunk hash verification failed: " + path);
            }
            delegate.putVerifiedChunk(chunk.chunkHash(), 0, data);
        }
    }

    private void hydrateCurrentManifestForWrite(StagedWrite stagedWrite) throws IOException {
        if (stagedWrite.mode() != WriteMode.COPY_ON_WRITE) {
            return;
        }
        Optional<FileManifest> current = delegate.manifest(stagedWrite.path());
        if (current.isPresent()) {
            ensureManifestChunksLoaded(current.orElseThrow());
        }
    }

    private void ensureManifestChunksLoaded(ContentHash fileHash) throws IOException {
        FileManifest manifest = delegate.manifest(fileHash)
                .orElseThrow(() -> new IllegalArgumentException("unknown file hash: " + fileHash));
        ensureManifestChunksLoaded(manifest);
    }

    private void ensureManifestChunksLoaded(FileManifest manifest) throws IOException {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        for (ChunkRef chunk : manifest.chunks()) {
            ensureChunkLoaded(chunk.hash());
        }
    }

    private void ensurePlanChunksLoaded(ReadPlan plan) throws IOException {
        for (var chunk : plan.chunks()) {
            ensureChunkLoaded(chunk.chunkHash());
        }
    }

    private void ensureChunkLoaded(ContentHash chunkHash) throws IOException {
        if (delegate.containsChunk(chunkHash)) {
            return;
        }
        Path path = chunkPath(chunkHash);
        if (!Files.exists(path)) {
            throw new IOException("durable chunk container is missing: " + chunkHash);
        }
        loadContainer(path);
        if (!delegate.containsChunk(chunkHash)) {
            throw new IOException("durable container did not contain requested chunk: " + chunkHash);
        }
    }

    private void persistChunks(FileManifest manifest, byte[] data) throws IOException {
        long offset = 0;
        for (ChunkRef chunk : manifest.chunks()) {
            int size = chunk.size();
            if (offset + size > data.length) {
                throw new IOException("manifest chunk range exceeds source data for " + manifest.path());
            }
            byte[] chunkData = Arrays.copyOfRange(data, Math.toIntExact(offset), Math.toIntExact(offset + size));
            persistChunk(chunk.hash(), offset, chunkData);
            offset += size;
        }
        if (offset != data.length) {
            throw new IOException("manifest chunks do not cover source data for " + manifest.path());
        }
    }

    private void persistChunks(FileManifest manifest, InputStream data) throws IOException {
        long offset = 0;
        for (ChunkRef chunk : manifest.chunks()) {
            byte[] chunkData = data.readNBytes(chunk.size());
            if (chunkData.length != chunk.size()) {
                throw new IOException("manifest chunk range exceeds source data for " + manifest.path());
            }
            persistChunk(chunk.hash(), offset, chunkData);
            offset += chunk.size();
        }
        if (offset != manifest.size()) {
            throw new IOException("manifest chunks do not cover source data for " + manifest.path());
        }
        if (data.read() != -1) {
            throw new IOException("source data exceeds manifest chunks for " + manifest.path());
        }
    }

    private void persistMissingChunks(FileManifest manifest) throws IOException {
        long offset = 0;
        for (ChunkRef chunk : manifest.chunks()) {
            if (!Files.exists(chunkPath(chunk.hash()))) {
                persistChunk(chunk.hash(), offset, delegate.readRange(manifest.fileHash(), offset, chunk.size()));
            }
            offset += chunk.size();
        }
        if (offset != manifest.size()) {
            throw new IOException("manifest chunks do not cover staged write for " + manifest.path());
        }
    }

    private void persistManifestMutation(DiskRepositoryJournalEntry entry) throws IOException {
        if (entry.empty()) {
            return;
        }
        Path journalPath = journalsRoot.resolve("journal-" + UUID.randomUUID() + JOURNAL_SUFFIX);
        writeAtomically(journalPath, DiskRepositoryJournalCodec.encode(entry));
        mutationFaults.afterJournalWrite(journalPath, entry);
        applyManifestMutation(entry);
        deleteIfExistsAndForce(journalPath);
    }

    private void applyManifestMutation(DiskRepositoryJournalEntry entry) throws IOException {
        boolean deletedManifest = false;
        for (String path : entry.deletePaths()) {
            deletedManifest = Files.deleteIfExists(manifestPath(path)) || deletedManifest;
        }
        if (deletedManifest) {
            forceDirectory(manifestsRoot);
            mutationFaults.afterManifestDeletes(entry);
        }
        int manifestIndex = 0;
        int manifestCount = entry.putManifests().size();
        for (FileManifest manifest : entry.putManifests()) {
            persistManifest(manifest);
            manifestIndex++;
            mutationFaults.afterManifestPut(manifest, manifestIndex, manifestCount);
        }
    }

    private static boolean journalEntryReady(DiskRepositoryJournalEntry entry, Path containersRoot) {
        for (FileManifest manifest : entry.putManifests()) {
            if (!validManifest(manifest)) {
                return false;
            }
            for (ChunkRef chunk : manifest.chunks()) {
                if (!Files.exists(chunkPath(containersRoot, chunk.hash()))) {
                    return false;
                }
            }
        }
        return true;
    }

    private void persistChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
        if (chunkHash == null) {
            throw new IllegalArgumentException("chunkHash is required");
        }
        if (sourceOffset < 0) {
            throw new IllegalArgumentException("sourceOffset must be non-negative");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is required");
        }
        if (!Hashing.chunk(data).equals(chunkHash)) {
            throw new IOException("durable chunk hash does not match payload");
        }
        Path path = chunkPath(chunkHash);
        if (Files.exists(path)) {
            return;
        }
        Container container = containerBuilder.build(List.of(new Chunk(chunkHash, sourceOffset, data)));
        writeAtomically(path, ContainerCodec.encode(container));
    }

    private void persistManifest(FileManifest manifest) throws IOException {
        writeAtomically(manifestPath(manifest.path()), ManifestCodec.encode(manifest));
    }

    private Map<String, Path> manifestFilesMatching(String sourcePath) throws IOException {
        Map<String, Path> exact = new LinkedHashMap<>();
        Map<String, Path> children = new LinkedHashMap<>();
        for (Path manifestPath : listFiles(manifestsRoot, MANIFEST_SUFFIX)) {
            FileManifest manifest = ManifestCodec.decode(Files.readAllBytes(manifestPath));
            String manifestNamespacePath = normalizeNamespacePath(manifest.path());
            if (manifestNamespacePath.equals(sourcePath)) {
                exact.put(manifestNamespacePath, manifestPath);
            } else if (isPathUnder(sourcePath, manifestNamespacePath)) {
                children.put(manifestNamespacePath, manifestPath);
            }
        }
        return exact.isEmpty() ? children : exact;
    }

    private Path chunkPath(ContentHash chunkHash) {
        return chunkPath(containersRoot, chunkHash);
    }

    private static Path chunkPath(Path containersRoot, ContentHash chunkHash) {
        return containersRoot.resolve(chunkHash.hex() + CONTAINER_SUFFIX);
    }

    private Path manifestPath(String path) {
        String normalizedPath = normalizeNamespacePath(path);
        ContentHash pathHash = Hashing.chunk(normalizedPath.getBytes(StandardCharsets.UTF_8));
        return manifestsRoot.resolve(pathHash.hex() + MANIFEST_SUFFIX);
    }

    private static Path manifestPath(Path manifestsRoot, String path) {
        String normalizedPath = normalizeNamespacePath(path);
        ContentHash pathHash = Hashing.chunk(normalizedPath.getBytes(StandardCharsets.UTF_8));
        return manifestsRoot.resolve(pathHash.hex() + MANIFEST_SUFFIX);
    }

    private void writeAtomically(Path target, byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }
        Files.createDirectories(target.getParent());
        Files.createDirectories(tempRoot);
        Path temp = tempRoot.resolve(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        boolean moved = false;
        try {
            writeAndForce(temp, bytes);
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            moved = true;
            forceDirectory(temp.getParent());
            forceDirectory(target.getParent());
        } finally {
            if (!moved) {
                if (Files.deleteIfExists(temp)) {
                    forceDirectory(temp.getParent());
                }
            }
        }
    }

    private static void writeAndForce(Path target, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    private static boolean deleteIfExistsAndForce(Path path) throws IOException {
        boolean deleted = Files.deleteIfExists(path);
        if (deleted) {
            forceDirectory(path.getParent());
        }
        return deleted;
    }

    private static void forceDirectory(Path directory) throws IOException {
        if (directory == null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (UnsupportedOperationException e) {
            // Some non-Linux providers cannot open directories as channels.
        } catch (IOException e) {
            if (directoryForceUnsupported(e)) {
                return;
            }
            throw e;
        }
    }

    private static boolean directoryForceUnsupported(IOException e) {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (osName.contains("windows")) {
            return true;
        }
        String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
        return message.contains("is a directory")
                || message.contains("operation not supported")
                || message.contains("invalid argument");
    }

    private static List<Path> listFiles(Path directory, String suffix) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(suffix))
                    .sorted(Comparator.naturalOrder())
                    .toList();
        }
    }

    private static String normalizeNamespacePath(String path) {
        return NamespacePath.normalize(path);
    }

    private static boolean isPathUnder(String directory, String candidate) {
        return directory.equals("/")
                ? candidate.length() > 1
                : candidate.startsWith(directory + "/");
    }

    private static boolean validManifest(FileManifest manifest) {
        ContentHash actualFileHash = Hashing.file(
                manifest.chunks().stream().map(ChunkRef::hash).toList(),
                manifest.chunks().stream().map(ChunkRef::size).toList()
        );
        long actualSize = manifest.chunks().stream().mapToLong(ChunkRef::size).sum();
        return actualFileHash.equals(manifest.fileHash()) && actualSize == manifest.size();
    }

    private static boolean validContainerPath(Path path, Container container) {
        String fileName = path.getFileName().toString();
        if (fileName.equals(container.hash().hex() + CONTAINER_SUFFIX)) {
            return true;
        }
        return container.chunks().stream()
                .map(StoredChunk::chunkHash)
                .map(ContentHash::hex)
                .anyMatch(hash -> fileName.equals(hash + CONTAINER_SUFFIX));
    }

    private static int namespaceConflictCount(Set<String> paths) {
        int conflicts = 0;
        List<String> sortedPaths = paths.stream().sorted().toList();
        for (int i = 0; i < sortedPaths.size(); i++) {
            String path = sortedPaths.get(i);
            for (int j = i + 1; j < sortedPaths.size(); j++) {
                String candidate = sortedPaths.get(j);
                if (!candidate.startsWith(path + "/")) {
                    break;
                }
                conflicts++;
            }
        }
        return conflicts;
    }

    private <T> T writeLockedIo(IoSupplier<T> supplier) throws IOException {
        lock.writeLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private <T> T readLockedIo(IoSupplier<T> supplier) throws IOException {
        lock.readLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeLockedIo(IoRunnable runnable) throws IOException {
        lock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private record TempCleanupResult(int filesRemoved, long bytesRemoved, int retainedYoungFiles) {
    }

    interface MutationFaults {
        MutationFaults NONE = new MutationFaults() {
        };

        default void afterJournalWrite(Path journalPath, DiskRepositoryJournalEntry entry) throws IOException {
        }

        default void afterManifestDeletes(DiskRepositoryJournalEntry entry) throws IOException {
        }

        default void afterManifestPut(FileManifest manifest, int manifestIndex, int manifestCount) throws IOException {
        }
    }

    private static final class AuditAccumulator {
        private final Path root;
        private final CompressionCodecRegistry codecRegistry = CompressionCodecRegistry.defaults();
        private final Set<ContentHash> referencedChunks = new LinkedHashSet<>();
        private final Set<ContentHash> storedChunks = new LinkedHashSet<>();
        private int manifestFiles;
        private int containerFiles;
        private int journalFiles;
        private int tempFiles;
        private int corruptManifestFiles;
        private int corruptContainerFiles;
        private int corruptJournalFiles;
        private int invalidManifestFiles;
        private int replayableJournalFiles;
        private int incompleteJournalFiles;
        private int containerNameMismatches;
        private int missingChunks;
        private int orphanContainers;
        private int namespaceConflicts;

        private AuditAccumulator(Path root) {
            this.root = root;
        }

        private DiskRepositoryAuditResult result() {
            return new DiskRepositoryAuditResult(
                    root,
                    manifestFiles,
                    containerFiles,
                    journalFiles,
                    tempFiles,
                    referencedChunks.size(),
                    storedChunks.size(),
                    corruptManifestFiles,
                    corruptContainerFiles,
                    corruptJournalFiles,
                    invalidManifestFiles,
                    replayableJournalFiles,
                    incompleteJournalFiles,
                    containerNameMismatches,
                    missingChunks,
                    orphanContainers,
                    namespaceConflicts
            );
        }
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }

    @FunctionalInterface
    private interface IoRunnable {
        void run() throws IOException;
    }
}
