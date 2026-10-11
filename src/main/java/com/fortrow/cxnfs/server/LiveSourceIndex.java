package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.Chunker;
import com.fortrow.cxnfs.checksum.TransferChecksums;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.ReadPlanner;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferChecksum;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.store.CxnfsStore;
import com.fortrow.cxnfs.store.PathDeleteResult;
import com.fortrow.cxnfs.store.PathRenameResult;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.StagedPayload;
import com.fortrow.cxnfs.transport.StagedPayloadStorage;
import com.fortrow.cxnfs.transport.StreamingVerifiedEntryPublisher;
import com.fortrow.cxnfs.transport.VerifiedEntry;
import com.fortrow.cxnfs.transport.VerifiedEntryPublisher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class LiveSourceIndex {
    private static final int WRITER_QUEUE_DEPTH = 128;
    private static final long WRITER_STATS_MIN_BYTES = 64L * 1024L * 1024L;
    private static final long MAX_PENDING_CHECKSUM_PAYLOAD_BYTES = 512L * 1024L * 1024L;
    private static final boolean FORCE_LIVE_WRITES_ON_COMMIT = forceLiveWritesOnCommit();

    private final List<ServedRoot> roots;
    private final Chunker chunker;
    private final LiveWriteMode liveWriteMode;
    private final ReadPlanner readPlanner = new ReadPlanner();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, SourceFile> filesByPath = new LinkedHashMap<>();
    private final Map<ContentHash, LinkedHashSet<String>> pathsByHash = new LinkedHashMap<>();

    LiveSourceIndex(List<ServedRoot> roots, Chunker chunker, LiveWriteMode liveWriteMode) {
        if (roots == null || roots.isEmpty()) {
            throw new IllegalArgumentException("roots are required");
        }
        if (chunker == null) {
            throw new IllegalArgumentException("chunker is required");
        }
        if (liveWriteMode == null) {
            throw new IllegalArgumentException("liveWriteMode is required");
        }
        this.roots = List.copyOf(roots);
        this.chunker = chunker;
        this.liveWriteMode = liveWriteMode;
    }

    DirectoryLoadResult refreshNow() throws IOException {
        long files = 0;
        long bytes = 0;
        Set<String> loadedPaths = new LinkedHashSet<>();
        for (ServedRoot root : roots) {
            Path directory = root.localDirectory();
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("directory does not exist: " + directory);
            }
            try (Stream<Path> paths = Files.walk(directory)) {
                for (Path path : paths
                        .filter(candidate -> Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
                        .sorted(Comparator.naturalOrder())
                        .toList()) {
                    String namespacePath = namespacePath(root.remotePrefix(), directory.relativize(path));
                    SourceFileState state = SourceFileState.from(Files.readAttributes(
                            path,
                            BasicFileAttributes.class,
                            LinkOption.NOFOLLOW_LINKS
                    ));
                    boolean changed;
                    lock.readLock().lock();
                    try {
                        SourceFile existing = filesByPath.get(namespacePath);
                        changed = existing == null || !existing.state().equals(state);
                    } finally {
                        lock.readLock().unlock();
                    }
                    if (changed) {
                        SourceFile sourceFile = discoveredSourceFile(namespacePath, path, state);
                        putSourceFile(sourceFile);
                        files++;
                        bytes += state.size();
                    }
                    loadedPaths.add(namespacePath);
                }
            }
        }
        retainOnly(loadedPaths);
        return new DirectoryLoadResult(files, bytes);
    }

    boolean isCovered(String path) {
        String normalizedPath = normalizePath(path);
        for (ServedRoot root : roots) {
            String prefix = root.remotePrefix();
            if (prefix.equals("/")) {
                return true;
            }
            if (normalizedPath.equals(prefix) || normalizedPath.startsWith(prefix + "/")) {
                return true;
            }
            if (prefix.startsWith(normalizedPath.equals("/") ? "/" : normalizedPath + "/")) {
                return true;
            }
        }
        return false;
    }

    Optional<Path> localPathFor(String namespacePath) {
        String normalizedPath = normalizePath(namespacePath);
        for (ServedRoot root : roots) {
            String prefix = root.remotePrefix();
            if (!prefix.equals("/") && !normalizedPath.equals(prefix) && !normalizedPath.startsWith(prefix + "/")) {
                continue;
            }
            String relative = prefix.equals("/")
                    ? normalizedPath.substring(1)
                    : normalizedPath.substring(prefix.length());
            while (relative.startsWith("/")) {
                relative = relative.substring(1);
            }
            Path local = root.localDirectory().resolve(relative).normalize();
            if (!local.startsWith(root.localDirectory())) {
                throw new SecurityException("resolved path escapes served root: " + namespacePath);
            }
            return Optional.of(local);
        }
        return Optional.empty();
    }

    DirectoryListing list(String path) {
        lock.readLock().lock();
        try {
            String normalizedPath = normalizeDirectoryPath(path);
            Set<FileMetadata> entries = new LinkedHashSet<>();
            for (ServedRoot root : roots) {
                directChild(normalizedPath, root.remotePrefix()).ifPresent(entries::add);
            }
            for (SourceFile sourceFile : filesByPath.values()) {
                directChild(normalizedPath, sourceFile).ifPresent(entries::add);
            }
            return new DirectoryListing(normalizedPath, entries.stream()
                    .sorted(Comparator.comparing(FileMetadata::type).thenComparing(FileMetadata::name))
                    .toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    Optional<FileMetadata> stat(String path) {
        lock.readLock().lock();
        try {
            String normalizedPath = normalizePath(path);
            SourceFile sourceFile = filesByPath.get(normalizedPath);
            if (sourceFile != null) {
                return Optional.of(fileMetadata(sourceFile, nameOf(normalizedPath)));
            }
            if (isServedDirectory(normalizedPath)) {
                return Optional.of(directoryMetadata(normalizedPath));
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    Optional<FileManifest> manifest(String path) throws IOException {
        String normalizedPath = normalizePath(path);
        if (!filesByPath.containsKey(normalizedPath)) {
            return Optional.empty();
        }
        return Optional.of(hydrateSourceFile(normalizedPath).manifest());
    }

    Optional<FileManifest> manifest(ContentHash fileHash) {
        lock.readLock().lock();
        try {
            return firstByHash(fileHash).map(SourceFile::manifest);
        } finally {
            lock.readLock().unlock();
        }
    }

    ReadPlan readPlan(String path, long offset, long length) throws IOException {
        SourceFile sourceFile = hydrateSourceFile(path);
        return readPlanner.plan(sourceFile.manifest(), offset, length);
    }

    ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        lock.readLock().lock();
        try {
            SourceFile sourceFile = requireSourceFile(fileHash);
            return readPlanner.plan(sourceFile.manifest(), offset, length);
        } finally {
            lock.readLock().unlock();
        }
    }

    byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        ReadPlan plan = readPlan(fileHash, offset, length);
        if (plan.requestedRange().isEmpty()) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(plan.requestedRange().length()));
        readVerifiedPlanChunks(plan, (chunk, payload) ->
                out.write(payload, chunk.offsetInChunk(), chunk.bytesToRead()));
        return out.toByteArray();
    }

    byte[] reconstruct(ContentHash fileHash) throws IOException {
        SourceFile sourceFile = sourceFileSnapshot(fileHash);
        if (sourceFile.manifest().size() == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(sourceFile.manifest().size()));
        ReadPlan plan = readPlanner.plan(sourceFile.manifest(), 0L, sourceFile.manifest().size());
        readVerifiedPlanChunks(plan, (chunk, payload) -> out.write(payload));
        return out.toByteArray();
    }

    void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer is required");
        }
        SourceFile sourceFile = sourceFileSnapshot(plan.fileHash());
        ReadPlan canonicalPlan = readPlanner.plan(
                sourceFile.manifest(),
                plan.requestedRange().offset(),
                plan.requestedRange().length()
        );
        if (!canonicalPlan.equals(plan)) {
            throw new IllegalArgumentException("read plan does not match live source manifest");
        }
        for (PlannedChunkRead chunk : plan.chunks()) {
            byte[] payload = readSourceChunk(sourceFile, chunk);
            consumer.accept(chunk, payload);
        }
    }

    PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        String normalizedPath = normalizePath(path);
        if (normalizedPath.equals("/")) {
            throw new IllegalStateException("refusing to delete namespace root");
        }
        Path localPath = localPathFor(normalizedPath)
                .orElseThrow(() -> new IllegalArgumentException("path is outside served roots: " + normalizedPath));
        Optional<FileMetadata> metadata = stat(normalizedPath);
        if (metadata.isEmpty()) {
            throw new IllegalArgumentException("unknown path: " + normalizedPath);
        }
        long deletedEntries;
        if (Files.isDirectory(localPath, LinkOption.NOFOLLOW_LINKS)) {
            List<String> children = pathsUnder(normalizedPath);
            if (!recursive && !children.isEmpty()) {
                throw new IllegalStateException("directory delete requires recursive=true: " + normalizedPath);
            }
            deletedEntries = children.size();
            if (recursive) {
                try (Stream<Path> paths = Files.walk(localPath)) {
                    for (Path child : paths.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(child);
                    }
                }
            } else {
                Files.delete(localPath);
            }
            removePaths(children);
        } else {
            Files.deleteIfExists(localPath);
            deletedEntries = filesByPath.containsKey(normalizedPath) ? 1 : 0;
            removePaths(List.of(normalizedPath));
        }
        return new PathDeleteResult(normalizedPath, deletedEntries);
    }

    PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        String normalizedSource = normalizePath(sourcePath);
        String normalizedTarget = normalizePath(targetPath);
        if (normalizedSource.equals("/")) {
            throw new IllegalStateException("refusing to rename namespace root");
        }
        if (normalizedTarget.equals("/")) {
            throw new IllegalStateException("rename target cannot be namespace root");
        }
        Path localSource = localPathFor(normalizedSource)
                .orElseThrow(() -> new IllegalArgumentException("path is outside served roots: " + normalizedSource));
        Path localTarget = localPathFor(normalizedTarget)
                .orElseThrow(() -> new IllegalArgumentException("path is outside served roots: " + normalizedTarget));
        Optional<FileMetadata> sourceMetadata = stat(normalizedSource);
        if (sourceMetadata.isEmpty() || !Files.exists(localSource, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("unknown path: " + normalizedSource);
        }
        if (isPathUnder(normalizedSource, normalizedTarget)) {
            throw new IllegalStateException("cannot rename a directory into itself: " + normalizedSource);
        }
        Path parent = localTarget.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        boolean replacingFile = Files.isRegularFile(localSource, LinkOption.NOFOLLOW_LINKS)
                && Files.isRegularFile(localTarget, LinkOption.NOFOLLOW_LINKS);
        if (replacingFile) {
            Files.move(localSource, localTarget, StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.move(localSource, localTarget);
        }

        List<SourceFile> renamed = renameIndexedFiles(normalizedSource, normalizedTarget, localSource, localTarget);
        FileMetadata metadata = renamed.size() == 1 && renamed.get(0).namespacePath().equals(normalizedTarget)
                ? fileMetadata(renamed.get(0), nameOf(normalizedTarget))
                : directoryMetadata(normalizedTarget);
        return new PathRenameResult(
                normalizedSource,
                normalizedTarget,
                metadata,
                hydratedManifests(renamed)
        );
    }

    VerifiedEntryPublisher threadedPublisher(String namespacePath, long sizeHintBytes) throws IOException {
        Path localPath = localPathFor(namespacePath)
                .orElseThrow(() -> new IllegalArgumentException("path is outside served roots: " + namespacePath));
        return new ThreadedSourceBackedPublisher(normalizePath(namespacePath), localPath, sizeHintBytes);
    }

    private SourceFile discoveredSourceFile(String namespacePath, Path localPath, SourceFileState state) {
        return new SourceFile(
                normalizePath(namespacePath),
                localPath.toAbsolutePath().normalize(),
                null,
                state
        );
    }

    private SourceFile hydratedSourceFile(String namespacePath, Path localPath, SourceFileState state) throws IOException {
        List<ChunkRef> refs = new ArrayList<>();
        List<ContentHash> hashes = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        try (var input = Files.newInputStream(localPath)) {
            chunker.split(input, chunk -> recordChunk(chunk, refs, hashes, sizes));
        }
        ContentHash fileHash = Hashing.file(hashes, sizes);
        FileManifest manifest = new FileManifest(normalizePath(namespacePath), fileHash, state.size(), refs);
        return new SourceFile(normalizePath(namespacePath), localPath.toAbsolutePath().normalize(), manifest, state);
    }

    private static void recordChunk(
            Chunk chunk,
            List<ChunkRef> refs,
            List<ContentHash> hashes,
            List<Integer> sizes
    ) {
        refs.add(new ChunkRef(chunk.hash(), chunk.size()));
        hashes.add(chunk.hash());
        sizes.add(chunk.size());
    }

    private void putSourceFile(SourceFile sourceFile) {
        lock.writeLock().lock();
        try {
            putSourceFileLocked(sourceFile);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void putSourceFileLocked(SourceFile sourceFile) {
        SourceFile previous = filesByPath.put(sourceFile.namespacePath(), sourceFile);
        if (previous != null && previous.manifest() != null) {
            removeHashPath(previous.manifest().fileHash(), previous.namespacePath());
        }
        if (sourceFile.manifest() != null) {
            pathsByHash.computeIfAbsent(sourceFile.manifest().fileHash(), ignored -> new LinkedHashSet<>())
                    .add(sourceFile.namespacePath());
        }
    }

    private void retainOnly(Set<String> namespacePaths) {
        lock.writeLock().lock();
        try {
            List<String> stalePaths = filesByPath.keySet().stream()
                    .filter(path -> !namespacePaths.contains(path))
                    .toList();
            removePathsLocked(stalePaths);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private SourceFile requireSourceFile(String path) {
        SourceFile sourceFile = filesByPath.get(normalizePath(path));
        if (sourceFile == null) {
            throw new IllegalArgumentException("unknown file path: " + path);
        }
        return sourceFile;
    }

    private SourceFile hydrateSourceFile(String path) throws IOException {
        String normalizedPath = normalizePath(path);
        while (true) {
            SourceFile snapshot;
            lock.readLock().lock();
            try {
                snapshot = requireSourceFile(normalizedPath);
                if (snapshot.manifest() != null) {
                    return snapshot;
                }
            } finally {
                lock.readLock().unlock();
            }

            SourceFileState currentState = SourceFileState.from(Files.readAttributes(
                    snapshot.localPath(),
                    BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS
            ));
            SourceFile hydrated = hydratedSourceFile(normalizedPath, snapshot.localPath(), currentState);

            lock.writeLock().lock();
            try {
                SourceFile current = requireSourceFile(normalizedPath);
                if (current.manifest() != null) {
                    return current;
                }
                if (!current.localPath().equals(snapshot.localPath()) || !current.state().equals(currentState)) {
                    continue;
                }
                putSourceFileLocked(hydrated);
                return hydrated;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private SourceFile requireSourceFile(ContentHash fileHash) {
        return firstByHash(fileHash)
                .orElseThrow(() -> new IllegalArgumentException("unknown file hash: " + fileHash));
    }

    private Optional<SourceFile> firstByHash(ContentHash fileHash) {
        LinkedHashSet<String> paths = pathsByHash.get(fileHash);
        if (paths == null || paths.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(filesByPath.get(paths.iterator().next()));
    }

    private SourceFile sourceFileSnapshot(ContentHash fileHash) {
        lock.readLock().lock();
        try {
            return requireSourceFile(fileHash);
        } finally {
            lock.readLock().unlock();
        }
    }

    private byte[] readSourceChunk(SourceFile sourceFile, PlannedChunkRead chunk) throws IOException {
        byte[] payload = new byte[chunk.chunkSize()];
        try (RandomAccessFile file = new RandomAccessFile(sourceFile.localPath().toFile(), "r")) {
            file.seek(chunk.fileOffset());
            file.readFully(payload);
        }
        CxnfsStore.requireVerifiedPlanChunk(chunk, payload);
        return payload;
    }

    private boolean isServedDirectory(String normalizedPath) {
        for (ServedRoot root : roots) {
            if (normalizedPath.equals(root.remotePrefix())) {
                return true;
            }
        }
        return filesByPath.keySet().stream()
                .anyMatch(candidate -> normalizedPath.equals("/")
                        ? candidate.length() > 1
                        : candidate.startsWith(normalizedPath + "/"));
    }

    private List<String> pathsUnder(String directory) {
        lock.readLock().lock();
        try {
            return filesByPath.keySet().stream()
                    .filter(candidate -> isPathUnder(directory, candidate))
                    .sorted()
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void removePaths(List<String> paths) {
        lock.writeLock().lock();
        try {
            removePathsLocked(paths);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void removePathsLocked(List<String> paths) {
        for (String path : paths) {
            SourceFile removed = filesByPath.remove(path);
            if (removed != null && removed.manifest() != null) {
                removeHashPath(removed.manifest().fileHash(), path);
            }
        }
    }

    private void removeHashPath(ContentHash fileHash, String path) {
        LinkedHashSet<String> paths = pathsByHash.get(fileHash);
        if (paths == null) {
            return;
        }
        paths.remove(path);
        if (paths.isEmpty()) {
            pathsByHash.remove(fileHash);
        }
    }

    private List<SourceFile> renameIndexedFiles(
            String normalizedSource,
            String normalizedTarget,
            Path localSource,
            Path localTarget
    ) {
        lock.writeLock().lock();
        try {
            List<SourceFile> sourceFiles = filesByPath.values().stream()
                    .filter(sourceFile -> sourceFile.namespacePath().equals(normalizedSource)
                            || isPathUnder(normalizedSource, sourceFile.namespacePath()))
                    .sorted(Comparator.comparing(SourceFile::namespacePath))
                    .toList();
            removePathsLocked(sourceFiles.stream().map(SourceFile::namespacePath).toList());

            List<SourceFile> targetFiles = filesByPath.values().stream()
                    .filter(sourceFile -> sourceFile.namespacePath().equals(normalizedTarget)
                            || isPathUnder(normalizedTarget, sourceFile.namespacePath()))
                    .sorted(Comparator.comparing(SourceFile::namespacePath))
                    .toList();
            removePathsLocked(targetFiles.stream().map(SourceFile::namespacePath).toList());

            List<SourceFile> renamed = new ArrayList<>();
            for (SourceFile sourceFile : sourceFiles) {
                String suffix = sourceFile.namespacePath().substring(normalizedSource.length());
                String targetPath = normalizedTarget + suffix;
                Path targetLocalPath = localTarget.resolve(localSource.relativize(sourceFile.localPath())).normalize();
                if (sourceFile.namespacePath().equals(normalizedSource)) {
                    targetLocalPath = localTarget;
                }
                FileManifest manifest = sourceFile.manifest() == null ? null : new FileManifest(
                        targetPath,
                        sourceFile.manifest().fileHash(),
                        sourceFile.manifest().size(),
                        sourceFile.manifest().chunks()
                );
                SourceFile updated = new SourceFile(
                        targetPath,
                        targetLocalPath,
                        manifest,
                        sourceFile.state()
                );
                putSourceFileLocked(updated);
                renamed.add(updated);
            }
            return List.copyOf(renamed);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void publishSourceManifest(String namespacePath, Path localPath, FileManifest manifest) throws IOException {
        SourceFileState state = SourceFileState.from(Files.readAttributes(
                localPath,
                BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS
        ));
        SourceFile sourceFile = new SourceFile(
                normalizePath(namespacePath),
                localPath.toAbsolutePath().normalize(),
                new FileManifest(normalizePath(namespacePath), manifest.fileHash(), manifest.size(), manifest.chunks()),
                state
        );
        putSourceFile(sourceFile);
    }

    private Optional<FileMetadata> directChild(String directory, String candidatePath) {
        String relative = relativeTo(directory, candidatePath);
        if (relative == null || relative.isEmpty()) {
            return Optional.empty();
        }
        int slash = relative.indexOf('/');
        String name = slash >= 0 ? relative.substring(0, slash) : relative;
        String childPath = childPath(directory, name);
        return Optional.of(directoryMetadata(childPath));
    }

    private Optional<FileMetadata> directChild(String directory, SourceFile sourceFile) {
        String relative = relativeTo(directory, sourceFile.namespacePath());
        if (relative == null || relative.isEmpty()) {
            return Optional.empty();
        }
        int slash = relative.indexOf('/');
        if (slash >= 0) {
            String name = relative.substring(0, slash);
            return Optional.of(directoryMetadata(childPath(directory, name)));
        }
        return Optional.of(fileMetadata(sourceFile, relative));
    }

    private static FileMetadata directoryMetadata(String path) {
        return new FileMetadata(path, nameOf(path), FileType.DIRECTORY, 0, Optional.empty(), Instant.EPOCH);
    }

    private static FileMetadata fileMetadata(SourceFile sourceFile, String name) {
        return new FileMetadata(
                sourceFile.namespacePath(),
                name,
                FileType.REGULAR_FILE,
                sourceFile.state().size(),
                sourceFile.manifest() == null
                        ? Optional.empty()
                        : Optional.of(sourceFile.manifest().fileHash()),
                sourceFile.state().modifiedAt().toInstant()
        );
    }

    private static List<FileManifest> hydratedManifests(List<SourceFile> sourceFiles) {
        return sourceFiles.stream()
                .map(SourceFile::manifest)
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    private static String namespacePath(String remotePrefix, Path relativePath) {
        String relative = relativePath.toString().replace('\\', '/');
        if (remotePrefix.equals("/")) {
            return "/" + relative;
        }
        return remotePrefix + "/" + relative;
    }

    private static boolean isPathUnder(String directory, String path) {
        String normalizedDirectory = normalizePath(directory);
        String normalizedPath = normalizePath(path);
        return normalizedDirectory.equals("/")
                ? normalizedPath.length() > 1
                : normalizedPath.startsWith(normalizedDirectory + "/");
    }

    private static String relativeTo(String directory, String path) {
        String normalizedDirectory = normalizeDirectoryPath(directory);
        String normalizedPath = normalizePath(path);
        if (normalizedDirectory.equals("/")) {
            return normalizedPath.equals("/") ? "" : normalizedPath.substring(1);
        }
        if (normalizedPath.equals(normalizedDirectory)) {
            return "";
        }
        if (!normalizedPath.startsWith(normalizedDirectory + "/")) {
            return null;
        }
        return normalizedPath.substring(normalizedDirectory.length() + 1);
    }

    private static String childPath(String directory, String name) {
        return normalizeDirectoryPath(directory).equals("/") ? "/" + name : normalizeDirectoryPath(directory) + "/" + name;
    }

    private static String nameOf(String path) {
        String normalizedPath = normalizePath(path);
        if (normalizedPath.equals("/")) {
            return "";
        }
        int slash = normalizedPath.lastIndexOf('/');
        return slash < 0 ? normalizedPath : normalizedPath.substring(slash + 1);
    }

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }

    private static String normalizeDirectoryPath(String path) {
        return NamespacePath.normalizeDirectory(path);
    }

    private record SourceFile(
            String namespacePath,
            Path localPath,
            FileManifest manifest,
            SourceFileState state
    ) {
    }

    private record SourceFileState(
            long size,
            FileTime modifiedAt,
            String fileKey
    ) {
        static SourceFileState from(BasicFileAttributes attributes) {
            Object key = attributes.fileKey();
            return new SourceFileState(
                    attributes.size(),
                    attributes.lastModifiedTime(),
                    key == null ? "" : key.toString()
            );
        }
    }

    private final class ThreadedSourceBackedPublisher implements StreamingVerifiedEntryPublisher {
        private final String namespacePath;
        private final Path targetPath;
        private final Path tempPath;
        private final Path statePath;
        private final Path chunkDirectory;
        private final long expectedSizeBytes;
        private final BlockingQueue<WriteCommand> queue = new ArrayBlockingQueue<>(WRITER_QUEUE_DEPTH);
        private final AtomicReference<IOException> failure = new AtomicReference<>();
        private final Map<Long, AcceptedChunk> acceptedChunks = new LinkedHashMap<>();
        private final Map<Long, ContentHash> pendingChunkHashes = new LinkedHashMap<>();
        private final TransferChecksums.Crc32cAccumulator checksum = TransferChecksums.crc32cAccumulator();
        private final AtomicInteger maxWriterQueueDepth = new AtomicInteger();
        private final AtomicInteger writerQueueFullEvents = new AtomicInteger();
        private final AtomicInteger writerQueueWaits = new AtomicInteger();
        private final AtomicLong writerQueueWaitNanos = new AtomicLong();
        private final AtomicInteger maxPendingChecksumCommands = new AtomicInteger();
        private final AtomicLong maxPendingChecksumPayloadBytes = new AtomicLong();
        private final AtomicInteger sparseZeroBatches = new AtomicInteger();
        private final AtomicLong sparseZeroBatchFrames = new AtomicLong();
        private final AtomicLong sparseZeroBatchBytes = new AtomicLong();
        private final Thread writerThread;
        private long pendingChecksumPayloadBytes;
        private long checksumBytes;
        private boolean checksumRequiresFileScan;
        private boolean finished;
        private boolean finishQueued;

        private ThreadedSourceBackedPublisher(String namespacePath, Path targetPath, long expectedSizeBytes) throws IOException {
            this.namespacePath = namespacePath;
            this.targetPath = targetPath.toAbsolutePath().normalize();
            this.expectedSizeBytes = expectedSizeBytes;
            Path parent = this.targetPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.tempPath = tempPathForTarget(this.targetPath);
            this.statePath = statePathForTemp(this.tempPath);
            this.chunkDirectory = chunkDirectoryForTemp(this.tempPath);
            prepareTempAndState();
            this.writerThread = new Thread(this::writeLoop, "cxnfs-live-source-writer-" + nameOf(namespacePath));
            this.writerThread.setDaemon(true);
            this.writerThread.start();
        }

        @Override
        public StagedPayload acceptFrame(BulkDataFrame frame) throws IOException {
            if (frame == null) {
                throw new IllegalArgumentException("frame is required");
            }
            if (!frame.finalFrameForChunk()) {
                throw new IOException("source-backed writer requires complete chunk frames");
            }
            ContentHash chunkHash = frameChunkHash(frame);
            byte[] data = frame.payloadPattern()
                    .map(pattern -> pattern.zeroRun() ? new byte[0] : null)
                    .orElseGet(frame::payloadView);
            AcceptedChunkResult accepted;
            synchronized (this) {
                accepted = acceptChunkInternal(
                        frame.chunkSequence(),
                        chunkHash,
                        frame.entryOffset(),
                        frame.payloadLength(),
                        frame.payloadPattern(),
                        data,
                        false
                );
            }
            enqueueAcceptedCommand(accepted);
            return new StagedPayload(
                    frame.key(),
                    frame.cordIndex(),
                    frame.entryOffset(),
                    frame.payloadLength(),
                    frame.finalFrameForChunk(),
                    StagedPayloadStorage.MEMORY,
                    Optional.empty(),
                    accepted.metadataVerified()
            );
        }

        @Override
        public void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
            AcceptedChunkResult accepted;
            synchronized (this) {
                accepted = acceptChunkInternal(
                        acceptedChunks.size(),
                        chunkHash,
                        sourceOffset,
                        data.length,
                        Optional.empty(),
                        data,
                        true
                );
            }
            enqueueAcceptedCommand(accepted);
        }

        @Override
        public synchronized int acceptMetadata(TransferMetadataUpdate update) throws IOException {
            if (update == null) {
                throw new IllegalArgumentException("update is required");
            }
            int verified = 0;
            for (int i = 0; i < update.chunkHashes().size(); i++) {
                long chunkSequence = update.metadataSequence() + i;
                ContentHash expectedHash = update.chunkHashes().get(i);
                ContentHash priorHash = pendingChunkHashes.putIfAbsent(chunkSequence, expectedHash);
                if (priorHash != null && !priorHash.equals(expectedHash)) {
                    throw new IOException("conflicting chunk metadata for source-backed writer sequence "
                            + chunkSequence);
                }
                AcceptedChunk accepted = acceptedChunks.get(chunkSequence);
                if (accepted == null) {
                    continue;
                }
                requireAcceptedHash(chunkSequence, accepted, expectedHash);
                AcceptedChunk verifiedChunk = accepted.verified();
                acceptedChunks.put(chunkSequence, verifiedChunk);
                appendStateEntry(chunkSequence, verifiedChunk);
                pendingChunkHashes.remove(chunkSequence);
                verified++;
            }
            return verified;
        }

        @Override
        public VerifiedEntry publishEntry(TransferEntryCommitRequest request) throws IOException {
            if (request == null) {
                throw new IllegalArgumentException("request is required");
            }
            finishWriter();
            FileManifest manifest;
            FileManifest publishedManifest;
            synchronized (this) {
                IOException currentFailure = failure.get();
                if (currentFailure != null) {
                    throw currentFailure;
                }
                verifyPersistedChecksum(request.checksum(), request.bytesWritten());
                manifest = buildVerifiedManifest(request);
                publishedManifest = publishManifestAfterWriter(manifest);
            }
            return new VerifiedEntry(request.transferId(), request.entrySequence(), publishedManifest, manifest.size());
        }

        private AcceptedChunkResult acceptChunkInternal(
                long chunkSequence,
                ContentHash chunkHash,
                long sourceOffset,
                int payloadLength,
                Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern,
                byte[] data,
                boolean metadataVerified
        ) throws IOException {
            if (finished) {
                throw new IOException("source-backed writer is already finished");
            }
            IOException currentFailure = failure.get();
            if (currentFailure != null) {
                throw currentFailure;
            }
            AcceptedChunk accepted = acceptedChunks.get(chunkSequence);
            if (accepted != null) {
                if (!accepted.hash().equals(chunkHash)
                        || accepted.size() != payloadLength
                        || accepted.sourceOffset() != sourceOffset) {
                    throw new IOException("source-backed writer duplicate chunk does not match at sequence "
                            + chunkSequence);
                }
                if (metadataVerified && !accepted.metadataVerified()) {
                    AcceptedChunk verified = accepted.verified();
                    acceptedChunks.put(chunkSequence, verified);
                    appendStateEntry(chunkSequence, verified);
                    return AcceptedChunkResult.noCommand(true);
                }
                return AcceptedChunkResult.noCommand(accepted.metadataVerified());
            }
            if (expectedSizeBytes >= 0 && sourceOffset + payloadLength > expectedSizeBytes) {
                throw new IOException("source-backed writer chunk exceeds expected size");
            }
            ContentHash pendingHash = pendingChunkHashes.get(chunkSequence);
            if (pendingHash != null) {
                requireAcceptedHash(
                        chunkSequence,
                        new AcceptedChunk(sourceOffset, chunkHash, payloadLength, false, false),
                        pendingHash
                );
                metadataVerified = true;
                pendingChunkHashes.remove(chunkSequence);
            }
            AcceptedChunk newAccepted = new AcceptedChunk(sourceOffset, chunkHash, payloadLength, metadataVerified, false);
            acceptedChunks.put(
                    chunkSequence,
                    newAccepted
            );
            return new AcceptedChunkResult(
                    metadataVerified,
                    writeCommand(chunkSequence, chunkHash, sourceOffset, payloadLength, data, pattern),
                    newAccepted,
                    Optional.ofNullable(pendingHash)
            );
        }

        private void enqueueAcceptedCommand(AcceptedChunkResult accepted) throws IOException {
            if (accepted.command().isEmpty()) {
                return;
            }
            try {
                enqueue(accepted.command().orElseThrow());
            } catch (IOException e) {
                rollbackAcceptedCommand(accepted);
                throw e;
            }
        }

        private synchronized void rollbackAcceptedCommand(AcceptedChunkResult accepted) {
            WriteCommand command = accepted.command().orElse(null);
            if (command == null) {
                return;
            }
            AcceptedChunk current = acceptedChunks.get(command.chunkSequence());
            if (accepted.acceptedChunk().isPresent() && accepted.acceptedChunk().orElseThrow().equals(current)) {
                acceptedChunks.remove(command.chunkSequence());
            }
            accepted.pendingHash().ifPresent(hash -> pendingChunkHashes.putIfAbsent(command.chunkSequence(), hash));
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) throws IOException {
            if (manifest == null) {
                throw new IllegalArgumentException("manifest is required");
            }
            finishWriter();
            synchronized (this) {
                return publishManifestAfterWriter(manifest);
            }
        }

        private FileManifest publishManifestAfterWriter(FileManifest manifest) throws IOException {
            finished = true;
            IOException currentFailure = failure.get();
            if (currentFailure != null) {
                throw currentFailure;
            }
            if (expectedSizeBytes >= 0 && manifest.size() != expectedSizeBytes) {
                throw new IOException("source-backed writer manifest size does not match expected size");
            }
            if (manifest.size() != persistedBytes()) {
                throw new IOException("source-backed writer size does not match manifest");
            }
            if (liveWriteMode == LiveWriteMode.CHUNK_FILES) {
                assembleChunkFiles(manifest);
            }
            forceTempFile();
            try {
                Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.deleteIfExists(statePath);
            deleteDirectoryIfExists(chunkDirectory);
            FileManifest normalized = new FileManifest(
                    namespacePath,
                    manifest.fileHash(),
                    manifest.size(),
                    manifest.chunks()
            );
            publishSourceManifest(namespacePath, targetPath, normalized);
            logWriterStats(normalized);
            return normalized;
        }

        @Override
        public void abort() throws IOException {
            finishWriter();
        }

        private FileManifest buildVerifiedManifest(TransferEntryCommitRequest request) throws IOException {
            if (acceptedChunks.isEmpty() && request.bytesWritten() > 0) {
                throw new IllegalStateException("entry has no verified payloads: " + request.path());
            }
            List<ChunkRef> chunks = new ArrayList<>(acceptedChunks.size());
            List<ContentHash> chunkHashes = new ArrayList<>(acceptedChunks.size());
            List<Integer> chunkSizes = new ArrayList<>(acceptedChunks.size());
            long expectedSequence = 0;
            long expectedOffset = 0;
            for (Map.Entry<Long, AcceptedChunk> entry : acceptedChunks.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .toList()) {
                if (entry.getKey() != expectedSequence) {
                    throw new IllegalStateException("missing verified chunk sequence: " + expectedSequence);
                }
                AcceptedChunk accepted = entry.getValue();
                if (accepted.sourceOffset() != expectedOffset) {
                    throw new IllegalStateException("non-contiguous verified payload at offset: "
                            + accepted.sourceOffset());
                }
                if (!accepted.metadataVerified()) {
                    throw new IllegalStateException("source-backed writer metadata is missing for chunk sequence: "
                            + entry.getKey());
                }
                if (!accepted.persisted()) {
                    throw new IllegalStateException("source-backed writer has not persisted chunk sequence: "
                            + entry.getKey());
                }
                chunks.add(new ChunkRef(accepted.hash(), accepted.size()));
                chunkHashes.add(accepted.hash());
                chunkSizes.add(accepted.size());
                expectedOffset += accepted.size();
                expectedSequence++;
            }
            if (expectedOffset != request.bytesWritten()) {
                throw new IllegalStateException("verified entry size does not match commit request");
            }
            ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
            if (!fileHash.equals(request.contentHash())) {
                throw new IOException("verified entry hash does not match commit request");
            }
            return new FileManifest(namespacePath, request.contentHash(), request.bytesWritten(), chunks);
        }

        private void requireAcceptedHash(
                long chunkSequence,
                AcceptedChunk accepted,
                ContentHash expectedHash
        ) throws IOException {
            if (!accepted.hash().equals(expectedHash)) {
                throw new IOException("source-backed writer metadata hash mismatch at sequence " + chunkSequence);
            }
        }

        private void enqueue(WriteCommand command) throws IOException {
            updateMax(maxWriterQueueDepth, queue.size());
            boolean full = queue.remainingCapacity() == 0;
            long waitStart = full ? System.nanoTime() : 0L;
            if (full) {
                writerQueueFullEvents.incrementAndGet();
            }
            try {
                queue.put(command);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for source-backed writer", e);
            }
            if (full) {
                writerQueueWaits.incrementAndGet();
                writerQueueWaitNanos.addAndGet(System.nanoTime() - waitStart);
            }
            updateMax(maxWriterQueueDepth, queue.size());
        }

        private void joinWriter() throws IOException {
            try {
                writerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for source-backed writer", e);
            }
        }

        private void finishWriter() throws IOException {
            boolean enqueueEnd = false;
            synchronized (this) {
                finished = true;
                if (!finishQueued) {
                    finishQueued = true;
                    enqueueEnd = true;
                }
            }
            if (enqueueEnd) {
                enqueue(WriteCommand.endCommand());
            }
            joinWriter();
        }

        private void writeLoop() {
            try (FileChannel channel = FileChannel.open(
                    tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ
            )) {
                Map<Long, WriteCommand> pendingChecksumCommands = new LinkedHashMap<>();
                long nextChecksumSequence = nextUnpersistedSequence();
                WriteCommand deferred = null;
                while (true) {
                    WriteCommand command;
                    if (deferred == null) {
                        command = queue.take();
                    } else {
                        command = deferred;
                        deferred = null;
                    }
                    if (command.end()) {
                        nextChecksumSequence = drainPendingChecksums(pendingChecksumCommands, nextChecksumSequence);
                        if (!pendingChecksumCommands.isEmpty()) {
                            throw new IOException("source-backed writer has non-contiguous pending chunks at finish");
                        }
                        forceChannel(channel, false);
                        break;
                    }
                    WriteBatch batch;
                    if (canBatchSparseZeroRun(command)) {
                        batch = writeSparseZeroRunBatch(channel, command);
                        deferred = batch.deferred().orElse(null);
                    } else {
                        writeAndMarkPersisted(channel, command);
                        batch = WriteBatch.single(command);
                    }
                    nextChecksumSequence = recordPersistedChecksums(
                            batch.commands(),
                            pendingChecksumCommands,
                            nextChecksumSequence
                    );
                    updateMax(maxPendingChecksumCommands, pendingChecksumCommands.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure.compareAndSet(null, new IOException("source-backed writer interrupted", e));
            } catch (IOException e) {
                failure.compareAndSet(null, e);
            }
        }

        private void writeAndMarkPersisted(FileChannel channel, WriteCommand command) throws IOException {
            if (liveWriteMode == LiveWriteMode.CHUNK_FILES) {
                writeChunkFileCommand(command);
            } else {
                writeCommand(channel, command);
            }
            markPersisted(command.chunkSequence());
        }

        private WriteBatch writeSparseZeroRunBatch(FileChannel channel, WriteCommand first) throws IOException {
            List<WriteCommand> commands = new ArrayList<>();
            commands.add(first);
            long nextSequence = first.chunkSequence() + 1L;
            long nextOffset = first.sourceOffset() + first.size();
            WriteCommand deferred = null;
            while (true) {
                WriteCommand next = queue.poll();
                if (next == null) {
                    break;
                }
                if (!canMergeSparseZeroRun(next, nextSequence, nextOffset)) {
                    deferred = next;
                    break;
                }
                commands.add(next);
                nextSequence++;
                nextOffset += next.size();
            }
            ensureSparseZeroRange(channel, first.sourceOffset(), nextOffset - first.sourceOffset());
            markPersisted(commands);
            sparseZeroBatches.incrementAndGet();
            sparseZeroBatchFrames.addAndGet(commands.size());
            sparseZeroBatchBytes.addAndGet(nextOffset - first.sourceOffset());
            return new WriteBatch(List.copyOf(commands), Optional.ofNullable(deferred));
        }

        private void logWriterStats(FileManifest manifest) {
            if (manifest.size() < WRITER_STATS_MIN_BYTES
                    && writerQueueFullEvents.get() == 0
                    && sparseZeroBatches.get() == 0) {
                return;
            }
            String queuePressure = writerQueueFullEvents.get() > 0
                    || maxWriterQueueDepth.get() >= WRITER_QUEUE_DEPTH
                    ? "high"
                    : "normal";
            System.err.println(String.format(Locale.ROOT,
                    "cxnfs source writer stats path=%s bytes=%d queue_depth_limit=%d "
                            + "max_queue_depth=%d queue_pressure=%s queue_full_events=%d "
                            + "queue_waits=%d queue_wait_ms=%.1f max_pending_checksum=%d "
                            + "max_pending_checksum_payload_bytes=%d "
                            + "checksum_file_scan=%s force_on_commit=%s sparse_zero_batches=%d "
                            + "sparse_zero_frames=%d sparse_zero_bytes=%d",
                    namespacePath,
                    manifest.size(),
                    WRITER_QUEUE_DEPTH,
                    maxWriterQueueDepth.get(),
                    queuePressure,
                    writerQueueFullEvents.get(),
                    writerQueueWaits.get(),
                    writerQueueWaitNanos.get() / 1_000_000.0d,
                    maxPendingChecksumCommands.get(),
                    maxPendingChecksumPayloadBytes.get(),
                    checksumRequiresFileScan,
                    FORCE_LIVE_WRITES_ON_COMMIT,
                    sparseZeroBatches.get(),
                    sparseZeroBatchFrames.get(),
                    sparseZeroBatchBytes.get()));
        }

        private boolean canBatchSparseZeroRun(WriteCommand command) {
            return liveWriteMode != LiveWriteMode.CHUNK_FILES && command.zeroRun();
        }

        private boolean canMergeSparseZeroRun(WriteCommand command, long nextSequence, long nextOffset) {
            return canBatchSparseZeroRun(command)
                    && command.chunkSequence() == nextSequence
                    && command.sourceOffset() == nextOffset;
        }

        private void ensureSparseZeroRange(FileChannel channel, long offset, long length) throws IOException {
            if (expectedSizeBytes >= 0 || length <= 0) {
                return;
            }
            long endOffset = offset + length;
            if (endOffset < offset) {
                throw new IOException("source-backed writer sparse zero range overflow");
            }
            if (channel.size() < endOffset) {
                channel.write(ByteBuffer.wrap(new byte[]{0}), endOffset - 1L);
            }
        }

        private long drainPendingChecksums(
                Map<Long, WriteCommand> pendingCommands,
                long nextChecksumSequence
        ) throws IOException {
            while (true) {
                WriteCommand command = removePendingChecksumCommand(pendingCommands, nextChecksumSequence);
                if (command == null) {
                    return nextChecksumSequence;
                }
                if (checksumRequiresFileScan || !command.hasChecksumPayload()) {
                    if (liveWriteMode == LiveWriteMode.CHUNK_FILES && !checksumRequiresFileScan) {
                        updateChecksumFromChunkFile(command);
                    } else {
                        checksumRequiresFileScan = true;
                    }
                    nextChecksumSequence++;
                } else if (command.zeroRun()) {
                    ZeroChecksumBatch batch = zeroChecksumBatch(command, pendingCommands, nextChecksumSequence);
                    checksum.updateZeros(batch.bytes());
                    checksumBytes += batch.bytes();
                    nextChecksumSequence = batch.nextSequence();
                } else {
                    updateChecksum(command);
                    nextChecksumSequence++;
                }
            }
        }

        private long recordPersistedChecksums(
                List<WriteCommand> commands,
                Map<Long, WriteCommand> pendingCommands,
                long nextChecksumSequence
        ) throws IOException {
            for (WriteCommand command : commands) {
                if (!checksumRequiresFileScan
                        && pendingCommands.isEmpty()
                        && command.chunkSequence() == nextChecksumSequence) {
                    if (command.zeroRun()) {
                        checksum.updateZeros(command.size());
                        checksumBytes += command.size();
                    } else {
                        updateChecksum(command);
                    }
                    nextChecksumSequence++;
                    continue;
                }
                if (liveWriteMode == LiveWriteMode.CHUNK_FILES) {
                    command = command.withoutData();
                } else {
                    command = pendingChecksumCommand(command, pendingCommands);
                }
                putPendingChecksumCommand(pendingCommands, command);
            }
            return drainPendingChecksums(pendingCommands, nextChecksumSequence);
        }

        private WriteCommand pendingChecksumCommand(
                WriteCommand command,
                Map<Long, WriteCommand> pendingCommands
        ) {
            if (checksumRequiresFileScan || !command.hasChecksumPayload()) {
                requireChecksumFileScan(pendingCommands);
                return command.withoutData();
            }
            return command.withoutData();
        }

        private void putPendingChecksumCommand(Map<Long, WriteCommand> pendingCommands, WriteCommand command) {
            WriteCommand prior = pendingCommands.put(command.chunkSequence(), command);
            if (prior != null) {
                pendingChecksumPayloadBytes -= prior.checksumPayloadBytes();
            }
            pendingChecksumPayloadBytes += command.checksumPayloadBytes();
            updateMax(maxPendingChecksumPayloadBytes, pendingChecksumPayloadBytes);
        }

        private WriteCommand removePendingChecksumCommand(Map<Long, WriteCommand> pendingCommands, long sequence) {
            WriteCommand command = pendingCommands.remove(sequence);
            if (command != null) {
                pendingChecksumPayloadBytes -= command.checksumPayloadBytes();
                if (pendingChecksumPayloadBytes < 0L) {
                    pendingChecksumPayloadBytes = 0L;
                }
            }
            return command;
        }

        private void requireChecksumFileScan(Map<Long, WriteCommand> pendingCommands) {
            if (checksumRequiresFileScan) {
                return;
            }
            checksumRequiresFileScan = true;
            if (pendingChecksumPayloadBytes == 0L || pendingCommands.isEmpty()) {
                pendingChecksumPayloadBytes = 0L;
                return;
            }
            pendingCommands.replaceAll((ignored, command) -> command.withoutData());
            pendingChecksumPayloadBytes = 0L;
        }

        private ZeroChecksumBatch zeroChecksumBatch(
                WriteCommand first,
                Map<Long, WriteCommand> pendingCommands,
                long firstSequence
        ) {
            long bytes = first.size();
            long nextSequence = firstSequence + 1L;
            long nextOffset = first.sourceOffset() + first.size();
            while (true) {
                WriteCommand next = pendingCommands.get(nextSequence);
                if (next == null || !next.zeroRun() || next.sourceOffset() != nextOffset) {
                    return new ZeroChecksumBatch(nextSequence, bytes);
                }
                pendingCommands.remove(nextSequence);
                bytes += next.size();
                nextOffset += next.size();
                nextSequence++;
            }
        }

        private void writeCommand(FileChannel channel, WriteCommand command) throws IOException {
            Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern = command.pattern();
            if (pattern.isPresent()) {
                writePattern(channel, command, pattern.orElseThrow());
                return;
            }
            byte[] data = command.data();
            writeAll(channel, ByteBuffer.wrap(data), command.sourceOffset());
        }

        private void writePattern(
                FileChannel channel,
                WriteCommand command,
                com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern pattern
        ) throws IOException {
            if (pattern.zeroRun()) {
                return;
            }
            byte[] materialized = com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.materializePayloadPattern(pattern);
            writeAll(channel, ByteBuffer.wrap(materialized), command.sourceOffset());
        }

        private void writeChunkFileCommand(WriteCommand command) throws IOException {
            Files.createDirectories(chunkDirectory);
            Path chunkPath = chunkPath(command.chunkSequence());
            Path partialPath = chunkPath.resolveSibling(chunkPath.getFileName() + ".partial");
            Files.deleteIfExists(partialPath);
            Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern = command.pattern();
            if (pattern.isPresent()) {
                writePatternChunkFile(partialPath, command, pattern.orElseThrow());
            } else {
                byte[] data = command.data();
                Files.write(
                        partialPath,
                        data,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                );
            }
            try {
                Files.move(partialPath, chunkPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                Files.move(partialPath, chunkPath, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        private void writePatternChunkFile(
                Path chunkPath,
                WriteCommand command,
                com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern pattern
        ) throws IOException {
            if (pattern.zeroRun()) {
                try (FileChannel channel = FileChannel.open(
                        chunkPath,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ
                )) {
                    ensureChannelLength(channel, command.size());
                    channel.force(false);
                }
                return;
            }
            byte[] materialized = com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.materializePayloadPattern(pattern);
            try (FileChannel channel = FileChannel.open(
                    chunkPath,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
            )) {
                writeAll(channel, ByteBuffer.wrap(materialized), 0L);
                channel.force(false);
            }
        }

        private void writeAll(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
            long cursor = position;
            while (buffer.hasRemaining()) {
                int written = channel.write(buffer, cursor);
                if (written <= 0) {
                    throw new IOException("source-backed writer failed to advance positional write");
                }
                cursor += written;
            }
        }

        private synchronized void markPersisted(long chunkSequence) throws IOException {
            AcceptedChunk accepted = acceptedChunks.get(chunkSequence);
            if (accepted == null) {
                throw new IOException("source-backed writer persisted unknown chunk sequence: " + chunkSequence);
            }
            AcceptedChunk persisted = accepted.withPersisted();
            acceptedChunks.put(chunkSequence, persisted);
            appendStateEntry(chunkSequence, persisted);
        }

        private synchronized void markPersisted(List<WriteCommand> commands) throws IOException {
            if (commands.isEmpty()) {
                return;
            }
            StringBuilder builder = new StringBuilder();
            for (WriteCommand command : commands) {
                AcceptedChunk accepted = acceptedChunks.get(command.chunkSequence());
                if (accepted == null) {
                    throw new IOException("source-backed writer persisted unknown chunk sequence: "
                            + command.chunkSequence());
                }
                AcceptedChunk persisted = accepted.withPersisted();
                acceptedChunks.put(command.chunkSequence(), persisted);
                appendStateLine(builder, command.chunkSequence(), persisted);
            }
            Files.writeString(
                    statePath,
                    builder.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            );
        }

        private long persistedBytes() {
            long total = 0L;
            for (AcceptedChunk accepted : acceptedChunks.values()) {
                if (accepted.persisted()) {
                    total += accepted.size();
                }
            }
            return total;
        }

        private long nextUnpersistedSequence() {
            long sequence = 0L;
            while (true) {
                AcceptedChunk accepted = acceptedChunks.get(sequence);
                if (accepted == null || !accepted.persisted()) {
                    return sequence;
                }
                sequence++;
            }
        }

        private void updateChecksum(WriteCommand command) {
            if (command.checksumKnown()) {
                checksum.updateCrc32c(command.checksumValue(), command.size());
                checksumBytes += command.size();
                return;
            }
            Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern = command.pattern();
            if (pattern.isEmpty()) {
                byte[] data = command.data();
                checksum.update(data, 0, data.length);
                checksumBytes += data.length;
                return;
            }
            com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern payloadPattern = pattern.orElseThrow();
            if (payloadPattern.zeroRun()) {
                checksum.updateZeros(payloadPattern.originalLength());
                checksumBytes += payloadPattern.originalLength();
                return;
            }
            byte[] materialized = com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.materializePayloadPattern(payloadPattern);
            checksum.update(materialized, 0, materialized.length);
            checksumBytes += materialized.length;
        }

        private WriteCommand writeCommand(
                long chunkSequence,
                ContentHash chunkHash,
                long sourceOffset,
                int payloadLength,
                byte[] data,
                Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern
        ) throws IOException {
            return new WriteCommand(
                    chunkSequence,
                    chunkHash,
                    sourceOffset,
                    payloadLength,
                    data,
                    pattern,
                    false,
                    checksumValue(payloadLength, data, pattern),
                    true
            );
        }

        private long checksumValue(
                int payloadLength,
                byte[] data,
                Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern
        ) throws IOException {
            pattern = pattern == null ? Optional.empty() : pattern;
            if (pattern.isPresent()) {
                com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern payloadPattern = pattern.orElseThrow();
                if (payloadPattern.zeroRun()) {
                    return TransferChecksums.crc32cOfZeros(payloadPattern.originalLength());
                }
                byte[] materialized = com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.materializePayloadPattern(payloadPattern);
                return TransferChecksums.crc32cValue(materialized, 0, materialized.length);
            }
            if (data == null) {
                throw new IOException("source-backed writer cannot checksum command without payload data");
            }
            return TransferChecksums.crc32cValue(data, 0, payloadLength);
        }

        private void updateChecksumFromChunkFile(WriteCommand command) throws IOException {
            Path chunkPath = chunkPath(command.chunkSequence());
            if (!Files.exists(chunkPath)) {
                throw new IOException("source-backed writer chunk file is missing for checksum sequence "
                        + command.chunkSequence());
            }
            long size = Files.size(chunkPath);
            if (size != command.size()) {
                throw new IOException("source-backed writer chunk file checksum size mismatch for sequence "
                        + command.chunkSequence());
            }
            updateChecksumFromPath(chunkPath, command.size());
        }

        private void updateChecksumFromPath(Path path, long expectedBytes) throws IOException {
            byte[] buffer = new byte[1024 * 1024];
            long total = 0L;
            try (InputStream input = Files.newInputStream(path)) {
                while (true) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    checksum.update(buffer, 0, read);
                    total += read;
                }
            }
            if (expectedBytes >= 0 && total != expectedBytes) {
                throw new IOException("source-backed writer checksum byte count mismatch: expected "
                        + expectedBytes + ", got " + total);
            }
            checksumBytes += total;
        }

        private void verifyPersistedChecksum(TransferChecksum expected, long expectedBytes) throws IOException {
            expected = expected == null ? TransferChecksum.none() : expected;
            if (!expected.present()) {
                return;
            }
            if (checksumRequiresFileScan || checksumBytes != expectedBytes) {
                TransferChecksum actual = liveWriteMode == LiveWriteMode.CHUNK_FILES
                        ? crc32cChunkFiles(expectedBytes)
                        : TransferChecksums.crc32c(tempPath, expectedBytes);
                if (!expected.equals(actual)) {
                    throw new IOException("source-backed writer checksum mismatch: expected "
                            + expected.display() + ", got " + actual.display());
                }
                return;
            }
            TransferChecksum actual = TransferChecksums.crc32c(checksum);
            if (!expected.equals(actual)) {
                throw new IOException("source-backed writer checksum mismatch: expected "
                            + expected.display() + ", got " + actual.display());
            }
        }

        private TransferChecksum crc32cChunkFiles(long expectedBytes) throws IOException {
            TransferChecksums.Crc32cAccumulator accumulator = TransferChecksums.crc32cAccumulator();
            byte[] buffer = new byte[1024 * 1024];
            long total = 0L;
            long expectedSequence = 0L;
            long expectedOffset = 0L;
            for (Map.Entry<Long, AcceptedChunk> entry : acceptedChunks.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .toList()) {
                if (entry.getKey() != expectedSequence) {
                    throw new IOException("source-backed writer chunk checksum missing sequence: "
                            + expectedSequence);
                }
                AcceptedChunk accepted = entry.getValue();
                if (!accepted.persisted()) {
                    throw new IOException("source-backed writer chunk checksum found unpersisted sequence: "
                            + entry.getKey());
                }
                if (accepted.sourceOffset() != expectedOffset) {
                    throw new IOException("source-backed writer chunk checksum found non-contiguous offset: "
                            + accepted.sourceOffset());
                }
                Path chunkPath = chunkPath(entry.getKey());
                if (!Files.exists(chunkPath)) {
                    throw new IOException("source-backed writer chunk file is missing for checksum sequence "
                            + entry.getKey());
                }
                long size = Files.size(chunkPath);
                if (size != accepted.size()) {
                    throw new IOException("source-backed writer chunk file checksum size mismatch for sequence "
                            + entry.getKey());
                }
                long chunkBytes = 0L;
                try (InputStream input = Files.newInputStream(chunkPath)) {
                    while (true) {
                        int read = input.read(buffer);
                        if (read < 0) {
                            break;
                        }
                        accumulator.update(buffer, 0, read);
                        chunkBytes += read;
                    }
                }
                if (chunkBytes != accepted.size()) {
                    throw new IOException("source-backed writer chunk file checksum byte count mismatch for sequence "
                            + entry.getKey());
                }
                total += chunkBytes;
                expectedOffset += accepted.size();
                expectedSequence++;
            }
            if (expectedBytes >= 0 && total != expectedBytes) {
                throw new IOException("source-backed writer checksum byte count mismatch: expected "
                        + expectedBytes + ", got " + total);
            }
            return TransferChecksums.crc32c(accumulator);
        }

        private void prepareTempAndState() throws IOException {
            boolean resumed = loadState();
            if (!resumed) {
                acceptedChunks.clear();
                Files.deleteIfExists(statePath);
                Files.deleteIfExists(tempPath);
                deleteDirectoryIfExists(chunkDirectory);
            }
            if (liveWriteMode == LiveWriteMode.CHUNK_FILES) {
                Files.createDirectories(chunkDirectory);
            } else {
                deleteDirectoryIfExists(chunkDirectory);
            }
            try (FileChannel channel = FileChannel.open(
                    tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ
            )) {
                if (expectedSizeBytes >= 0) {
                    if (expectedSizeBytes == 0) {
                        channel.truncate(0L);
                    } else if (channel.size() != expectedSizeBytes) {
                        channel.write(ByteBuffer.wrap(new byte[]{0}), expectedSizeBytes - 1L);
                    }
                }
                channel.force(false);
            }
            writeStateSnapshot();
        }

        private boolean loadState() throws IOException {
            if (!Files.exists(tempPath) || !Files.exists(statePath)) {
                return false;
            }
            List<String> lines = Files.readAllLines(statePath, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                return false;
            }
            String[] header = lines.get(0).split("\t");
            if ((header.length != 2 && header.length != 3) || !header[0].equals("cxnfs-state-v1")) {
                return false;
            }
            long stateSize = Long.parseLong(header[1]);
            if (expectedSizeBytes >= 0 && stateSize != expectedSizeBytes) {
                return false;
            }
            if (header.length == 3 && !header[2].equals(liveWriteMode.configValue())) {
                return false;
            }
            for (int index = 1; index < lines.size(); index++) {
                String line = lines.get(index);
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t");
                if (parts.length != 5) {
                    return false;
                }
                long sequence = Long.parseLong(parts[0]);
                long offset = Long.parseLong(parts[1]);
                int size = Integer.parseInt(parts[2]);
                ContentHash hash = ContentHash.fromHex(parts[3]);
                boolean metadataVerified = Boolean.parseBoolean(parts[4]);
                acceptedChunks.put(sequence, new AcceptedChunk(offset, hash, size, metadataVerified, true));
            }
            return true;
        }

        private synchronized void writeStateSnapshot() throws IOException {
            StringBuilder builder = new StringBuilder();
            builder.append("cxnfs-state-v1\t")
                    .append(expectedSizeBytes)
                    .append('\t')
                    .append(liveWriteMode.configValue())
                    .append('\n');
            acceptedChunks.entrySet().stream()
                    .filter(entry -> entry.getValue().persisted())
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> appendStateLine(builder, entry.getKey(), entry.getValue()));
            Files.writeString(
                    statePath,
                    builder.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }

        private synchronized void appendStateEntry(long chunkSequence, AcceptedChunk accepted) throws IOException {
            StringBuilder builder = new StringBuilder();
            appendStateLine(builder, chunkSequence, accepted);
            Files.writeString(
                    statePath,
                    builder.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            );
        }

        private void appendStateLine(StringBuilder builder, long chunkSequence, AcceptedChunk accepted) {
            builder
                    .append(chunkSequence).append('\t')
                    .append(accepted.sourceOffset()).append('\t')
                    .append(accepted.size()).append('\t')
                    .append(accepted.hash().hex()).append('\t')
                    .append(accepted.metadataVerified()).append('\n');
        }

        private void forceTempFile() throws IOException {
            if (!FORCE_LIVE_WRITES_ON_COMMIT) {
                return;
            }
            try (FileChannel channel = FileChannel.open(tempPath, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
        }

        private void assembleChunkFiles(FileManifest manifest) throws IOException {
            try (FileChannel channel = FileChannel.open(
                    tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                ensureChannelLength(channel, manifest.size());
                for (Map.Entry<Long, AcceptedChunk> entry : acceptedChunks.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .toList()) {
                    AcceptedChunk accepted = entry.getValue();
                    Path chunkPath = chunkPath(entry.getKey());
                    if (!Files.exists(chunkPath)) {
                        throw new IOException("source-backed writer chunk file is missing for sequence "
                                + entry.getKey());
                    }
                    if (Files.size(chunkPath) != accepted.size()) {
                        throw new IOException("source-backed writer chunk file size mismatch for sequence "
                                + entry.getKey());
                    }
                    try (FileChannel source = FileChannel.open(chunkPath, StandardOpenOption.READ)) {
                        long position = accepted.sourceOffset();
                        long remaining = accepted.size();
                        while (remaining > 0) {
                            long copied = source.transferTo(accepted.size() - remaining, remaining, channel.position(position));
                            if (copied <= 0) {
                                ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(remaining, 1024 * 1024));
                                int read = source.read(buffer, accepted.size() - remaining);
                                if (read < 0) {
                                    throw new IOException("source-backed writer chunk file ended early");
                                }
                                buffer.flip();
                                writeAll(channel, buffer, position);
                                copied = read;
                            }
                            position += copied;
                            remaining -= copied;
                        }
                    }
                }
                channel.force(false);
            }
        }

        private Path chunkPath(long chunkSequence) {
            return chunkDirectory.resolve(String.format(java.util.Locale.ROOT, "%020d.chunk", chunkSequence));
        }
    }

    private static Path tempPathForTarget(Path targetPath) {
        Path fileName = targetPath.getFileName();
        String tempName = (fileName == null ? "cxnfs-target" : fileName.toString()) + ".tmp";
        Path parent = targetPath.getParent();
        return parent == null ? Path.of(tempName) : parent.resolve(tempName);
    }

    private static Path statePathForTemp(Path tempPath) {
        Path fileName = tempPath.getFileName();
        String stateName = (fileName == null ? "cxnfs-target.tmp" : fileName.toString()) + ".cxnfs-state";
        Path parent = tempPath.getParent();
        return parent == null ? Path.of(stateName) : parent.resolve(stateName);
    }

    private static Path chunkDirectoryForTemp(Path tempPath) {
        Path fileName = tempPath.getFileName();
        String directoryName = (fileName == null ? "cxnfs-target.tmp" : fileName.toString()) + ".chunks";
        Path parent = tempPath.getParent();
        return parent == null ? Path.of(directoryName) : parent.resolve(directoryName);
    }

    private static void ensureChannelLength(FileChannel channel, long size) throws IOException {
        if (size < 0) {
            return;
        }
        if (size == 0) {
            channel.truncate(0L);
            return;
        }
        if (channel.size() > size) {
            channel.truncate(size);
        }
        if (channel.size() < size) {
            channel.write(ByteBuffer.wrap(new byte[]{0}), size - 1L);
        }
    }

    private static void deleteDirectoryIfExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void updateMax(AtomicInteger target, int value) {
        target.accumulateAndGet(value, Math::max);
    }

    private static void updateMax(AtomicLong target, long value) {
        target.accumulateAndGet(value, Math::max);
    }

    private static void forceChannel(FileChannel channel, boolean metadata) throws IOException {
        if (FORCE_LIVE_WRITES_ON_COMMIT) {
            channel.force(metadata);
        }
    }

    private static boolean forceLiveWritesOnCommit() {
        String property = System.getProperty("cxnfs.liveWriteForceOnCommit");
        if (property != null) {
            return parseBoolean(property, false);
        }
        return parseBoolean(System.getenv("CXNFS_LIVE_WRITE_FORCE_ON_COMMIT"), false);
    }

    private static boolean parseBoolean(String value, boolean fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("1")
                || normalized.equals("true")
                || normalized.equals("yes")
                || normalized.equals("on");
    }

    private static ContentHash frameChunkHash(BulkDataFrame frame) {
        Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern = frame.payloadPattern();
        if (pattern.isEmpty()) {
            return Hashing.chunk(frame.payloadView());
        }
        com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern payloadPattern = pattern.orElseThrow();
        return switch (payloadPattern.kind()) {
            case RUN -> Hashing.chunkRun(payloadPattern.value(), payloadPattern.originalLength());
            case REPEATED_SYMBOL -> Hashing.chunkRepeatedSymbol(payloadPattern.symbol(), payloadPattern.originalLength());
        };
    }

    private record AcceptedChunkResult(
            boolean metadataVerified,
            Optional<WriteCommand> command,
            Optional<AcceptedChunk> acceptedChunk,
            Optional<ContentHash> pendingHash
    ) {
        private AcceptedChunkResult(
                boolean metadataVerified,
                WriteCommand command,
                AcceptedChunk acceptedChunk,
                Optional<ContentHash> pendingHash
        ) {
            this(
                    metadataVerified,
                    Optional.ofNullable(command),
                    Optional.ofNullable(acceptedChunk),
                    pendingHash == null ? Optional.empty() : pendingHash
            );
        }

        private static AcceptedChunkResult noCommand(boolean metadataVerified) {
            return new AcceptedChunkResult(metadataVerified, Optional.empty(), Optional.empty(), Optional.empty());
        }
    }

    private record WriteCommand(
            long chunkSequence,
            ContentHash chunkHash,
            long sourceOffset,
            int size,
            byte[] data,
            Optional<com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern> pattern,
            boolean end,
            long checksumValue,
            boolean checksumKnown
    ) {
        private static WriteCommand endCommand() {
            return new WriteCommand(0L, null, 0L, 0, new byte[0], Optional.empty(), true, 0L, false);
        }

        private boolean zeroRun() {
            return pattern.map(com.fortrow.cxnfs.transport.BulkPayloadPatternCodec.PayloadPattern::zeroRun)
                    .orElse(false);
        }

        private boolean hasChecksumPayload() {
            return checksumKnown || data != null || pattern.isPresent();
        }

        private long checksumPayloadBytes() {
            return data == null ? 0L : data.length;
        }

        private WriteCommand withoutData() {
            return data == null || data.length == 0
                    ? this
                    : new WriteCommand(
                            chunkSequence,
                            chunkHash,
                            sourceOffset,
                            size,
                            null,
                            pattern,
                            end,
                            checksumValue,
                            checksumKnown
                    );
        }
    }

    private record WriteBatch(
            List<WriteCommand> commands,
            Optional<WriteCommand> deferred
    ) {
        private WriteBatch {
            commands = commands == null ? List.of() : List.copyOf(commands);
            deferred = deferred == null ? Optional.empty() : deferred;
        }

        private static WriteBatch single(WriteCommand command) {
            return new WriteBatch(List.of(command), Optional.empty());
        }
    }

    private record ZeroChecksumBatch(long nextSequence, long bytes) {
        private ZeroChecksumBatch {
            if (bytes < 0) {
                throw new IllegalArgumentException("bytes must be non-negative");
            }
        }
    }

    private record AcceptedChunk(
            long sourceOffset,
            ContentHash hash,
            int size,
            boolean metadataVerified,
            boolean persisted
    ) {
        private AcceptedChunk verified() {
            return new AcceptedChunk(sourceOffset, hash, size, true, persisted);
        }

        private AcceptedChunk withPersisted() {
            return new AcceptedChunk(sourceOffset, hash, size, metadataVerified, true);
        }
    }
}
