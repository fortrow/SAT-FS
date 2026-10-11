package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.Chunker;
import com.fortrow.cxnfs.codec.CompressionCodec;
import com.fortrow.cxnfs.codec.CompressionCodecRegistry;
import com.fortrow.cxnfs.codec.IdentityCompressionCodec;
import com.fortrow.cxnfs.container.Container;
import com.fortrow.cxnfs.container.ContainerBuilder;
import com.fortrow.cxnfs.container.ContainerChunkLocation;
import com.fortrow.cxnfs.container.StoredChunk;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.ReadPlanner;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.io.WriteExtent;
import com.fortrow.cxnfs.io.WriteMode;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public final class CxnfsRepository implements CxnfsStore {
    private final Chunker chunker;
    private final CompressionCodec codec;
    private final CompressionCodecRegistry codecRegistry;
    private final ContainerBuilder containerBuilder;
    private final ReadPlanner readPlanner = new ReadPlanner();
    private final Map<ContentHash, ContainerChunkLocation> chunkIndex = new LinkedHashMap<>();
    private final Map<ContentHash, Container> containers = new LinkedHashMap<>();
    private final Map<ContentHash, FileManifest> manifests = new LinkedHashMap<>();
    private final Map<String, FileManifest> manifestsByPath = new LinkedHashMap<>();
    private final Map<String, Instant> modifiedAtByPath = new LinkedHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public CxnfsRepository(Chunker chunker) {
        this(chunker, new IdentityCompressionCodec());
    }

    public CxnfsRepository(Chunker chunker, CompressionCodec codec) {
        this(chunker, codec, CompressionCodecRegistry.defaults());
    }

    CxnfsRepository(Chunker chunker, CompressionCodec codec, CompressionCodecRegistry codecRegistry) {
        if (chunker == null) {
            throw new IllegalArgumentException("chunker is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        if (codecRegistry == null) {
            throw new IllegalArgumentException("codecRegistry is required");
        }
        this.chunker = chunker;
        this.codec = codec;
        this.codecRegistry = codecRegistry;
        this.containerBuilder = new ContainerBuilder(codec);
    }

    @Override
    public FileManifest put(String path, byte[] data) throws IOException {
        return writeLockedIo(() -> {
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            String normalizedPath = normalizePath(path);
            requireWritableFilePath(normalizedPath);
            List<Chunk> chunks = chunker.split(data);
            List<ContentHash> chunkHashes = chunks.stream().map(Chunk::hash).toList();
            List<Integer> chunkSizes = chunks.stream().map(Chunk::size).toList();
            ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);

            Map<ContentHash, Chunk> missingChunks = new LinkedHashMap<>();
            for (Chunk chunk : chunks) {
                if (!chunkIndex.containsKey(chunk.hash())) {
                    missingChunks.putIfAbsent(chunk.hash(), chunk);
                }
            }

            if (!missingChunks.isEmpty()) {
                Container container = containerBuilder.build(new ArrayList<>(missingChunks.values()));
                containers.put(container.hash(), container);
                for (int i = 0; i < container.chunks().size(); i++) {
                    StoredChunk storedChunk = container.chunkAt(i);
                    chunkIndex.put(storedChunk.chunkHash(), new ContainerChunkLocation(container.hash(), i));
                }
            }

            List<ChunkRef> refs = chunks.stream()
                    .map(chunk -> new ChunkRef(chunk.hash(), chunk.size()))
                    .toList();
            FileManifest manifest = new FileManifest(normalizedPath, fileHash, data.length, refs);
            manifests.put(fileHash, manifest);
            manifestsByPath.put(normalizedPath, manifest);
            modifiedAtByPath.put(normalizedPath, Instant.now());
            return manifest;
        });
    }

    @Override
    public FileManifest putStream(String path, InputStream data) throws IOException {
        return writeLockedIo(() -> {
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            String normalizedPath = normalizePath(path);
            requireWritableFilePath(normalizedPath);
            List<ChunkRef> refs = new ArrayList<>();
            List<ContentHash> chunkHashes = new ArrayList<>();
            List<Integer> chunkSizes = new ArrayList<>();
            long[] size = new long[1];

            chunker.split(data, chunk -> {
                indexChunkIfMissing(chunk);
                ChunkRef ref = new ChunkRef(chunk.hash(), chunk.size());
                refs.add(ref);
                chunkHashes.add(chunk.hash());
                chunkSizes.add(chunk.size());
                size[0] += chunk.size();
            });

            ContentHash fileHash = Hashing.file(chunkHashes, chunkSizes);
            FileManifest manifest = new FileManifest(normalizedPath, fileHash, size[0], refs);
            manifests.put(fileHash, manifest);
            manifestsByPath.put(normalizedPath, manifest);
            modifiedAtByPath.put(normalizedPath, Instant.now());
            return manifest;
        });
    }

    @Override
    public FileManifest commit(StagedWrite stagedWrite) throws IOException {
        return writeLockedIo(() -> {
            if (stagedWrite == null) {
                throw new IllegalArgumentException("stagedWrite is required");
            }
            Optional<FileManifest> patched = commitCopyOnWritePatch(stagedWrite);
            if (patched.isPresent()) {
                return patched.orElseThrow();
            }
            try (InputStream input = stagedWrite.openStream()) {
                return putStream(stagedWrite.path(), input);
            }
        });
    }

    @Override
    public void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
        writeLockedIo(() -> {
            if (chunkHash == null) {
                throw new IllegalArgumentException("chunkHash is required");
            }
            if (sourceOffset < 0) {
                throw new IllegalArgumentException("sourceOffset must be non-negative");
            }
            if (data == null) {
                throw new IllegalArgumentException("data is required");
            }
            ContentHash actualHash = Hashing.chunk(data);
            if (!actualHash.equals(chunkHash)) {
                throw new IOException("verified chunk hash does not match payload");
            }
            if (chunkIndex.containsKey(chunkHash)) {
                return;
            }

            Container container = containerBuilder.build(List.of(new Chunk(chunkHash, sourceOffset, data)));
            containers.put(container.hash(), container);
            chunkIndex.put(chunkHash, new ContainerChunkLocation(container.hash(), 0));
        });
    }

    @Override
    public FileManifest publishManifest(FileManifest manifest) {
        return writeLocked(() -> {
            requireValidManifest(manifest);
            for (ChunkRef chunk : manifest.chunks()) {
                locationFor(chunk.hash());
            }
            return publishManifestUncheckedLocked(manifest);
        });
    }

    FileManifest publishManifestUnchecked(FileManifest manifest) {
        return writeLocked(() -> {
            requireValidManifest(manifest);
            return publishManifestUncheckedLocked(manifest);
        });
    }

    @Override
    public PathDeleteResult deletePath(String path, boolean recursive) {
        return writeLocked(() -> {
            String normalizedPath = normalizePath(path);
            if (normalizedPath.equals("/")) {
                throw new IllegalStateException("refusing to delete namespace root");
            }

            FileManifest file = manifestsByPath.get(normalizedPath);
            if (file != null) {
                removeManifestPath(normalizedPath);
                return new PathDeleteResult(normalizedPath, 1);
            }

            List<String> childPaths = manifestPathsUnder(normalizedPath);
            if (childPaths.isEmpty()) {
                throw new IllegalArgumentException("unknown path: " + normalizedPath);
            }
            if (!recursive) {
                throw new IllegalStateException("directory delete requires recursive=true: " + normalizedPath);
            }

            Set<ContentHash> affectedHashes = new LinkedHashSet<>();
            for (String childPath : childPaths) {
                FileManifest removed = manifestsByPath.remove(childPath);
                modifiedAtByPath.remove(childPath);
                if (removed != null) {
                    affectedHashes.add(removed.fileHash());
                }
            }
            affectedHashes.forEach(this::refreshManifestHashIndex);
            return new PathDeleteResult(normalizedPath, childPaths.size());
        });
    }

    @Override
    public PathRenameResult renamePath(String sourcePath, String targetPath) {
        return writeLocked(() -> {
            String normalizedSource = normalizePath(sourcePath);
            String normalizedTarget = normalizePath(targetPath);
            if (normalizedSource.equals("/")) {
                throw new IllegalStateException("refusing to rename namespace root");
            }
            if (normalizedTarget.equals("/")) {
                throw new IllegalStateException("rename target cannot be namespace root");
            }

            Optional<FileMetadata> existingSource = stat(normalizedSource);
            if (existingSource.isEmpty()) {
                throw new IllegalArgumentException("unknown path: " + normalizedSource);
            }
            if (normalizedSource.equals(normalizedTarget)) {
                List<FileManifest> manifests = manifestsForSource(normalizedSource);
                return new PathRenameResult(
                        normalizedSource,
                        normalizedTarget,
                        existingSource.orElseThrow(),
                        manifests
                );
            }
            if (isPathUnder(normalizedSource, normalizedTarget)) {
                throw new IllegalStateException("cannot rename a directory into itself: " + normalizedSource);
            }
            regularFileAncestor(normalizedTarget).ifPresent(ancestor -> {
                throw new IllegalStateException("target parent is a regular file: " + ancestor);
            });

            FileManifest sourceFile = manifestsByPath.get(normalizedSource);
            Optional<FileMetadata> existingTarget = stat(normalizedTarget);
            FileManifest replacedTarget = null;
            if (existingTarget.isPresent()) {
                replacedTarget = manifestsByPath.get(normalizedTarget);
                if (sourceFile == null || replacedTarget == null) {
                    throw new IllegalStateException("target path already exists: " + normalizedTarget);
                }
            }
            if (sourceFile != null) {
                FileManifest renamed = renameManifest(sourceFile, normalizedTarget);
                manifestsByPath.remove(normalizedSource);
                modifiedAtByPath.remove(normalizedSource);
                if (replacedTarget != null) {
                    manifestsByPath.remove(normalizedTarget);
                    modifiedAtByPath.remove(normalizedTarget);
                }
                manifestsByPath.put(normalizedTarget, renamed);
                modifiedAtByPath.put(normalizedTarget, Instant.now());
                refreshManifestHashIndex(sourceFile.fileHash());
                if (replacedTarget != null) {
                    refreshManifestHashIndex(replacedTarget.fileHash());
                }
                return new PathRenameResult(
                        normalizedSource,
                        normalizedTarget,
                        fileMetadata(renamed, nameOf(normalizedTarget)),
                        List.of(renamed)
                );
            }

            List<FileManifest> sourceChildren = manifestsByPath.values().stream()
                    .filter(manifest -> isPathUnder(normalizedSource, manifest.path()))
                    .sorted(Comparator.comparing(FileManifest::path))
                    .toList();
            if (sourceChildren.isEmpty()) {
                throw new IllegalArgumentException("unknown path: " + normalizedSource);
            }

            Set<ContentHash> affectedHashes = sourceChildren.stream()
                    .map(FileManifest::fileHash)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
            List<FileManifest> renamedChildren = new ArrayList<>();
            Instant modifiedAt = Instant.now();
            for (FileManifest manifest : sourceChildren) {
                manifestsByPath.remove(manifest.path());
                modifiedAtByPath.remove(manifest.path());
            }
            for (FileManifest manifest : sourceChildren) {
                String suffix = manifest.path().substring(normalizedSource.length());
                FileManifest renamed = renameManifest(manifest, normalizedTarget + suffix);
                manifestsByPath.put(renamed.path(), renamed);
                modifiedAtByPath.put(renamed.path(), modifiedAt);
                renamedChildren.add(renamed);
            }
            affectedHashes.forEach(this::refreshManifestHashIndex);
            return new PathRenameResult(
                    normalizedSource,
                    normalizedTarget,
                    directoryMetadata(normalizedTarget),
                    renamedChildren
            );
        });
    }

    @Override
    public void pruneExcept(Set<String> retainedPaths) {
        writeLocked(() -> {
            if (retainedPaths == null) {
                throw new IllegalArgumentException("retainedPaths is required");
            }
            Set<String> normalizedRetainedPaths = retainedPaths.stream()
                    .map(CxnfsRepository::normalizePath)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
            List<String> stalePaths = manifestsByPath.keySet().stream()
                    .filter(path -> !normalizedRetainedPaths.contains(path))
                    .toList();
            for (String path : stalePaths) {
                FileManifest removed = manifestsByPath.remove(path);
                modifiedAtByPath.remove(path);
                if (removed != null) {
                    refreshManifestHashIndex(removed.fileHash());
                }
            }
        });
    }

    @Override
    public DirectoryListing list(String path) {
        return readLocked(() -> {
            String normalizedPath = normalizeDirectoryPath(path);
            Set<FileMetadata> entries = new LinkedHashSet<>();
            for (FileManifest manifest : manifestsByPath.values()) {
                Optional<FileMetadata> entry = directChild(normalizedPath, manifest);
                entry.ifPresent(entries::add);
            }
            return new DirectoryListing(normalizedPath, entries.stream()
                    .sorted(Comparator.comparing(FileMetadata::type).thenComparing(FileMetadata::name))
                    .toList());
        });
    }

    @Override
    public Optional<FileMetadata> stat(String path) {
        return readLocked(() -> {
            String normalizedPath = normalizePath(path);
            FileManifest manifest = manifestsByPath.get(normalizedPath);
            if (manifest != null) {
                return Optional.of(fileMetadata(manifest, nameOf(normalizedPath)));
            }

            boolean hasChildren = manifestsByPath.keySet().stream()
                    .anyMatch(candidate -> normalizedPath.equals("/")
                            ? candidate.length() > 1
                            : candidate.startsWith(normalizedPath + "/"));
            if (!hasChildren) {
                return Optional.empty();
            }

            return Optional.of(new FileMetadata(
                    normalizedPath,
                    nameOf(normalizedPath),
                    FileType.DIRECTORY,
                    0,
                    Optional.empty(),
                    Instant.EPOCH
            ));
        });
    }

    @Override
    public Optional<FileManifest> manifest(ContentHash fileHash) {
        return readLocked(() -> Optional.ofNullable(manifests.get(fileHash)));
    }

    @Override
    public Optional<FileManifest> manifest(String path) {
        return readLocked(() -> Optional.ofNullable(manifestsByPath.get(normalizePath(path))));
    }

    @Override
    public List<ContentHash> missingChunks(List<ChunkRef> chunks) {
        return readLocked(() -> {
            if (chunks == null) {
                throw new IllegalArgumentException("chunks are required");
            }
            Set<ContentHash> missing = new LinkedHashSet<>();
            for (ChunkRef chunk : chunks) {
                if (!chunkIndex.containsKey(chunk.hash())) {
                    missing.add(chunk.hash());
                }
            }
            return List.copyOf(missing);
        });
    }

    @Override
    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        return readLocked(() -> {
            FileManifest manifest = manifests.get(fileHash);
            if (manifest == null) {
                throw new IllegalArgumentException("unknown file hash: " + fileHash);
            }
            return readPlanner.plan(manifest, offset, length);
        });
    }

    @Override
    public ReconstructionPlan plan(ContentHash fileHash) {
        return readLocked(() -> {
            FileManifest manifest = manifests.get(fileHash);
            if (manifest == null) {
                throw new IllegalArgumentException("unknown file hash: " + fileHash);
            }

            List<ReconstructionTerm> terms = new ArrayList<>();
            ContainerChunkLocation current = null;
            int currentCount = 0;

            for (ChunkRef chunk : manifest.chunks()) {
                ContainerChunkLocation location = locationFor(chunk.hash());
                if (current == null) {
                    current = location;
                    currentCount = 1;
                    continue;
                }
                boolean adjacent = current.containerHash().equals(location.containerHash())
                        && current.chunkIndex() + currentCount == location.chunkIndex();
                if (adjacent) {
                    currentCount++;
                } else {
                    terms.add(new ReconstructionTerm(current.containerHash(), current.chunkIndex(), currentCount));
                    current = location;
                    currentCount = 1;
                }
            }

            if (current != null) {
                terms.add(new ReconstructionTerm(current.containerHash(), current.chunkIndex(), currentCount));
            }

            return new ReconstructionPlan(fileHash, manifest.size(), terms);
        });
    }

    @Override
    public byte[] reconstruct(ContentHash fileHash) throws IOException {
        return readLockedIo(() -> {
            FileManifest manifest = manifests.get(fileHash);
            if (manifest == null) {
                throw new IllegalArgumentException("unknown file hash: " + fileHash);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(manifest.size()));
            for (ChunkRef ref : manifest.chunks()) {
                out.write(readVerifiedChunk(ref));
            }
            return out.toByteArray();
        });
    }

    @Override
    public byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        return readLockedIo(() -> {
            ReadPlan plan = readPlan(fileHash, offset, length);
            if (plan.requestedRange().isEmpty()) {
                return new byte[0];
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(plan.requestedRange().length()));
            for (PlannedChunkRead chunk : plan.chunks()) {
                byte[] data = readVerifiedChunk(new ChunkRef(chunk.chunkHash(), chunk.chunkSize()));
                out.write(data, chunk.offsetInChunk(), chunk.bytesToRead());
            }
            return out.toByteArray();
        });
    }

    @Override
    public void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer is required");
        }
        ReadPlan canonicalPlan = readLocked(() -> {
            FileManifest manifest = manifests.get(plan.fileHash());
            if (manifest == null) {
                throw new IllegalArgumentException("unknown file hash: " + plan.fileHash());
            }
            return readPlanner.plan(manifest, plan.requestedRange().offset(), plan.requestedRange().length());
        });
        if (!canonicalPlan.equals(plan)) {
            throw new IllegalArgumentException("read plan does not match repository manifest");
        }
        for (PlannedChunkRead chunk : plan.chunks()) {
            byte[] payload = readLockedIo(() -> readVerifiedChunk(new ChunkRef(chunk.chunkHash(), chunk.chunkSize())));
            consumer.accept(chunk, payload);
        }
    }

    public int containerCount() {
        return readLocked(() -> containers.size());
    }

    public int chunkCount() {
        return readLocked(() -> chunkIndex.size());
    }

    private ContainerChunkLocation locationFor(ContentHash chunkHash) {
        ContainerChunkLocation location = chunkIndex.get(chunkHash);
        if (location == null) {
            throw new IllegalStateException("manifest references missing chunk: " + chunkHash);
        }
        return location;
    }

    boolean containsChunk(ContentHash chunkHash) {
        return readLocked(() -> chunkIndex.containsKey(chunkHash));
    }

    private FileManifest publishManifestUncheckedLocked(FileManifest manifest) {
        String normalizedPath = normalizePath(manifest.path());
        requireWritableFilePath(normalizedPath);
        FileManifest normalizedManifest = new FileManifest(
                normalizedPath,
                manifest.fileHash(),
                manifest.size(),
                manifest.chunks()
        );
        manifests.put(normalizedManifest.fileHash(), normalizedManifest);
        manifestsByPath.put(normalizedPath, normalizedManifest);
        modifiedAtByPath.put(normalizedPath, Instant.now());
        return normalizedManifest;
    }

    private static void requireValidManifest(FileManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        ContentHash actualFileHash = Hashing.file(
                manifest.chunks().stream().map(ChunkRef::hash).toList(),
                manifest.chunks().stream().map(ChunkRef::size).toList()
        );
        if (!actualFileHash.equals(manifest.fileHash())) {
            throw new IllegalArgumentException("manifest file hash does not match chunk list");
        }
        long actualSize = manifest.chunks().stream().mapToLong(ChunkRef::size).sum();
        if (actualSize != manifest.size()) {
            throw new IllegalArgumentException("manifest size does not match chunk list");
        }
    }

    private void indexChunkIfMissing(Chunk chunk) throws IOException {
        if (chunkIndex.containsKey(chunk.hash())) {
            return;
        }
        Container container = containerBuilder.build(List.of(chunk));
        containers.put(container.hash(), container);
        chunkIndex.put(chunk.hash(), new ContainerChunkLocation(container.hash(), 0));
    }

    private Optional<FileManifest> commitCopyOnWritePatch(StagedWrite stagedWrite) throws IOException {
        if (stagedWrite.mode() != WriteMode.COPY_ON_WRITE) {
            return Optional.empty();
        }
        String normalizedPath = normalizePath(stagedWrite.path());
        FileManifest current = manifestsByPath.get(normalizedPath);
        if (current == null) {
            return Optional.empty();
        }
        long targetSize = stagedWrite.declaredSize();
        if (current.size() == targetSize && stagedWrite.extents().isEmpty()) {
            return Optional.of(current);
        }

        List<WriteExtent> relevantExtents = relevantExtents(stagedWrite.extents(), targetSize);

        List<ChunkSpan> spans = chunkSpans(current);
        List<ChunkRange> dirtyRanges = dirtyChunkRanges(spans, relevantExtents, targetSize);
        boolean appendsBytes = targetSize > current.size();
        if (dirtyRanges.isEmpty() && !appendsBytes && current.size() == targetSize) {
            return Optional.of(current);
        }
        if (dirtyRanges.stream().anyMatch(range -> patchWindowLength(spans, range, targetSize) > Integer.MAX_VALUE)) {
            return Optional.empty();
        }
        long appendLength = appendsBytes ? targetSize - current.size() : 0;
        if (appendLength > Integer.MAX_VALUE) {
            return Optional.empty();
        }

        List<ChunkReplacement> replacements = new ArrayList<>(dirtyRanges.size());
        for (ChunkRange dirtyRange : dirtyRanges) {
            byte[] patchedWindow = readPatchedWindow(current, spans, dirtyRange, relevantExtents, targetSize);
            long windowOffset = spans.get(dirtyRange.startIndex()).offset();
            List<Chunk> replacementChunks = new ArrayList<>();
            for (Chunk chunk : chunker.split(patchedWindow)) {
                replacementChunks.add(new Chunk(
                        chunk.hash(),
                        windowOffset + chunk.sourceOffset(),
                        chunk.data()
                ));
            }
            replacements.add(new ChunkReplacement(dirtyRange, replacementChunks));
        }

        List<Chunk> appendedChunks = appendsBytes
                ? appendChunks(current.size(), targetSize, relevantExtents)
                : List.of();
        List<ChunkRef> refs = new ArrayList<>(patchedRefs(current, spans, replacements, targetSize));
        for (Chunk chunk : appendedChunks) {
            refs.add(new ChunkRef(chunk.hash(), chunk.size()));
        }
        long patchedSize = refs.stream().mapToLong(ChunkRef::size).sum();
        if (patchedSize != targetSize) {
            return Optional.empty();
        }
        for (ChunkReplacement replacement : replacements) {
            for (Chunk chunk : replacement.chunks()) {
                indexChunkIfMissing(chunk);
            }
        }
        for (Chunk chunk : appendedChunks) {
            indexChunkIfMissing(chunk);
        }

        ContentHash fileHash = Hashing.file(
                refs.stream().map(ChunkRef::hash).toList(),
                refs.stream().map(ChunkRef::size).toList()
        );
        FileManifest manifest = new FileManifest(normalizedPath, fileHash, targetSize, refs);
        manifests.put(fileHash, manifest);
        manifestsByPath.put(normalizedPath, manifest);
        modifiedAtByPath.put(normalizedPath, Instant.now());
        return Optional.of(manifest);
    }

    private static List<WriteExtent> relevantExtents(List<WriteExtent> extents, long fileSize) {
        return extents.stream()
                .filter(extent -> extent.size() > 0)
                .filter(extent -> extent.offset() < fileSize)
                .filter(extent -> Math.min(fileSize, extentEndExclusive(extent)) > extent.offset())
                .sorted(Comparator.comparingLong(WriteExtent::offset))
                .toList();
    }

    private static List<ChunkRange> dirtyChunkRanges(
            List<ChunkSpan> spans,
            List<WriteExtent> extents,
            long targetSize
    ) {
        boolean[] dirty = new boolean[spans.size()];
        for (WriteExtent extent : extents) {
            long extentEnd = Math.min(targetSize, extentEndExclusive(extent));
            for (int index = 0; index < spans.size(); index++) {
                ChunkSpan span = spans.get(index);
                if (span.offset() >= targetSize) {
                    continue;
                }
                if (span.offset() < extentEnd && span.endExclusive() > extent.offset()) {
                    dirty[index] = true;
                }
            }
        }
        if (targetSize > 0) {
            for (int index = 0; index < spans.size(); index++) {
                ChunkSpan span = spans.get(index);
                if (span.offset() < targetSize && targetSize < span.endExclusive()) {
                    dirty[index] = true;
                    break;
                }
            }
        }

        List<ChunkRange> ranges = new ArrayList<>();
        int index = 0;
        while (index < dirty.length) {
            if (!dirty[index]) {
                index++;
                continue;
            }
            int start = index;
            while (index < dirty.length && dirty[index]) {
                index++;
            }
            ranges.add(new ChunkRange(start, index));
        }
        return List.copyOf(ranges);
    }

    private static List<ChunkSpan> chunkSpans(FileManifest manifest) {
        List<ChunkSpan> spans = new ArrayList<>(manifest.chunks().size());
        long offset = 0;
        for (ChunkRef chunk : manifest.chunks()) {
            spans.add(new ChunkSpan(offset, chunk.size()));
            offset += chunk.size();
        }
        return spans;
    }

    private byte[] readPatchedWindow(
            FileManifest manifest,
            List<ChunkSpan> spans,
            ChunkRange dirtyRange,
            List<WriteExtent> extents,
            long targetSize
    ) throws IOException {
        long windowOffset = spans.get(dirtyRange.startIndex()).offset();
        long windowEnd = Math.min(spans.get(dirtyRange.endIndexExclusive() - 1).endExclusive(), targetSize);
        int windowSize = Math.toIntExact(patchWindowLength(spans, dirtyRange, targetSize));
        ByteArrayOutputStream out = new ByteArrayOutputStream(windowSize);
        for (int index = dirtyRange.startIndex(); index < dirtyRange.endIndexExclusive(); index++) {
            ChunkSpan span = spans.get(index);
            long chunkBytes = Math.min(span.endExclusive(), targetSize) - span.offset();
            if (chunkBytes > 0) {
                out.write(readVerifiedChunk(manifest.chunks().get(index)), 0, Math.toIntExact(chunkBytes));
            }
        }
        byte[] window = out.toByteArray();
        if (window.length != windowSize) {
            throw new IOException("patched chunk window size mismatch for " + manifest.path());
        }
        for (WriteExtent extent : extents) {
            applyExtentToWindow(window, windowOffset, windowEnd, extent);
        }
        return window;
    }

    private static void applyExtentToWindow(byte[] window, long windowOffset, long windowEnd, WriteExtent extent) {
        long extentStart = extent.offset();
        long extentEnd = extentEndExclusive(extent);
        long copyStart = Math.max(windowOffset, extentStart);
        long copyEnd = Math.min(windowEnd, extentEnd);
        if (copyEnd <= copyStart) {
            return;
        }
        byte[] data = extent.data();
        System.arraycopy(
                data,
                Math.toIntExact(copyStart - extentStart),
                window,
                Math.toIntExact(copyStart - windowOffset),
                Math.toIntExact(copyEnd - copyStart)
        );
    }

    private List<Chunk> appendChunks(long appendOffset, long targetSize, List<WriteExtent> extents) {
        int appendLength = Math.toIntExact(targetSize - appendOffset);
        byte[] window = new byte[appendLength];
        for (WriteExtent extent : extents) {
            applyExtentToWindow(window, appendOffset, targetSize, extent);
        }
        List<Chunk> chunks = new ArrayList<>();
        for (Chunk chunk : chunker.split(window)) {
            chunks.add(new Chunk(
                    chunk.hash(),
                    appendOffset + chunk.sourceOffset(),
                    chunk.data()
            ));
        }
        return List.copyOf(chunks);
    }

    private static List<ChunkRef> patchedRefs(
            FileManifest current,
            List<ChunkSpan> spans,
            List<ChunkReplacement> replacements,
            long targetSize
    ) {
        List<ChunkRef> refs = new ArrayList<>();
        int cursor = 0;
        for (ChunkReplacement replacement : replacements) {
            while (cursor < replacement.range().startIndex() && spans.get(cursor).endExclusive() <= targetSize) {
                refs.add(current.chunks().get(cursor));
                cursor++;
            }
            for (Chunk chunk : replacement.chunks()) {
                refs.add(new ChunkRef(chunk.hash(), chunk.size()));
            }
            cursor = replacement.range().endIndexExclusive();
        }
        while (cursor < current.chunks().size() && spans.get(cursor).endExclusive() <= targetSize) {
            refs.add(current.chunks().get(cursor));
            cursor++;
        }
        return List.copyOf(refs);
    }

    private static long extentEndExclusive(WriteExtent extent) {
        long end = extent.endExclusive();
        return end < extent.offset() ? Long.MAX_VALUE : end;
    }

    private static long patchWindowLength(List<ChunkSpan> spans, ChunkRange dirtyRange, long targetSize) {
        return Math.min(spans.get(dirtyRange.endIndexExclusive() - 1).endExclusive(), targetSize)
                - spans.get(dirtyRange.startIndex()).offset();
    }

    private byte[] readVerifiedChunk(ChunkRef ref) throws IOException {
        ContainerChunkLocation location = locationFor(ref.hash());
        Container container = containers.get(location.containerHash());
        StoredChunk storedChunk = container.chunkAt(location.chunkIndex());
        byte[] data = codecRegistry.require(storedChunk.compressionType())
                .decompress(storedChunk.payload(), storedChunk.originalSize());
        if (data.length != ref.size()) {
            throw new IOException("chunk size verification failed: " + ref.hash());
        }
        if (!Hashing.chunk(data).equals(ref.hash())) {
            throw new IOException("chunk hash verification failed: " + ref.hash());
        }
        return data;
    }

    private void removeManifestPath(String path) {
        FileManifest removed = manifestsByPath.remove(path);
        modifiedAtByPath.remove(path);
        if (removed != null) {
            refreshManifestHashIndex(removed.fileHash());
        }
    }

    private void refreshManifestHashIndex(ContentHash fileHash) {
        Optional<FileManifest> replacement = manifestsByPath.values().stream()
                .filter(manifest -> manifest.fileHash().equals(fileHash))
                .findFirst();
        if (replacement.isPresent()) {
            manifests.put(fileHash, replacement.orElseThrow());
        } else {
            manifests.remove(fileHash);
        }
    }

    private List<String> manifestPathsUnder(String directory) {
        return manifestsByPath.keySet().stream()
                .filter(candidate -> isPathUnder(directory, candidate))
                .sorted()
                .toList();
    }

    private List<FileManifest> manifestsForSource(String path) {
        FileManifest file = manifestsByPath.get(path);
        if (file != null) {
            return List.of(file);
        }
        return manifestsByPath.values().stream()
                .filter(manifest -> isPathUnder(path, manifest.path()))
                .sorted(Comparator.comparing(FileManifest::path))
                .toList();
    }

    private FileManifest renameManifest(FileManifest manifest, String targetPath) {
        return new FileManifest(
                normalizePath(targetPath),
                manifest.fileHash(),
                manifest.size(),
                manifest.chunks()
        );
    }

    private FileMetadata directoryMetadata(String path) {
        return new FileMetadata(
                path,
                nameOf(path),
                FileType.DIRECTORY,
                0,
                Optional.empty(),
                Instant.EPOCH
        );
    }

    private void requireWritableFilePath(String normalizedPath) {
        regularFileAncestor(normalizedPath).ifPresent(ancestor -> {
            throw new IllegalStateException("parent path is a regular file: " + ancestor);
        });
        boolean hasChildren = manifestsByPath.keySet().stream()
                .anyMatch(candidate -> isPathUnder(normalizedPath, candidate));
        if (hasChildren) {
            throw new IllegalStateException("target path is a directory: " + normalizedPath);
        }
    }

    private Optional<FileMetadata> directChild(String directory, FileManifest manifest) {
        String relative = relativeTo(directory, manifest.path());
        if (relative == null || relative.isEmpty()) {
            return Optional.empty();
        }

        int slash = relative.indexOf('/');
        if (slash >= 0) {
            String name = relative.substring(0, slash);
            String childPath = childPath(directory, name);
            return Optional.of(new FileMetadata(
                    childPath,
                    name,
                    FileType.DIRECTORY,
                    0,
                    Optional.empty(),
                    Instant.EPOCH
            ));
        }

        return Optional.of(fileMetadata(manifest, relative));
    }

    private FileMetadata fileMetadata(FileManifest manifest, String name) {
        return new FileMetadata(
                manifest.path(),
                name,
                FileType.REGULAR_FILE,
                manifest.size(),
                Optional.of(manifest.fileHash()),
                modifiedAtByPath.getOrDefault(manifest.path(), Instant.EPOCH)
        );
    }

    private <T> T readLocked(Supplier<T> supplier) {
        lock.readLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.readLock().unlock();
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

    private <T> T writeLocked(Supplier<T> supplier) {
        lock.writeLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeLocked(Runnable runnable) {
        lock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private <T> T writeLockedIo(IoSupplier<T> supplier) throws IOException {
        lock.writeLock().lock();
        try {
            return supplier.get();
        } finally {
            lock.writeLock().unlock();
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

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }

    private static String normalizeDirectoryPath(String path) {
        return NamespacePath.normalizeDirectory(path);
    }

    private static String relativeTo(String directory, String filePath) {
        if (directory.equals("/")) {
            return filePath.substring(1);
        }
        String prefix = directory + "/";
        return filePath.startsWith(prefix) ? filePath.substring(prefix.length()) : null;
    }

    private static boolean isPathUnder(String directory, String candidate) {
        return directory.equals("/")
                ? candidate.length() > 1
                : candidate.startsWith(directory + "/");
    }

    private Optional<String> regularFileAncestor(String path) {
        int slash = path.lastIndexOf('/');
        while (slash > 0) {
            String ancestor = path.substring(0, slash);
            if (manifestsByPath.containsKey(ancestor)) {
                return Optional.of(ancestor);
            }
            slash = ancestor.lastIndexOf('/');
        }
        return Optional.empty();
    }

    private static String childPath(String directory, String name) {
        return directory.equals("/") ? "/" + name : directory + "/" + name;
    }

    private static String nameOf(String path) {
        if (path.equals("/")) {
            return "";
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }

    @FunctionalInterface
    private interface IoRunnable {
        void run() throws IOException;
    }

    private record ChunkSpan(long offset, int size) {
        private long endExclusive() {
            return offset + size;
        }
    }

    private record ChunkRange(int startIndex, int endIndexExclusive) {
    }

    private record ChunkReplacement(ChunkRange range, List<Chunk> chunks) {
    }
}
