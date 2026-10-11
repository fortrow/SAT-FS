package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.ReadPlanner;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import com.fortrow.cxnfs.store.CxnfsStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class MountedBlockfsStore implements CxnfsStore {
    private final Path mountRoot;
    private final ReadPlanner readPlanner = new ReadPlanner();
    private final Map<ContentHash, FileManifest> manifestsByHash = new LinkedHashMap<>();
    private final Map<String, FileManifest> manifestsByPath = new LinkedHashMap<>();
    private final Map<ContentHash, FileManifest> manifestsBySyntheticContainer = new LinkedHashMap<>();
    private final Map<String, Instant> modifiedAtByPath = new LinkedHashMap<>();

    public MountedBlockfsStore(
            Path mountRoot,
            LinuxBlockfsNamespaceSnapshot namespace,
            LinuxBlockfsManifestIndexSnapshot manifestIndex
    ) throws IOException {
        if (mountRoot == null) {
            throw new IllegalArgumentException("mountRoot is required");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("namespace is required");
        }
        if (manifestIndex == null) {
            throw new IllegalArgumentException("manifestIndex is required");
        }
        this.mountRoot = mountRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(this.mountRoot)) {
            throw new IllegalArgumentException("mount root does not exist: " + mountRoot);
        }
        new LinuxBlockfsMetadataAuditor().audit(
                namespace,
                Optional.of(manifestIndex),
                Optional.empty()
        );
        for (LinuxBlockfsManifestEntry entry : manifestIndex.entries()) {
            FileManifest manifest = toManifest(entry);
            Path mountedPath = resolveMountedPath(manifest.path());
            if (!Files.isRegularFile(mountedPath)) {
                throw new IllegalArgumentException("mounted blockfs file is missing: " + mountedPath);
            }
            long mountedSize = Files.size(mountedPath);
            if (mountedSize != manifest.size()) {
                throw new IllegalArgumentException("mounted blockfs file size mismatch: " + mountedPath);
            }
            manifestsByHash.put(manifest.fileHash(), manifest);
            manifestsByPath.put(manifest.path(), manifest);
            manifestsBySyntheticContainer.put(syntheticContainerHash(manifest), manifest);
            modifiedAtByPath.put(manifest.path(), Files.getLastModifiedTime(mountedPath).toInstant());
        }
    }

    public static MountedBlockfsStore load(Path mountRoot, Path metadataRoot) throws IOException {
        LinuxBlockfsNamespaceSnapshot namespace = new LinuxBlockfsNamespaceStore().load(metadataRoot);
        LinuxBlockfsManifestIndexSnapshot manifestIndex = new LinuxBlockfsManifestIndexStore().load(metadataRoot);
        return new MountedBlockfsStore(mountRoot, namespace, manifestIndex);
    }

    @Override
    public FileManifest put(String path, byte[] data) {
        throw new UnsupportedOperationException("mounted blockfs store is read-only");
    }

    @Override
    public FileManifest commit(StagedWrite stagedWrite) {
        throw new UnsupportedOperationException("mounted blockfs store is read-only");
    }

    @Override
    public void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) {
        throw new UnsupportedOperationException("mounted blockfs store is read-only");
    }

    @Override
    public FileManifest publishManifest(FileManifest manifest) {
        throw new UnsupportedOperationException("mounted blockfs store is read-only");
    }

    @Override
    public DirectoryListing list(String path) {
        String normalizedPath = normalizeDirectoryPath(path);
        Set<FileMetadata> entries = new LinkedHashSet<>();
        for (FileManifest manifest : manifestsByPath.values()) {
            directChild(normalizedPath, manifest).ifPresent(entries::add);
        }
        return new DirectoryListing(normalizedPath, entries.stream()
                .sorted(Comparator.comparing(FileMetadata::type).thenComparing(FileMetadata::name))
                .toList());
    }

    @Override
    public Optional<FileMetadata> stat(String path) {
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
    }

    @Override
    public Optional<FileManifest> manifest(ContentHash fileHash) {
        return Optional.ofNullable(manifestsByHash.get(fileHash));
    }

    @Override
    public Optional<FileManifest> manifest(String path) {
        return Optional.ofNullable(manifestsByPath.get(normalizePath(path)));
    }

    @Override
    public List<ContentHash> missingChunks(List<ChunkRef> chunks) {
        throw new UnsupportedOperationException("mounted blockfs store is read-only");
    }

    @Override
    public ReconstructionPlan plan(ContentHash fileHash) {
        FileManifest manifest = requireManifest(fileHash);
        if (manifest.chunks().isEmpty()) {
            return new ReconstructionPlan(fileHash, manifest.size(), List.of());
        }
        return new ReconstructionPlan(
                fileHash,
                manifest.size(),
                List.of(new ReconstructionTerm(
                        syntheticContainerHash(manifest),
                        0,
                        manifest.chunks().size()
                ))
        );
    }

    @Override
    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        FileManifest manifest = requireManifest(fileHash);
        return readPlanner.plan(manifest, offset, length);
    }

    @Override
    public byte[] reconstruct(ContentHash fileHash) throws IOException {
        FileManifest manifest = requireManifest(fileHash);
        Path mountedPath = resolveMountedPath(manifest.path());
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(manifest.size()));
        try (InputStream in = Files.newInputStream(mountedPath)) {
            List<ContentHash> chunkHashes = new ArrayList<>(manifest.chunks().size());
            List<Integer> chunkSizes = new ArrayList<>(manifest.chunks().size());
            for (ChunkRef chunk : manifest.chunks()) {
                byte[] data = in.readNBytes(chunk.size());
                if (data.length != chunk.size()) {
                    throw new IOException("mounted blockfs file ended before manifest chunk completed: "
                            + manifest.path());
                }
                ContentHash actualHash = Hashing.chunk(data);
                if (!actualHash.equals(chunk.hash())) {
                    throw new IOException("mounted blockfs chunk hash verification failed: " + manifest.path());
                }
                out.write(data);
                chunkHashes.add(actualHash);
                chunkSizes.add(data.length);
            }
            if (in.read() != -1) {
                throw new IOException("mounted blockfs file has trailing bytes: " + manifest.path());
            }
            ContentHash actualFileHash = Hashing.file(chunkHashes, chunkSizes);
            if (!actualFileHash.equals(fileHash)) {
                throw new IOException("mounted blockfs file hash verification failed: " + manifest.path());
            }
        }
        return out.toByteArray();
    }

    @Override
    public byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        ReadPlan plan = readPlan(fileHash, offset, length);
        if (plan.requestedRange().isEmpty()) {
            return new byte[0];
        }
        FileManifest manifest = requireManifest(fileHash);
        Path mountedPath = resolveMountedPath(manifest.path());
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.toIntExact(plan.requestedRange().length()));
        try (SeekableByteChannel channel = Files.newByteChannel(mountedPath, StandardOpenOption.READ)) {
            for (var chunkRead : plan.chunks()) {
                byte[] chunk = readChunk(channel, chunkRead.fileOffset(), chunkRead.chunkSize());
                ContentHash actualHash = Hashing.chunk(chunk);
                if (!actualHash.equals(chunkRead.chunkHash())) {
                    throw new IOException("mounted blockfs chunk hash verification failed: " + manifest.path());
                }
                out.write(chunk, chunkRead.offsetInChunk(), chunkRead.bytesToRead());
            }
        }
        return out.toByteArray();
    }

    @Override
    public void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer is required");
        }
        FileManifest manifest = requireManifest(plan.fileHash());
        ReadPlan canonicalPlan = readPlanner.plan(
                manifest,
                plan.requestedRange().offset(),
                plan.requestedRange().length()
        );
        if (!canonicalPlan.equals(plan)) {
            throw new IllegalArgumentException("read plan does not match mounted blockfs manifest");
        }
        Path mountedPath = resolveMountedPath(manifest.path());
        try (SeekableByteChannel channel = Files.newByteChannel(mountedPath, StandardOpenOption.READ)) {
            for (var chunkRead : plan.chunks()) {
                byte[] chunk = readChunk(channel, chunkRead.fileOffset(), chunkRead.chunkSize());
                CxnfsStore.requireVerifiedPlanChunk(chunkRead, chunk);
                consumer.accept(chunkRead, chunk);
            }
        }
    }

    public Path mountRoot() {
        return mountRoot;
    }

    public int fileCount() {
        return manifestsByPath.size();
    }

    public Optional<FileManifest> manifestForSyntheticContainer(ContentHash containerHash) {
        if (containerHash == null) {
            throw new IllegalArgumentException("containerHash is required");
        }
        return Optional.ofNullable(manifestsBySyntheticContainer.get(containerHash));
    }

    private FileManifest requireManifest(ContentHash fileHash) {
        FileManifest manifest = manifestsByHash.get(fileHash);
        if (manifest == null) {
            throw new IllegalArgumentException("unknown file hash: " + fileHash);
        }
        return manifest;
    }

    private Path resolveMountedPath(String normalizedPath) {
        String relativePath = normalizedPath.startsWith("/")
                ? normalizedPath.substring(1)
                : normalizedPath;
        Path resolved = mountRoot.resolve(relativePath).normalize();
        if (!resolved.startsWith(mountRoot)) {
            throw new IllegalArgumentException("path escapes mounted blockfs root: " + normalizedPath);
        }
        return resolved;
    }

    private static byte[] readChunk(SeekableByteChannel channel, long fileOffset, int chunkSize)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        channel.position(fileOffset);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("mounted blockfs file ended before manifest chunk completed");
            }
        }
        return buffer.array();
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

    private static FileManifest toManifest(LinuxBlockfsManifestEntry entry) {
        List<ChunkRef> chunks = entry.chunks().stream()
                .map(chunk -> new ChunkRef(chunk.chunkHash(), chunk.length()))
                .toList();
        return new FileManifest(normalizePath(entry.path()), entry.fileHash(), entry.size(), chunks);
    }

    private static ContentHash syntheticContainerHash(FileManifest manifest) {
        String descriptor = "cxnfs.mounted-blockfs.container.v1\n"
                + manifest.fileHash().hex() + "\n"
                + manifest.size() + "\n"
                + manifest.path();
        return Hashing.container(descriptor.getBytes(StandardCharsets.UTF_8));
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

    private static String childPath(String directory, String name) {
        return directory.equals("/") ? "/" + name : directory + "/" + name;
    }

    private static String nameOf(String path) {
        if (path.equals("/")) {
            return "";
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }
}
