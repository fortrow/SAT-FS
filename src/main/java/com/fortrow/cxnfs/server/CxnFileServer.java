package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.protocol.PutFileRequest;
import com.fortrow.cxnfs.protocol.PutFileResponse;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.store.CxnfsStore;
import com.fortrow.cxnfs.store.PathDeleteResult;
import com.fortrow.cxnfs.store.PathRenameResult;
import com.fortrow.cxnfs.transport.VerifiedEntryPublisher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

public final class CxnFileServer {
    private final CxnFileServerConfig config;
    private final CxnfsStore repository;
    private final LiveDirectoryRefresher liveDirectoryRefresher;

    public CxnFileServer(CxnFileServerConfig config, CxnfsStore repository) {
        this(config, repository, null);
    }

    public CxnFileServer(
            CxnFileServerConfig config,
            CxnfsStore repository,
            LiveDirectoryRefresher liveDirectoryRefresher
    ) {
        this.config = config;
        this.repository = repository;
        this.liveDirectoryRefresher = liveDirectoryRefresher;
    }

    public FileManifest put(String path, byte[] data) throws IOException {
        return repository.put(path, data);
    }

    public FileManifest putStream(String path, InputStream data) throws IOException {
        return repository.putStream(path, data);
    }

    public FileManifest commit(StagedWrite stagedWrite) throws IOException {
        return repository.commit(stagedWrite);
    }

    public void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
        repository.putVerifiedChunk(chunkHash, sourceOffset, data);
    }

    public FileManifest publishManifest(FileManifest manifest) throws IOException {
        FileManifest published = repository.publishManifest(manifest);
        if (liveDirectoryRefresher != null) {
            writeThroughSourceFile(published);
        }
        return published;
    }

    public PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null) {
            if (liveDirectoryRefresher.isCovered(path)) {
                return liveDirectoryRefresher.deletePath(path, recursive);
            }
        }
        return repository.deletePath(path, recursive);
    }

    public PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null) {
            if (liveDirectoryRefresher.isCovered(sourcePath) || liveDirectoryRefresher.isCovered(targetPath)) {
                if (!liveDirectoryRefresher.isCovered(sourcePath) || !liveDirectoryRefresher.isCovered(targetPath)) {
                    throw new IllegalArgumentException("cannot rename across served and repository namespaces");
                }
                return liveDirectoryRefresher.renamePath(sourcePath, targetPath);
            }
        }
        return repository.renamePath(sourcePath, targetPath);
    }

    public PutFileResponse preparePutFile(PutFileRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        long actualSize = request.chunks().stream()
                .mapToLong(chunk -> chunk.size())
                .sum();
        if (actualSize != request.size()) {
            throw new IllegalArgumentException("put file chunk sizes do not match declared file size");
        }
        ContentHash actualFileHash = Hashing.file(
                request.chunks().stream().map(chunk -> chunk.hash()).toList(),
                request.chunks().stream().map(chunk -> chunk.size()).toList()
        );
        if (!actualFileHash.equals(request.fileHash())) {
            throw new IllegalArgumentException("put file hash does not match chunk manifest");
        }
        FileManifest manifest = new FileManifest(
                normalizePath(request.path()),
                request.fileHash(),
                request.size(),
                request.chunks()
        );
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(request.path())) {
            return new PutFileResponse(manifest, request.chunks().stream().map(chunk -> chunk.hash()).toList());
        }
        List<ContentHash> missingChunks = repository.missingChunks(request.chunks());
        return new PutFileResponse(manifest, missingChunks);
    }

    public DirectoryListing list(String path) {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            return liveDirectoryRefresher.list(path);
        }
        return repository.list(path);
    }

    public Optional<FileMetadata> stat(String path) {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            return liveDirectoryRefresher.stat(path);
        }
        return repository.stat(path);
    }

    public Optional<FileManifest> manifest(ContentHash fileHash) {
        if (liveDirectoryRefresher != null) {
            Optional<FileManifest> liveManifest = liveDirectoryRefresher.manifest(fileHash);
            if (liveManifest.isPresent()) {
                return liveManifest;
            }
        }
        return repository.manifest(fileHash);
    }

    public Optional<FileManifest> manifest(String path) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            return liveDirectoryRefresher.manifest(path);
        }
        return repository.manifest(path);
    }

    public ReconstructionPlan reconstructionPlan(ContentHash fileHash) {
        if (liveDirectoryRefresher != null) {
            Optional<FileManifest> liveManifest = liveDirectoryRefresher.manifest(fileHash);
            if (liveManifest.isPresent()) {
                FileManifest manifest = liveManifest.orElseThrow();
                return new ReconstructionPlan(fileHash, manifest.size(), List.of());
            }
        }
        return repository.plan(fileHash);
    }

    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.manifest(fileHash).isPresent()) {
            return liveDirectoryRefresher.readPlan(fileHash, offset, length);
        }
        return repository.readPlan(fileHash, offset, length);
    }

    public ReadPlan readPlan(String path, long offset, long length) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            return liveDirectoryRefresher.readPlan(path, offset, length);
        }
        FileManifest manifest = requireManifest(path);
        return repository.readPlan(manifest.fileHash(), offset, length);
    }

    public byte[] read(ContentHash fileHash) throws IOException {
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.manifest(fileHash).isPresent()) {
            return liveDirectoryRefresher.reconstruct(fileHash);
        }
        return repository.reconstruct(fileHash);
    }

    public byte[] read(String path) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            FileManifest manifest = liveDirectoryRefresher.manifest(path)
                    .orElseThrow(() -> new IllegalArgumentException("unknown file path: " + path));
            return liveDirectoryRefresher.reconstruct(manifest.fileHash());
        }
        FileManifest manifest = requireManifest(path);
        return repository.reconstruct(manifest.fileHash());
    }

    public byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException {
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.manifest(fileHash).isPresent()) {
            return liveDirectoryRefresher.readRange(fileHash, offset, length);
        }
        return repository.readRange(fileHash, offset, length);
    }

    public void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.manifest(plan.fileHash()).isPresent()) {
            liveDirectoryRefresher.readVerifiedPlanChunks(plan, consumer);
            return;
        }
        repository.readVerifiedPlanChunks(plan, consumer);
    }

    public byte[] readRange(String path, long offset, long length) throws IOException {
        refreshServedDirectories();
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            ReadPlan plan = liveDirectoryRefresher.readPlan(path, offset, length);
            return liveDirectoryRefresher.readRange(plan.fileHash(), offset, length);
        }
        FileManifest manifest = requireManifest(path);
        return repository.readRange(manifest.fileHash(), offset, length);
    }

    public VerifiedEntryPublisher verifiedEntryPublisher(String path) throws IOException {
        return verifiedEntryPublisher(path, -1L);
    }

    public VerifiedEntryPublisher verifiedEntryPublisher(String path, long sizeHintBytes) throws IOException {
        if (liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path)) {
            return liveDirectoryRefresher.threadedPublisher(path, sizeHintBytes);
        }
        return new RepositoryVerifiedEntryPublisher();
    }

    public boolean isServedPath(String path) {
        return liveDirectoryRefresher != null && liveDirectoryRefresher.isCovered(path);
    }

    public CxnFileServerConfig config() {
        return config;
    }

    private FileManifest requireManifest(String path) {
        return repository.manifest(path)
                .orElseThrow(() -> new IllegalArgumentException("unknown file path: " + path));
    }

    private void refreshServedDirectories() {
        if (liveDirectoryRefresher == null) {
            return;
        }
        try {
            liveDirectoryRefresher.refreshIfDue();
        } catch (IOException e) {
            throw new RuntimeException("served directory refresh failed: " + e.getMessage(), e);
        }
    }

    private void writeThroughSourceFile(FileManifest manifest) throws IOException {
        Optional<Path> localPath = localPathFor(manifest.path());
        if (localPath.isEmpty()) {
            return;
        }
        Path path = localPath.orElseThrow();
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path temp = null;
        try {
            temp = Files.createTempFile(parent == null ? Path.of(".") : parent, ".cxnfs-write-", ".tmp");
            writeThroughSourceFile(manifest, path, temp);
        } catch (IOException e) {
            throw new IOException("served source write-through failed for "
                    + manifest.path()
                    + " at "
                    + path
                    + ": "
                    + e.getMessage(), e);
        } finally {
            if (temp != null) {
                Files.deleteIfExists(temp);
            }
        }
    }

    private void writeThroughSourceFile(FileManifest manifest, Path path, Path temp) throws IOException {
        ReadPlan plan = repository.readPlan(manifest.fileHash(), 0L, manifest.size());
        try (OutputStream out = Files.newOutputStream(temp)) {
            repository.readVerifiedPlanChunks(plan, (chunk, payload) ->
                    out.write(payload, chunk.offsetInChunk(), chunk.bytesToRead()));
        }
        try {
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Optional<Path> localPathFor(String namespacePath) {
        if (liveDirectoryRefresher != null) {
            return liveDirectoryRefresher.localPathFor(namespacePath);
        }
        String normalizedPath = normalizePath(namespacePath);
        for (ServedRoot root : config.servedRoots()) {
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

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }

    private final class RepositoryVerifiedEntryPublisher implements VerifiedEntryPublisher {
        @Override
        public void acceptChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException {
            repository.putVerifiedChunk(chunkHash, sourceOffset, data);
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) throws IOException {
            return CxnFileServer.this.publishManifest(manifest);
        }
    }
}
