package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.PlannedChunkConsumer;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface CxnfsStore {
    FileManifest put(String path, byte[] data) throws IOException;

    default FileManifest putStream(String path, InputStream data) throws IOException {
        if (data == null) {
            throw new IllegalArgumentException("data is required");
        }
        return put(path, data.readAllBytes());
    }

    FileManifest commit(StagedWrite stagedWrite) throws IOException;

    void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) throws IOException;

    FileManifest publishManifest(FileManifest manifest) throws IOException;

    default PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("store does not support path deletion");
    }

    default PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        throw new UnsupportedOperationException("store does not support path rename");
    }

    default void pruneExcept(Set<String> retainedPaths) throws IOException {
        if (retainedPaths == null) {
            throw new IllegalArgumentException("retainedPaths is required");
        }
    }

    DirectoryListing list(String path);

    Optional<FileMetadata> stat(String path);

    Optional<FileManifest> manifest(ContentHash fileHash);

    Optional<FileManifest> manifest(String path);

    List<ContentHash> missingChunks(List<ChunkRef> chunks);

    ReconstructionPlan plan(ContentHash fileHash);

    ReadPlan readPlan(ContentHash fileHash, long offset, long length);

    byte[] readRange(ContentHash fileHash, long offset, long length) throws IOException;

    default void readVerifiedPlanChunks(ReadPlan plan, PlannedChunkConsumer consumer) throws IOException {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer is required");
        }
        for (PlannedChunkRead chunk : plan.chunks()) {
            byte[] payload = readRange(plan.fileHash(), chunk.fileOffset(), chunk.chunkSize());
            requireVerifiedPlanChunk(chunk, payload);
            consumer.accept(chunk, payload);
        }
    }

    byte[] reconstruct(ContentHash fileHash) throws IOException;

    static void requireVerifiedPlanChunk(PlannedChunkRead chunk, byte[] payload) throws IOException {
        if (chunk == null) {
            throw new IllegalArgumentException("chunk is required");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        if (payload.length != chunk.chunkSize()) {
            throw new IOException("read plan chunk could not be materialized fully: " + chunk.chunkIndex());
        }
        if (!Hashing.chunk(payload).equals(chunk.chunkHash())) {
            throw new IOException("read plan chunk hash verification failed: " + chunk.chunkIndex());
        }
    }
}
