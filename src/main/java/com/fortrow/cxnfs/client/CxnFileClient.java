package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.server.CxnFileServer;
import com.fortrow.cxnfs.store.PathDeleteResult;
import com.fortrow.cxnfs.store.PathRenameResult;

import java.io.IOException;
import java.util.Optional;

public final class CxnFileClient {
    private final CxnFileServer server;

    public CxnFileClient(CxnFileServer server) {
        this.server = server;
    }

    public FileManifest put(String path, byte[] data) throws IOException {
        return server.put(path, data);
    }

    public FileManifest commit(StagedWrite stagedWrite) throws IOException {
        return server.commit(stagedWrite);
    }

    public PathDeleteResult deletePath(String path, boolean recursive) throws IOException {
        return server.deletePath(path, recursive);
    }

    public PathRenameResult renamePath(String sourcePath, String targetPath) throws IOException {
        return server.renamePath(sourcePath, targetPath);
    }

    public DirectoryListing list(String path) {
        return server.list(path);
    }

    public Optional<FileMetadata> stat(String path) {
        return server.stat(path);
    }

    public Optional<FileManifest> manifest(ContentHash fileHash) {
        return server.manifest(fileHash);
    }

    public Optional<FileManifest> manifest(String path) throws IOException {
        return server.manifest(path);
    }

    public ReconstructionPlan plan(ContentHash fileHash) {
        return server.reconstructionPlan(fileHash);
    }

    public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
        return server.readPlan(fileHash, offset, length);
    }

    public ReadPlan readPlan(String path, long offset, long length) throws IOException {
        return server.readPlan(path, offset, length);
    }

    public byte[] get(ContentHash fileHash) throws IOException {
        return server.read(fileHash);
    }

    public byte[] get(String path) throws IOException {
        return server.read(path);
    }

    public byte[] getRange(ContentHash fileHash, long offset, long length) throws IOException {
        return server.readRange(fileHash, offset, length);
    }

    public byte[] getRange(String path, long offset, long length) throws IOException {
        return server.readRange(path, offset, length);
    }
}
