package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.performance.ReadAheadPlan;
import com.fortrow.cxnfs.performance.ReadAheadPlanner;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.transport.CordTransferPlan;
import com.fortrow.cxnfs.transport.CordTransferPlanner;

import java.nio.file.Path;
import java.util.List;

public final class EarlyReadStoragePlanner {
    private final ReadAheadPlanner readAheadPlanner = new ReadAheadPlanner();

    public EarlyReadStoragePlan plan(
            FileManifest manifest,
            ReconstructionPlan reconstructionPlan,
            long offset,
            long length,
            EarlyReadStoragePolicy policy
    ) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        if (reconstructionPlan == null) {
            throw new IllegalArgumentException("reconstructionPlan is required");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        if (!manifest.fileHash().equals(reconstructionPlan.fileHash())) {
            throw new IllegalArgumentException("manifest and reconstruction plan must describe the same file");
        }

        ReadAheadPlan readAheadPlan = readAheadPlanner.plan(manifest, offset, length, policy.readAheadPolicy());
        CordTransferPlan cordTransferPlan = new CordTransferPlanner(policy.cordPool()).plan(reconstructionPlan);
        List<EarlyReadPayloadExtent> payloadExtents = readAheadPlan.prefetch().chunks().stream()
                .map(chunk -> extent(chunk, policy))
                .toList();

        return new EarlyReadStoragePlan(
                manifest.path(),
                readAheadPlan,
                cordTransferPlan,
                payloadExtents,
                bootstrapStructures(policy, manifest.path()),
                trailingStructures(policy)
        );
    }

    private static EarlyReadPayloadExtent extent(PlannedChunkRead chunk, EarlyReadStoragePolicy policy) {
        return new EarlyReadPayloadExtent(
                chunk.chunkIndex(),
                chunk.readFileOffset(),
                chunk.bytesToRead(),
                policy.allowPayloadBeforeTrailingMetadata(),
                policy.requireTrailingMetadataBeforePublish()
        );
    }

    private static List<FilesystemStructureRef> bootstrapStructures(EarlyReadStoragePolicy policy, String path) {
        Path root = policy.osStructureRoot();
        return List.of(
                before(FilesystemStructureRole.AUTHORIZATION_GRANT, root.resolve("auth/grants.cxn")),
                before(FilesystemStructureRole.NAMESPACE_SNAPSHOT, root.resolve("namespace/current.cxn")),
                before(FilesystemStructureRole.FILE_EXTENT_HINTS, fileSidecar(root, path, "extent-hints.cxn"))
        );
    }

    private static List<FilesystemStructureRef> trailingStructures(EarlyReadStoragePolicy policy) {
        Path root = policy.osStructureRoot();
        return List.of(
                trailing(FilesystemStructureRole.MANIFEST_INDEX, root.resolve("manifests/index.cxn")),
                trailing(FilesystemStructureRole.CHUNK_HASH_INDEX, root.resolve("chunks/hash-index.cxn")),
                trailing(FilesystemStructureRole.CACHE_INDEX, root.resolve("cache/index.cxn")),
                trailing(FilesystemStructureRole.JOURNAL, root.resolve("journals/current.cxn")),
                trailing(FilesystemStructureRole.FREE_SPACE_MAP, root.resolve("allocation/free-space.cxn"))
        );
    }

    private static FilesystemStructureRef before(FilesystemStructureRole role, Path path) {
        return new FilesystemStructureRef(role, path, FilesystemStructureTiming.BEFORE_PAYLOAD);
    }

    private static FilesystemStructureRef trailing(FilesystemStructureRole role, Path path) {
        return new FilesystemStructureRef(role, path, FilesystemStructureTiming.TRAILING);
    }

    private static Path fileSidecar(Path root, String path, String fileName) {
        String relative = path.startsWith("/") ? path.substring(1) : path;
        if (relative.isBlank() || relative.contains("..") || relative.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("file path cannot be mapped to an OS metadata sidecar: " + path);
        }
        Path sidecar = root.resolve("files");
        for (String segment : relative.split("/")) {
            if (segment.isBlank()) {
                throw new IllegalArgumentException("file path contains an empty segment: " + path);
            }
            sidecar = sidecar.resolve(segment);
        }
        return sidecar.resolve(fileName);
    }
}
