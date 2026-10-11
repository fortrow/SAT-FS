package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.format.BinaryFormatException;
import com.fortrow.cxnfs.format.BinaryIO;
import com.fortrow.cxnfs.format.ManifestCodec;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.io.PlannedChunkRead;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class ControlPayloadCodec {
    public static final int VERSION = 1;
    public static final int MAX_TEXT_BYTES = 64 * 1024;
    public static final int MAX_AUTH_BYTES = 4 * 1024;
    public static final int MAX_LIST_ENTRIES = 100_000;
    public static final int MAX_CHUNK_REFS = 1_000_000;
    public static final int MAX_TERMS = 1_000_000;

    private static final byte[] MAGIC = "CXNFSPAY".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private ControlPayloadCodec() {
    }

    public static byte[] encode(ControlPayload payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            BinaryIO.writeMagic(out, MAGIC);
            out.writeInt(VERSION);
            out.writeInt(payload.operation().code());
            writePayload(out, payload);
        }
        return bytes.toByteArray();
    }

    public static ControlPayload decode(ControlOperation expectedOperation, byte[] bytes) throws IOException {
        if (expectedOperation == null) {
            throw new IllegalArgumentException("expectedOperation is required");
        }
        if (bytes == null) {
            throw new IllegalArgumentException("bytes is required");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (DataInputStream in = new DataInputStream(input)) {
            BinaryIO.requireMagic(in, MAGIC);
            BinaryIO.requireVersion(in.readInt(), VERSION);
            ControlOperation encodedOperation = ControlOperation.fromCode(in.readInt());
            if (encodedOperation != expectedOperation) {
                throw new BinaryFormatException("payload operation does not match frame operation");
            }
            ControlPayload payload = readPayload(in, expectedOperation);
            if (input.available() != 0) {
                throw new BinaryFormatException("trailing bytes after control payload");
            }
            return payload;
        }
    }

    private static void writePayload(DataOutputStream out, ControlPayload payload) throws IOException {
        if (payload instanceof AuthStart value) {
            out.writeInt(authMechanismCode(value.mechanism()));
            BinaryIO.writeUtf8(out, value.principal());
        } else if (payload instanceof AuthChallenge value) {
            writeUuid(out, value.challengeId());
            out.writeInt(authMechanismCode(value.mechanism()));
            BinaryIO.writeUtf8(out, value.principal());
            BinaryIO.writeBytes(out, value.nonce());
            BinaryIO.writeUtf8(out, value.proofAlgorithm());
            out.writeInt(value.iterations());
        } else if (payload instanceof AuthProof value) {
            writeUuid(out, value.challengeId());
            BinaryIO.writeUtf8(out, value.principal());
            BinaryIO.writeBytes(out, value.proof());
        } else if (payload instanceof AuthAccepted value) {
            writeUuid(out, value.sessionId());
            writeAuthMechanisms(out, value.mechanisms());
        } else if (payload instanceof DirectoryListRequest value) {
            BinaryIO.writeUtf8(out, value.path());
        } else if (payload instanceof DirectoryListResponse value) {
            writeDirectoryListing(out, value.listing());
        } else if (payload instanceof StatPathRequest value) {
            BinaryIO.writeUtf8(out, value.path());
        } else if (payload instanceof StatPathResponse value) {
            writeOptionalMetadata(out, value.metadata());
        } else if (payload instanceof PutFileRequest value) {
            BinaryIO.writeUtf8(out, value.path());
            out.writeLong(value.size());
            writeHash(out, value.fileHash());
            writeChunkRefs(out, value.chunks());
        } else if (payload instanceof PutFileResponse value) {
            writeManifest(out, value.manifest());
            writeHashes(out, value.missingChunks());
        } else if (payload instanceof ManifestRequest value) {
            writeHash(out, value.fileHash());
        } else if (payload instanceof ManifestResponse value) {
            writeOptionalManifest(out, value.manifest());
        } else if (payload instanceof ReconstructionPlanRequest value) {
            writeHash(out, value.fileHash());
        } else if (payload instanceof ReconstructionPlanResponse value) {
            writeReconstructionPlan(out, value.plan());
        } else if (payload instanceof ReadFileRangeRequest value) {
            BinaryIO.writeUtf8(out, value.path());
            out.writeLong(value.offset());
            out.writeLong(value.length());
        } else if (payload instanceof ReadFileRangeResponse value) {
            BinaryIO.writeUtf8(out, value.path());
            writeReadPlan(out, value.plan());
        } else if (payload instanceof ReadContainerRangeRequest value) {
            writeHash(out, value.containerHash());
            out.writeInt(value.firstChunkIndex());
            out.writeInt(value.chunkCount());
        } else if (payload instanceof RenamePathRequest value) {
            BinaryIO.writeUtf8(out, value.sourcePath());
            BinaryIO.writeUtf8(out, value.targetPath());
        } else if (payload instanceof DeletePathRequest value) {
            BinaryIO.writeUtf8(out, value.path());
            out.writeBoolean(value.recursive());
        } else if (payload instanceof RenamePathResponse value) {
            BinaryIO.writeUtf8(out, value.sourcePath());
            BinaryIO.writeUtf8(out, value.targetPath());
            writeMetadata(out, value.metadata());
        } else if (payload instanceof DeletePathResponse value) {
            BinaryIO.writeUtf8(out, value.path());
            out.writeLong(value.deletedEntries());
        } else if (payload instanceof MountNegotiateRequest value) {
            out.writeInt(mountBackendCode(value.backend()));
            writeMountCapabilities(out, value.requestedCapabilities());
        } else if (payload instanceof MountNegotiatedResponse value) {
            writeMountStrategy(out, value.strategy());
        } else if (payload instanceof TransferBootstrapRequest value) {
            writeUuid(out, value.transferId());
            out.writeInt(transferDirectionCode(value.direction()));
            BinaryIO.writeUtf8(out, value.path());
            out.writeInt(transferPayloadFormatCode(value.payloadFormat()));
            out.writeInt(value.initialCords());
            out.writeInt(value.maxCords());
            out.writeInt(value.provisionalContainerSizeBytes());
            out.writeLong(value.expectedSizeLimitBytes());
        } else if (payload instanceof TransferAcceptedResponse value) {
            writeUuid(out, value.transferId());
            out.writeInt(transferDirectionCode(value.direction()));
            out.writeInt(value.acceptedInitialCords());
            out.writeInt(value.acceptedMaxCords());
            out.writeLong(value.flowControlWindowBytes());
            out.writeBoolean(value.writePayloadAdmissionGranted());
        } else if (payload instanceof TransferMetadataUpdate value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            out.writeInt(value.metadataSequence());
            out.writeBoolean(value.finalMetadata());
            out.writeLong(value.bytesCovered());
            writeHashes(out, value.chunkHashes());
        } else if (payload instanceof TransferMetadataAcceptedResponse value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            out.writeInt(value.metadataSequence());
            out.writeInt(value.chunkHashesAccepted());
            out.writeInt(value.stagedPayloadsVerified());
            out.writeBoolean(value.finalMetadata());
        } else if (payload instanceof TransferCommitRequest value) {
            writeUuid(out, value.transferId());
            BinaryIO.writeUtf8(out, value.path());
            writeHash(out, value.contentHash());
            out.writeLong(value.bytesWritten());
        } else if (payload instanceof TransferCommittedResponse value) {
            writeUuid(out, value.transferId());
            writeHash(out, value.contentHash());
            out.writeLong(value.bytesWritten());
        } else if (payload instanceof TransferEntryUpdate value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            out.writeInt(transferEntryActionCode(value.action()));
            BinaryIO.writeUtf8(out, value.path());
            BinaryIO.writeUtf8(out, value.targetPath());
            out.writeLong(value.sizeHintBytes());
            out.writeInt(transferPayloadFormatCode(value.payloadFormat()));
        } else if (payload instanceof TransferEntryAcceptedResponse value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            BinaryIO.writeUtf8(out, value.path());
        } else if (payload instanceof TransferEntryCommitRequest value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            BinaryIO.writeUtf8(out, value.path());
            writeHash(out, value.contentHash());
            out.writeLong(value.bytesWritten());
            writeChecksum(out, value.checksum());
        } else if (payload instanceof TransferEntryCommittedResponse value) {
            writeUuid(out, value.transferId());
            out.writeInt(value.entrySequence());
            BinaryIO.writeUtf8(out, value.path());
            writeHash(out, value.contentHash());
            out.writeLong(value.bytesWritten());
            writeChecksum(out, value.checksum());
        } else if (payload instanceof ErrorResponse value) {
            out.writeInt(responseStatusCode(value.status()));
            BinaryIO.writeUtf8(out, value.message());
        } else {
            throw new BinaryFormatException("unsupported payload type: " + payload.getClass().getName());
        }
    }

    private static ControlPayload readPayload(DataInputStream in, ControlOperation operation) throws IOException {
        return switch (operation) {
            case AUTH_START -> new AuthStart(
                    readAuthMechanism(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES)
            );
            case AUTH_CHALLENGE -> new AuthChallenge(
                    readUuid(in),
                    readAuthMechanism(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    BinaryIO.readBytes(in, MAX_AUTH_BYTES),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    in.readInt()
            );
            case AUTH_PROOF -> new AuthProof(
                    readUuid(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    BinaryIO.readBytes(in, MAX_AUTH_BYTES)
            );
            case AUTH_ACCEPTED -> new AuthAccepted(readUuid(in), readAuthMechanisms(in));
            case LIST_DIRECTORY -> new DirectoryListRequest(BinaryIO.readUtf8(in, MAX_TEXT_BYTES));
            case LIST_DIRECTORY_RESULT -> new DirectoryListResponse(readDirectoryListing(in));
            case STAT_PATH -> new StatPathRequest(BinaryIO.readUtf8(in, MAX_TEXT_BYTES));
            case STAT_PATH_RESULT -> new StatPathResponse(readOptionalMetadata(in));
            case PUT_FILE -> new PutFileRequest(
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    in.readLong(),
                    readHash(in),
                    readChunkRefs(in)
            );
            case PUT_FILE_RESULT -> new PutFileResponse(readManifest(in), readHashes(in));
            case GET_MANIFEST -> new ManifestRequest(readHash(in));
            case MANIFEST_RESULT -> new ManifestResponse(readOptionalManifest(in));
            case GET_RECONSTRUCTION_PLAN -> new ReconstructionPlanRequest(readHash(in));
            case RECONSTRUCTION_PLAN_RESULT -> new ReconstructionPlanResponse(readReconstructionPlan(in));
            case READ_FILE_RANGE -> new ReadFileRangeRequest(
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    in.readLong(),
                    in.readLong()
            );
            case READ_FILE_RANGE_RESULT -> new ReadFileRangeResponse(
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readReadPlan(in)
            );
            case READ_CONTAINER_RANGE -> new ReadContainerRangeRequest(readHash(in), in.readInt(), in.readInt());
            case RENAME_PATH -> new RenamePathRequest(
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES)
            );
            case DELETE_PATH -> new DeletePathRequest(BinaryIO.readUtf8(in, MAX_TEXT_BYTES), in.readBoolean());
            case RENAME_PATH_RESULT -> new RenamePathResponse(
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readMetadata(in)
            );
            case DELETE_PATH_RESULT -> new DeletePathResponse(BinaryIO.readUtf8(in, MAX_TEXT_BYTES), in.readLong());
            case MOUNT_NEGOTIATE -> new MountNegotiateRequest(readMountBackend(in), readMountCapabilities(in));
            case MOUNT_NEGOTIATED -> new MountNegotiatedResponse(readMountStrategy(in));
            case TRANSFER_BOOTSTRAP -> new TransferBootstrapRequest(
                    readUuid(in),
                    readTransferDirection(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readTransferPayloadFormat(in),
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    in.readLong()
            );
            case TRANSFER_ACCEPTED -> new TransferAcceptedResponse(
                    readUuid(in),
                    readTransferDirection(in),
                    in.readInt(),
                    in.readInt(),
                    in.readLong(),
                    in.readBoolean()
            );
            case TRANSFER_METADATA -> new TransferMetadataUpdate(
                    readUuid(in),
                    in.readInt(),
                    in.readInt(),
                    in.readBoolean(),
                    in.readLong(),
                    readHashes(in)
            );
            case TRANSFER_METADATA_ACCEPTED -> new TransferMetadataAcceptedResponse(
                    readUuid(in),
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    in.readBoolean()
            );
            case TRANSFER_COMMIT -> new TransferCommitRequest(
                    readUuid(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readHash(in),
                    in.readLong()
            );
            case TRANSFER_COMMITTED -> new TransferCommittedResponse(readUuid(in), readHash(in), in.readLong());
            case TRANSFER_ENTRY_UPDATE -> new TransferEntryUpdate(
                    readUuid(in),
                    in.readInt(),
                    readTransferEntryAction(in),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    in.readLong(),
                    readTransferPayloadFormat(in)
            );
            case TRANSFER_ENTRY_ACCEPTED -> new TransferEntryAcceptedResponse(
                    readUuid(in),
                    in.readInt(),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES)
            );
            case TRANSFER_ENTRY_COMMIT -> new TransferEntryCommitRequest(
                    readUuid(in),
                    in.readInt(),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readHash(in),
                    in.readLong(),
                    readChecksum(in)
            );
            case TRANSFER_ENTRY_COMMITTED -> new TransferEntryCommittedResponse(
                    readUuid(in),
                    in.readInt(),
                    BinaryIO.readUtf8(in, MAX_TEXT_BYTES),
                    readHash(in),
                    in.readLong(),
                    readChecksum(in)
            );
            case ERROR -> new ErrorResponse(readResponseStatus(in), BinaryIO.readUtf8(in, MAX_TEXT_BYTES));
        };
    }

    private static void writeManifest(DataOutputStream out, FileManifest manifest) throws IOException {
        BinaryIO.writeBytes(out, ManifestCodec.encode(manifest));
    }

    private static FileManifest readManifest(DataInputStream in) throws IOException {
        return ManifestCodec.decode(BinaryIO.readBytes(in, ControlFrameCodec.MAX_PAYLOAD_BYTES));
    }

    private static void writeOptionalManifest(DataOutputStream out, Optional<FileManifest> manifest) throws IOException {
        out.writeBoolean(manifest.isPresent());
        if (manifest.isPresent()) {
            writeManifest(out, manifest.orElseThrow());
        }
    }

    private static Optional<FileManifest> readOptionalManifest(DataInputStream in) throws IOException {
        return in.readBoolean() ? Optional.of(readManifest(in)) : Optional.empty();
    }

    private static void writeDirectoryListing(DataOutputStream out, DirectoryListing listing) throws IOException {
        BinaryIO.writeUtf8(out, listing.path());
        out.writeInt(listing.entries().size());
        for (FileMetadata metadata : listing.entries()) {
            writeMetadata(out, metadata);
        }
    }

    private static DirectoryListing readDirectoryListing(DataInputStream in) throws IOException {
        String path = BinaryIO.readUtf8(in, MAX_TEXT_BYTES);
        int entryCount = readCount(in, MAX_LIST_ENTRIES, "directory listing entries");
        List<FileMetadata> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(readMetadata(in));
        }
        return new DirectoryListing(path, entries);
    }

    private static void writeOptionalMetadata(DataOutputStream out, Optional<FileMetadata> metadata) throws IOException {
        out.writeBoolean(metadata.isPresent());
        if (metadata.isPresent()) {
            writeMetadata(out, metadata.orElseThrow());
        }
    }

    private static Optional<FileMetadata> readOptionalMetadata(DataInputStream in) throws IOException {
        return in.readBoolean() ? Optional.of(readMetadata(in)) : Optional.empty();
    }

    private static void writeMetadata(DataOutputStream out, FileMetadata metadata) throws IOException {
        BinaryIO.writeUtf8(out, metadata.path());
        BinaryIO.writeUtf8(out, metadata.name());
        out.writeInt(fileTypeCode(metadata.type()));
        out.writeLong(metadata.size());
        out.writeBoolean(metadata.contentHash().isPresent());
        if (metadata.contentHash().isPresent()) {
            writeHash(out, metadata.contentHash().orElseThrow());
        }
        out.writeLong(metadata.modifiedAt().getEpochSecond());
        out.writeInt(metadata.modifiedAt().getNano());
    }

    private static FileMetadata readMetadata(DataInputStream in) throws IOException {
        String path = BinaryIO.readUtf8(in, MAX_TEXT_BYTES);
        String name = BinaryIO.readUtf8(in, MAX_TEXT_BYTES);
        FileType type = readFileType(in);
        long size = in.readLong();
        Optional<ContentHash> contentHash = in.readBoolean() ? Optional.of(readHash(in)) : Optional.empty();
        Instant modifiedAt = Instant.ofEpochSecond(in.readLong(), in.readInt());
        return new FileMetadata(path, name, type, size, contentHash, modifiedAt);
    }

    private static void writeReconstructionPlan(DataOutputStream out, ReconstructionPlan plan) throws IOException {
        writeHash(out, plan.fileHash());
        out.writeLong(plan.fileSize());
        out.writeInt(plan.terms().size());
        for (ReconstructionTerm term : plan.terms()) {
            writeHash(out, term.containerHash());
            out.writeInt(term.firstChunkIndex());
            out.writeInt(term.chunkCount());
        }
    }

    private static ReconstructionPlan readReconstructionPlan(DataInputStream in) throws IOException {
        ContentHash fileHash = readHash(in);
        long fileSize = in.readLong();
        int termCount = readCount(in, MAX_TERMS, "reconstruction terms");
        List<ReconstructionTerm> terms = new ArrayList<>(termCount);
        for (int i = 0; i < termCount; i++) {
            terms.add(new ReconstructionTerm(readHash(in), in.readInt(), in.readInt()));
        }
        return new ReconstructionPlan(fileHash, fileSize, terms);
    }

    private static void writeReadPlan(DataOutputStream out, ReadPlan plan) throws IOException {
        writeHash(out, plan.fileHash());
        out.writeLong(plan.fileSize());
        out.writeLong(plan.requestedRange().offset());
        out.writeLong(plan.requestedRange().length());
        out.writeInt(plan.chunks().size());
        for (PlannedChunkRead chunk : plan.chunks()) {
            out.writeInt(chunk.chunkIndex());
            writeHash(out, chunk.chunkHash());
            out.writeLong(chunk.fileOffset());
            out.writeInt(chunk.chunkSize());
            out.writeInt(chunk.offsetInChunk());
            out.writeInt(chunk.bytesToRead());
        }
    }

    private static ReadPlan readReadPlan(DataInputStream in) throws IOException {
        ContentHash fileHash = readHash(in);
        long fileSize = in.readLong();
        ByteRange requestedRange = new ByteRange(in.readLong(), in.readLong());
        int chunkCount = readCount(in, MAX_CHUNK_REFS, "read plan chunks");
        List<PlannedChunkRead> chunks = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunks.add(new PlannedChunkRead(
                    in.readInt(),
                    readHash(in),
                    in.readLong(),
                    in.readInt(),
                    in.readInt(),
                    in.readInt()
            ));
        }
        return new ReadPlan(fileHash, fileSize, requestedRange, chunks);
    }

    private static void writeChunkRefs(DataOutputStream out, List<ChunkRef> chunks) throws IOException {
        out.writeInt(chunks.size());
        for (ChunkRef chunk : chunks) {
            writeHash(out, chunk.hash());
            out.writeInt(chunk.size());
        }
    }

    private static List<ChunkRef> readChunkRefs(DataInputStream in) throws IOException {
        int count = readCount(in, MAX_CHUNK_REFS, "chunk refs");
        List<ChunkRef> chunks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            chunks.add(new ChunkRef(readHash(in), in.readInt()));
        }
        return chunks;
    }

    private static void writeHashes(DataOutputStream out, List<ContentHash> hashes) throws IOException {
        out.writeInt(hashes.size());
        for (ContentHash hash : hashes) {
            writeHash(out, hash);
        }
    }

    private static List<ContentHash> readHashes(DataInputStream in) throws IOException {
        int count = readCount(in, MAX_CHUNK_REFS, "hashes");
        List<ContentHash> hashes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hashes.add(readHash(in));
        }
        return hashes;
    }

    private static void writeMountStrategy(DataOutputStream out, MountStrategy strategy) throws IOException {
        out.writeInt(mountBackendCode(strategy.backend()));
        writeMountCapabilities(out, strategy.capabilities());
    }

    private static MountStrategy readMountStrategy(DataInputStream in) throws IOException {
        return new MountStrategy(readMountBackend(in), readMountCapabilities(in));
    }

    private static void writeAuthMechanisms(DataOutputStream out, List<AuthMechanism> mechanisms) throws IOException {
        out.writeInt(mechanisms.size());
        for (AuthMechanism mechanism : mechanisms) {
            out.writeInt(authMechanismCode(mechanism));
        }
    }

    private static List<AuthMechanism> readAuthMechanisms(DataInputStream in) throws IOException {
        int count = readCount(in, 16, "auth mechanisms");
        List<AuthMechanism> mechanisms = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            mechanisms.add(readAuthMechanism(in));
        }
        return mechanisms;
    }

    private static void writeMountCapabilities(DataOutputStream out, Set<MountCapability> capabilities) throws IOException {
        List<MountCapability> ordered = capabilities.stream()
                .sorted(java.util.Comparator.comparingInt(ControlPayloadCodec::mountCapabilityCode))
                .toList();
        out.writeInt(ordered.size());
        for (MountCapability capability : ordered) {
            out.writeInt(mountCapabilityCode(capability));
        }
    }

    private static Set<MountCapability> readMountCapabilities(DataInputStream in) throws IOException {
        int count = readCount(in, MountCapability.values().length, "mount capabilities");
        EnumSet<MountCapability> capabilities = EnumSet.noneOf(MountCapability.class);
        for (int i = 0; i < count; i++) {
            capabilities.add(readMountCapability(in));
        }
        return capabilities;
    }

    private static void writeHash(DataOutputStream out, ContentHash hash) throws IOException {
        out.write(hash.bytes());
    }

    private static ContentHash readHash(DataInputStream in) throws IOException {
        return ContentHash.of(BinaryIO.readFixedBytes(in, ContentHash.BYTE_LENGTH));
    }

    private static void writeChecksum(DataOutputStream out, TransferChecksum checksum) throws IOException {
        checksum = checksum == null ? TransferChecksum.none() : checksum;
        out.writeBoolean(checksum.present());
        if (checksum.present()) {
            BinaryIO.writeUtf8(out, checksum.algorithm());
            out.writeLong(checksum.value());
        }
    }

    private static TransferChecksum readChecksum(DataInputStream in) throws IOException {
        if (in.available() <= 0) {
            return TransferChecksum.none();
        }
        if (!in.readBoolean()) {
            return TransferChecksum.none();
        }
        return new TransferChecksum(BinaryIO.readUtf8(in, MAX_TEXT_BYTES), in.readLong());
    }

    private static void writeUuid(DataOutputStream out, UUID uuid) throws IOException {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    private static int readCount(DataInputStream in, int max, String name) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > max) {
            throw new BinaryFormatException("invalid " + name + " count: " + count);
        }
        return count;
    }

    private static int authMechanismCode(AuthMechanism mechanism) {
        return switch (mechanism) {
            case USERNAME_PASSWORD -> 1;
            case BEARER_TOKEN -> 2;
        };
    }

    private static AuthMechanism readAuthMechanism(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> AuthMechanism.USERNAME_PASSWORD;
            case 2 -> AuthMechanism.BEARER_TOKEN;
            default -> throw new BinaryFormatException("unknown auth mechanism code");
        };
    }

    private static int fileTypeCode(FileType type) {
        return switch (type) {
            case DIRECTORY -> 1;
            case REGULAR_FILE -> 2;
        };
    }

    private static FileType readFileType(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> FileType.DIRECTORY;
            case 2 -> FileType.REGULAR_FILE;
            default -> throw new BinaryFormatException("unknown file type code");
        };
    }

    private static int mountBackendCode(MountBackend backend) {
        return switch (backend) {
            case LINUX_FUSE -> 1;
            case MACOS_FUSE_T -> 2;
            case MACOS_MACFUSE -> 3;
            case WINDOWS_WINFSP -> 4;
            case WINDOWS_PROJFS -> 5;
            case CLI_LIST_ONLY -> 6;
        };
    }

    private static MountBackend readMountBackend(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> MountBackend.LINUX_FUSE;
            case 2 -> MountBackend.MACOS_FUSE_T;
            case 3 -> MountBackend.MACOS_MACFUSE;
            case 4 -> MountBackend.WINDOWS_WINFSP;
            case 5 -> MountBackend.WINDOWS_PROJFS;
            case 6 -> MountBackend.CLI_LIST_ONLY;
            default -> throw new BinaryFormatException("unknown mount backend code");
        };
    }

    private static int mountCapabilityCode(MountCapability capability) {
        return switch (capability) {
            case LIST -> 1;
            case READ -> 2;
            case WRITE -> 3;
        };
    }

    private static MountCapability readMountCapability(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> MountCapability.LIST;
            case 2 -> MountCapability.READ;
            case 3 -> MountCapability.WRITE;
            default -> throw new BinaryFormatException("unknown mount capability code");
        };
    }

    private static int transferDirectionCode(TransferDirection direction) {
        return switch (direction) {
            case READ -> 1;
            case WRITE -> 2;
        };
    }

    private static TransferDirection readTransferDirection(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> TransferDirection.READ;
            case 2 -> TransferDirection.WRITE;
            default -> throw new BinaryFormatException("unknown transfer direction code");
        };
    }

    private static int transferPayloadFormatCode(TransferPayloadFormat format) {
        return switch (format) {
            case CONTAINER -> 1;
            case MDS_SHARDS -> 2;
            case SAFETENSORS -> 3;
        };
    }

    private static TransferPayloadFormat readTransferPayloadFormat(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> TransferPayloadFormat.CONTAINER;
            case 2 -> TransferPayloadFormat.MDS_SHARDS;
            case 3 -> TransferPayloadFormat.SAFETENSORS;
            default -> throw new BinaryFormatException("unknown transfer payload format code");
        };
    }

    private static int transferEntryActionCode(TransferEntryAction action) {
        return switch (action) {
            case CREATE_OR_REPLACE -> 1;
            case DELETE -> 2;
            case RENAME -> 3;
        };
    }

    private static TransferEntryAction readTransferEntryAction(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 1 -> TransferEntryAction.CREATE_OR_REPLACE;
            case 2 -> TransferEntryAction.DELETE;
            case 3 -> TransferEntryAction.RENAME;
            default -> throw new BinaryFormatException("unknown transfer entry action code");
        };
    }

    private static int responseStatusCode(ResponseStatus status) {
        return switch (status) {
            case OK -> 0;
            case UNAUTHENTICATED -> 1;
            case UNAUTHORIZED -> 2;
            case NOT_FOUND -> 3;
            case CONFLICT -> 4;
            case INVALID_REQUEST -> 5;
            case UNSUPPORTED -> 6;
            case SERVER_ERROR -> 7;
        };
    }

    private static ResponseStatus readResponseStatus(DataInputStream in) throws IOException {
        return switch (in.readInt()) {
            case 0 -> ResponseStatus.OK;
            case 1 -> ResponseStatus.UNAUTHENTICATED;
            case 2 -> ResponseStatus.UNAUTHORIZED;
            case 3 -> ResponseStatus.NOT_FOUND;
            case 4 -> ResponseStatus.CONFLICT;
            case 5 -> ResponseStatus.INVALID_REQUEST;
            case 6 -> ResponseStatus.UNSUPPORTED;
            case 7 -> ResponseStatus.SERVER_ERROR;
            default -> throw new BinaryFormatException("unknown response status code");
        };
    }
}
