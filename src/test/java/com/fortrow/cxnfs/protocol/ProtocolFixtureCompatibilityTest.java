package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.auth.AuthMechanism;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkReadRequestCodec;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.BulkWriteAckCodec;
import com.fortrow.cxnfs.transport.StagedPayloadStorage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProtocolFixtureCompatibilityTest {
    private static final UUID SESSION_ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private static final UUID TRANSFER_ID = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
    private static final String OUTPUT_ROOT = "/outputs/run-1";
    private static final String OUTPUT_FILE = "/outputs/run-1/model.safetensors";
    private static final String FILE_HASH = "0404040404040404040404040404040404040404040404040404040404040404";
    private static final String TRANSFER_HASH = "0505050505050505050505050505050505050505050505050505050505050505";

    @Test
    void decodesAndReencodesAuthStartControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-auth-start-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        AuthStart payload = assertPayload(frame, AuthStart.class);

        assertEquals(ControlOperation.AUTH_START, frame.operation());
        assertTrue(frame.sessionId().isEmpty());
        assertEquals(UUID.fromString("11111111-1111-1111-1111-111111111111"), frame.messageId());
        assertEquals(AuthMechanism.USERNAME_PASSWORD, payload.mechanism());
        assertEquals("trainer", payload.principal());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesReadRangeResultControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-read-file-range-result-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        ReadFileRangeResponse payload = assertPayload(frame, ReadFileRangeResponse.class);
        ReadPlan plan = payload.plan();

        assertEquals(ControlOperation.READ_FILE_RANGE_RESULT, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals("/datasets/train/shard-00000.mds", payload.path());
        assertEquals(8192, plan.fileSize());
        assertEquals(128, plan.requestedRange().offset());
        assertEquals(1024, plan.requestedRange().length());
        assertEquals(2, plan.chunks().size());
        assertEquals(0, plan.chunks().getFirst().chunkIndex());
        assertEquals(3968, plan.chunks().getFirst().bytesToRead());
        assertEquals(1, plan.chunks().get(1).chunkIndex());
        assertEquals(1152, plan.chunks().get(1).bytesToRead());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferEntryUpdateControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-entry-update-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferEntryUpdate payload = assertPayload(frame, TransferEntryUpdate.class);

        assertEquals(ControlOperation.TRANSFER_ENTRY_UPDATE, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(TransferEntryAction.CREATE_OR_REPLACE, payload.action());
        assertEquals("/outputs/run-1/model.safetensors", payload.path());
        assertEquals("", payload.targetPath());
        assertEquals(123456789, payload.sizeHintBytes());
        assertEquals(TransferPayloadFormat.SAFETENSORS, payload.payloadFormat());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferBootstrapControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-bootstrap-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferBootstrapRequest payload = assertPayload(frame, TransferBootstrapRequest.class);

        assertEquals(ControlOperation.TRANSFER_BOOTSTRAP, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("55555555-5555-5555-5555-555555555555"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(TransferDirection.WRITE, payload.direction());
        assertEquals(OUTPUT_ROOT, payload.path());
        assertEquals(TransferPayloadFormat.SAFETENSORS, payload.payloadFormat());
        assertEquals(8, payload.initialCords());
        assertEquals(64, payload.maxCords());
        assertEquals(268_435_456, payload.provisionalContainerSizeBytes());
        assertEquals(5_368_709_120L, payload.expectedSizeLimitBytes());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferAcceptedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-accepted-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferAcceptedResponse payload = assertPayload(frame, TransferAcceptedResponse.class);

        assertEquals(ControlOperation.TRANSFER_ACCEPTED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("66666666-6666-6666-6666-666666666666"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(TransferDirection.WRITE, payload.direction());
        assertEquals(8, payload.acceptedInitialCords());
        assertEquals(64, payload.acceptedMaxCords());
        assertEquals(1_048_576, payload.flowControlWindowBytes());
        assertTrue(payload.writePayloadAdmissionGranted());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferEntryAcceptedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-entry-accepted-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferEntryAcceptedResponse payload = assertPayload(frame, TransferEntryAcceptedResponse.class);

        assertEquals(ControlOperation.TRANSFER_ENTRY_ACCEPTED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("77777777-7777-7777-7777-777777777777"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(OUTPUT_FILE, payload.path());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferMetadataControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-metadata-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferMetadataUpdate payload = assertPayload(frame, TransferMetadataUpdate.class);

        assertEquals(ControlOperation.TRANSFER_METADATA, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("88888888-8888-8888-8888-888888888888"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(2, payload.metadataSequence());
        assertTrue(payload.finalMetadata());
        assertEquals(123_456_789, payload.bytesCovered());
        assertEquals(2, payload.chunkHashes().size());
        assertEquals("0606060606060606060606060606060606060606060606060606060606060606",
                payload.chunkHashes().getFirst().hex());
        assertEquals("0707070707070707070707070707070707070707070707070707070707070707",
                payload.chunkHashes().get(1).hex());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferMetadataAcceptedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-metadata-accepted-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferMetadataAcceptedResponse payload = assertPayload(frame, TransferMetadataAcceptedResponse.class);

        assertEquals(ControlOperation.TRANSFER_METADATA_ACCEPTED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("99999999-9999-9999-9999-999999999999"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(2, payload.metadataSequence());
        assertEquals(2, payload.chunkHashesAccepted());
        assertEquals(2, payload.stagedPayloadsVerified());
        assertTrue(payload.finalMetadata());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferEntryCommitControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-entry-commit-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferEntryCommitRequest payload = assertPayload(frame, TransferEntryCommitRequest.class);

        assertEquals(ControlOperation.TRANSFER_ENTRY_COMMIT, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("abababab-abab-abab-abab-abababababab"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(OUTPUT_FILE, payload.path());
        assertEquals(FILE_HASH, payload.contentHash().hex());
        assertEquals(123_456_789, payload.bytesWritten());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferEntryCommittedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-entry-committed-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferEntryCommittedResponse payload = assertPayload(frame, TransferEntryCommittedResponse.class);

        assertEquals(ControlOperation.TRANSFER_ENTRY_COMMITTED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(7, payload.entrySequence());
        assertEquals(OUTPUT_FILE, payload.path());
        assertEquals(FILE_HASH, payload.contentHash().hex());
        assertEquals(123_456_789, payload.bytesWritten());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferCommitControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-commit-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferCommitRequest payload = assertPayload(frame, TransferCommitRequest.class);

        assertEquals(ControlOperation.TRANSFER_COMMIT, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(OUTPUT_ROOT, payload.path());
        assertEquals(TRANSFER_HASH, payload.contentHash().hex());
        assertEquals(123_456_789, payload.bytesWritten());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesTransferCommittedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-transfer-committed-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        TransferCommittedResponse payload = assertPayload(frame, TransferCommittedResponse.class);

        assertEquals(ControlOperation.TRANSFER_COMMITTED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("dededede-dede-dede-dede-dededededede"), frame.messageId());
        assertEquals(TRANSFER_ID, payload.transferId());
        assertEquals(TRANSFER_HASH, payload.contentHash().hex());
        assertEquals(123_456_789, payload.bytesWritten());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesErrorControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-error-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        ErrorResponse payload = assertPayload(frame, ErrorResponse.class);

        assertEquals(ControlOperation.ERROR, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("efefefef-efef-efef-efef-efefefefefef"), frame.messageId());
        assertEquals(ResponseStatus.CONFLICT, payload.status());
        assertEquals("transfer summary mismatch", payload.message());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesReadContainerRangeControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-read-container-range-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        ReadContainerRangeRequest payload = assertPayload(frame, ReadContainerRangeRequest.class);

        assertEquals(ControlOperation.READ_CONTAINER_RANGE, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("22222222-2222-2222-2222-222222222222"), frame.messageId());
        assertEquals(2, payload.firstChunkIndex());
        assertEquals(4, payload.chunkCount());
        assertEquals("0303030303030303030303030303030303030303030303030303030303030303", payload.containerHash().hex());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesRenamePathResultControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-rename-path-result-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        RenamePathResponse payload = assertPayload(frame, RenamePathResponse.class);
        FileMetadata metadata = payload.metadata();

        assertEquals(ControlOperation.RENAME_PATH_RESULT, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("33333333-3333-3333-3333-333333333333"), frame.messageId());
        assertEquals("/datasets/train/old.mds", payload.sourcePath());
        assertEquals("/datasets/train/new.mds", payload.targetPath());
        assertEquals("/datasets/train/new.mds", metadata.path());
        assertEquals("new.mds", metadata.name());
        assertEquals(FileType.REGULAR_FILE, metadata.type());
        assertEquals(4096, metadata.size());
        assertEquals("0101010101010101010101010101010101010101010101010101010101010101", metadata.contentHash().orElseThrow().hex());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesDeletePathResultControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-delete-path-result-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        DeletePathResponse payload = assertPayload(frame, DeletePathResponse.class);

        assertEquals(ControlOperation.DELETE_PATH_RESULT, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("44444444-4444-4444-4444-444444444444"), frame.messageId());
        assertEquals("/datasets/train/new.mds", payload.path());
        assertEquals(1, payload.deletedEntries());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesMountNegotiateControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-mount-negotiate-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        MountNegotiateRequest payload = assertPayload(frame, MountNegotiateRequest.class);

        assertEquals(ControlOperation.MOUNT_NEGOTIATE, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("12121212-1212-1212-1212-121212121212"), frame.messageId());
        assertEquals(MountBackend.MACOS_FUSE_T, payload.backend());
        assertEquals(Set.of(MountCapability.LIST, MountCapability.READ), payload.requestedCapabilities());
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesMountNegotiatedControlFrameFixture() throws Exception {
        byte[] fixture = fixture("control-mount-negotiated-frame.hex");

        ControlFrame frame = ControlFrameCodec.decode(fixture);
        MountNegotiatedResponse payload = assertPayload(frame, MountNegotiatedResponse.class);

        assertEquals(ControlOperation.MOUNT_NEGOTIATED, frame.operation());
        assertEquals(SESSION_ID, frame.sessionId().orElseThrow());
        assertEquals(UUID.fromString("13131313-1313-1313-1313-131313131313"), frame.messageId());
        assertEquals(MountBackend.MACOS_FUSE_T, payload.strategy().backend());
        assertEquals(
                Set.of(MountCapability.LIST, MountCapability.READ, MountCapability.WRITE),
                payload.strategy().capabilities()
        );
        assertRoundTrips(fixture, frame, payload);
    }

    @Test
    void decodesAndReencodesBulkReadRequestFixture() throws Exception {
        byte[] fixture = fixture("bulk-read-request.hex");

        BulkReadRequest request = BulkReadRequestCodec.decode(fixture);

        assertEquals(SESSION_ID, request.sessionId());
        assertEquals(TRANSFER_ID, request.transferId());
        assertEquals("/datasets/train/shard-00000.mds", request.path());
        assertEquals(128, request.offset());
        assertEquals(1024, request.length());
        assertEquals(16, request.activeCords());
        assertArrayEquals(fixture, BulkReadRequestCodec.encode(request));
    }

    @Test
    void decodesAndReencodesBulkDataFrameFixture() throws Exception {
        byte[] fixture = fixture("bulk-data-frame.hex");

        BulkDataFrame frame = BulkDataFrameCodec.decode(fixture);

        assertEquals(TRANSFER_ID, frame.transferId());
        assertEquals(3, frame.cordIndex());
        assertEquals(7, frame.entrySequence());
        assertEquals(11, frame.chunkSequence());
        assertEquals(4096, frame.entryOffset());
        assertTrue(frame.finalFrameForChunk());
        assertArrayEquals("CXNFS fixture payload".getBytes(java.nio.charset.StandardCharsets.UTF_8), frame.payload());
        assertArrayEquals(fixture, BulkDataFrameCodec.encode(frame));
    }

    @Test
    void decodesAndReencodesBulkWriteAckFixture() throws Exception {
        byte[] fixture = fixture("bulk-write-ack.hex");

        BulkWriteAck ack = BulkWriteAckCodec.decode(fixture);

        assertEquals(TRANSFER_ID, ack.key().transferId());
        assertEquals(7, ack.key().entrySequence());
        assertEquals(11, ack.key().chunkSequence());
        assertEquals(21, ack.payloadLength());
        assertTrue(ack.metadataVerified());
        assertEquals(StagedPayloadStorage.MEMORY, ack.storage());
        assertArrayEquals(fixture, BulkWriteAckCodec.encode(ack));
    }

    private static <T extends ControlPayload> T assertPayload(ControlFrame frame, Class<T> type) throws Exception {
        return assertInstanceOf(
                type,
                ControlPayloadCodec.decode(frame.operation(), frame.payload())
        );
    }

    private static void assertRoundTrips(
            byte[] fixture,
            ControlFrame frame,
            ControlPayload payload
    ) throws Exception {
        byte[] payloadBytes = ControlPayloadCodec.encode(payload);
        assertArrayEquals(frame.payload(), payloadBytes);
        assertArrayEquals(
                fixture,
                ControlFrameCodec.encode(new ControlFrame(
                        frame.version(),
                        frame.messageId(),
                        frame.sessionId(),
                        frame.operation(),
                        payloadBytes
                ))
        );
    }

    private static byte[] fixture(String name) throws Exception {
        String hex = Files.readString(Path.of("protocol-fixtures").resolve(name))
                .replaceAll("\\s+", "");
        return HexFormat.of().parseHex(hex);
    }
}
