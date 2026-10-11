package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.dataset.SafetensorsOutputSender;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;
import com.fortrow.cxnfs.mount.MountStrategy;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.protocol.AuthAccepted;
import com.fortrow.cxnfs.protocol.AuthChallenge;
import com.fortrow.cxnfs.protocol.ControlEnvelope;
import com.fortrow.cxnfs.protocol.ControlFrame;
import com.fortrow.cxnfs.protocol.ControlFrameCodec;
import com.fortrow.cxnfs.protocol.ControlFrameStream;
import com.fortrow.cxnfs.protocol.ControlPayload;
import com.fortrow.cxnfs.protocol.ControlPayloadCodec;
import com.fortrow.cxnfs.protocol.DeletePathRequest;
import com.fortrow.cxnfs.protocol.DeletePathResponse;
import com.fortrow.cxnfs.protocol.DirectoryListRequest;
import com.fortrow.cxnfs.protocol.DirectoryListResponse;
import com.fortrow.cxnfs.protocol.ErrorResponse;
import com.fortrow.cxnfs.protocol.ManifestRequest;
import com.fortrow.cxnfs.protocol.ManifestResponse;
import com.fortrow.cxnfs.protocol.MountNegotiateRequest;
import com.fortrow.cxnfs.protocol.MountNegotiatedResponse;
import com.fortrow.cxnfs.protocol.PutFileRequest;
import com.fortrow.cxnfs.protocol.PutFileResponse;
import com.fortrow.cxnfs.protocol.ReadFileRangeRequest;
import com.fortrow.cxnfs.protocol.ReadFileRangeResponse;
import com.fortrow.cxnfs.protocol.ReconstructionPlanRequest;
import com.fortrow.cxnfs.protocol.ReconstructionPlanResponse;
import com.fortrow.cxnfs.protocol.RenamePathRequest;
import com.fortrow.cxnfs.protocol.RenamePathResponse;
import com.fortrow.cxnfs.protocol.StatPathRequest;
import com.fortrow.cxnfs.protocol.StatPathResponse;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferBootstrapRequest;
import com.fortrow.cxnfs.protocol.TransferCommitRequest;
import com.fortrow.cxnfs.protocol.TransferCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferDirection;
import com.fortrow.cxnfs.protocol.TransferEntryAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryCommitRequest;
import com.fortrow.cxnfs.protocol.TransferEntryCommittedResponse;
import com.fortrow.cxnfs.protocol.TransferEntryUpdate;
import com.fortrow.cxnfs.protocol.TransferMetadataAcceptedResponse;
import com.fortrow.cxnfs.protocol.TransferMetadataUpdate;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkFrameStream;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkReadRequestCodec;
import com.fortrow.cxnfs.transport.BulkReadRequestStream;
import com.fortrow.cxnfs.transport.BulkReadStreamingReceiver;
import com.fortrow.cxnfs.transport.BulkWriteAck;
import com.fortrow.cxnfs.transport.BulkWriteAckCodec;
import com.fortrow.cxnfs.transport.BulkWriteAckStream;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CxnFileNetworkClient implements AutoCloseable, SafetensorsOutputSender.WriteClient {
    private final InputStream controlInput;
    private final OutputStream controlOutput;
    private final CxnFileNetworkClientConfig config;
    private final AutoCloseable closeAction;
    private final BulkTransferClientChannel bulkChannel;
    private final Object controlLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private UUID sessionId;

    public CxnFileNetworkClient(
            InputStream controlInput,
            OutputStream controlOutput,
            InputStream bulkInput,
            OutputStream bulkOutput
    ) {
        this(controlInput, controlOutput, bulkInput, bulkOutput, CxnFileNetworkClientConfig.defaults(), () -> {
        });
    }

    public CxnFileNetworkClient(
            InputStream controlInput,
            OutputStream controlOutput,
            InputStream bulkInput,
            OutputStream bulkOutput,
            CxnFileNetworkClientConfig config,
            AutoCloseable closeAction
    ) {
        this(
                controlInput,
                controlOutput,
                config,
                closeAction,
                new LengthPrefixedBulkTransferClientChannel(bulkInput, bulkOutput, null)
        );
    }

    private CxnFileNetworkClient(
            InputStream controlInput,
            OutputStream controlOutput,
            CxnFileNetworkClientConfig config,
            AutoCloseable closeAction,
            BulkTransferClientChannel bulkChannel
    ) {
        if (controlInput == null) {
            throw new IllegalArgumentException("controlInput is required");
        }
        if (controlOutput == null) {
            throw new IllegalArgumentException("controlOutput is required");
        }
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        if (bulkChannel == null) {
            throw new IllegalArgumentException("bulkChannel is required");
        }
        this.controlInput = controlInput;
        this.controlOutput = controlOutput;
        this.config = config;
        this.closeAction = closeAction == null ? () -> {
        } : closeAction;
        this.bulkChannel = bulkChannel;
    }

    public static CxnFileNetworkClient connect(
            InetSocketAddress controlAddress,
            InetSocketAddress bulkAddress,
            SocketFactory socketFactory
    ) throws IOException {
        return connect(
                controlAddress,
                bulkAddress,
                socketFactory,
                socketFactory,
                CxnFileNetworkClientConfig.defaults()
        );
    }

    public static CxnFileNetworkClient connect(
            InetSocketAddress controlAddress,
            InetSocketAddress bulkAddress,
            SocketFactory controlSocketFactory,
            SocketFactory bulkSocketFactory,
            CxnFileNetworkClientConfig config
    ) throws IOException {
        if (controlAddress == null) {
            throw new IllegalArgumentException("controlAddress is required");
        }
        if (bulkAddress == null) {
            throw new IllegalArgumentException("bulkAddress is required");
        }
        if (controlSocketFactory == null) {
            throw new IllegalArgumentException("controlSocketFactory is required");
        }
        if (bulkSocketFactory == null) {
            throw new IllegalArgumentException("bulkSocketFactory is required");
        }
        CxnFileNetworkClientConfig clientConfig = config == null ? CxnFileNetworkClientConfig.defaults() : config;
        Socket controlSocket;
        try {
            controlSocket = openConnectedSocket(controlSocketFactory, controlAddress, clientConfig);
        } catch (IOException e) {
            throw endpointConnectionFailure("control", controlAddress, e);
        }
        Socket bulkSocket = null;
        BulkWriteSocketPool writeSocketPool = null;
        try {
            try {
                bulkSocket = openConnectedSocket(bulkSocketFactory, bulkAddress, clientConfig);
            } catch (IOException e) {
                throw endpointConnectionFailure("stream bulk", bulkAddress, e);
            }
            Socket finalBulkSocket = bulkSocket;
            writeSocketPool = new BulkWriteSocketPool(
                    bulkSocketFactory,
                    bulkAddress,
                    clientConfig,
                    clientConfig.maxCords()
            );
            BulkWriteSocketPool finalWriteSocketPool = writeSocketPool;
            return new CxnFileNetworkClient(
                    controlSocket.getInputStream(),
                    controlSocket.getOutputStream(),
                    clientConfig,
                    () -> closeClientResources(controlSocket, finalBulkSocket, finalWriteSocketPool),
                    new LengthPrefixedBulkTransferClientChannel(
                            bulkSocket.getInputStream(),
                            bulkSocket.getOutputStream(),
                            finalWriteSocketPool
                    )
            );
        } catch (IOException | RuntimeException e) {
            closeQuietly(controlSocket);
            closeQuietly(bulkSocket);
            if (writeSocketPool != null) {
                closeQuietly(writeSocketPool);
            }
            throw e;
        }
    }

    public static CxnFileNetworkClient connect(
            InetSocketAddress controlAddress,
            SocketFactory controlSocketFactory,
            CxnFileNetworkClientConfig config,
            BulkTransferClientChannel bulkChannel
    ) throws IOException {
        if (controlAddress == null) {
            throw new IllegalArgumentException("controlAddress is required");
        }
        if (controlSocketFactory == null) {
            throw new IllegalArgumentException("controlSocketFactory is required");
        }
        if (bulkChannel == null) {
            throw new IllegalArgumentException("bulkChannel is required");
        }
        CxnFileNetworkClientConfig clientConfig = config == null ? CxnFileNetworkClientConfig.defaults() : config;
        Socket controlSocket;
        try {
            controlSocket = openConnectedSocket(controlSocketFactory, controlAddress, clientConfig);
        } catch (IOException e) {
            throw endpointConnectionFailure("control", controlAddress, e);
        }
        boolean owned = true;
        try {
            CxnFileNetworkClient client = new CxnFileNetworkClient(
                    controlSocket.getInputStream(),
                    controlSocket.getOutputStream(),
                    clientConfig,
                    controlSocket::close,
                    bulkChannel
            );
            owned = false;
            return client;
        } finally {
            if (owned) {
                closeQuietly(controlSocket);
                closeQuietly(bulkChannel);
            }
        }
    }

    private static IOException endpointConnectionFailure(
            String role,
            InetSocketAddress address,
            IOException cause
    ) {
        return new IOException(
                "failed to connect " + role + " endpoint "
                        + address.getHostString() + ":" + address.getPort()
                        + ": " + cause.getMessage(),
                cause
        );
    }

    private static Socket openConnectedSocket(
            SocketFactory socketFactory,
            InetSocketAddress address,
            CxnFileNetworkClientConfig config
    ) throws IOException {
        if (socketFactory instanceof ControlTlsClientSocketFactory tlsSocketFactory) {
            return tlsSocketFactory.createConnectedSocket(
                    address,
                    config.connectTimeoutMillis(),
                    config.readTimeoutMillis()
            );
        }
        Socket socket = socketFactory.createSocket();
        boolean connected = false;
        try {
            socket.connect(address, config.connectTimeoutMillis());
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(config.readTimeoutMillis());
            if (socket instanceof SSLSocket sslSocket) {
                sslSocket.startHandshake();
            }
            connected = true;
            return socket;
        } finally {
            if (!connected) {
                closeQuietly(socket);
            }
        }
    }

    public Optional<UUID> sessionId() {
        return Optional.ofNullable(sessionId);
    }

    public AuthAccepted authenticate(ClientCredentials credentials) throws IOException {
        ensureOpen();
        ClientAuthenticator authenticator = new ClientAuthenticator();
        ControlPayload challenge = exchangeUnauthenticated(authenticator.start(credentials));
        if (!(challenge instanceof AuthChallenge authChallenge)) {
            throw unexpectedPayload("AuthChallenge", challenge);
        }
        ControlPayload accepted = exchangeUnauthenticated(authenticator.answer(authChallenge, credentials));
        if (!(accepted instanceof AuthAccepted authAccepted)) {
            throw unexpectedPayload("AuthAccepted", accepted);
        }
        sessionId = authAccepted.sessionId();
        return authAccepted;
    }

    public DirectoryListing list(String path) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new DirectoryListRequest(path));
        if (payload instanceof DirectoryListResponse response) {
            return response.listing();
        }
        throw unexpectedPayload("DirectoryListResponse", payload);
    }

    public Optional<FileMetadata> stat(String path) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new StatPathRequest(path));
        if (payload instanceof StatPathResponse response) {
            return response.metadata();
        }
        throw unexpectedPayload("StatPathResponse", payload);
    }

    public Optional<FileManifest> manifest(ContentHash fileHash) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new ManifestRequest(fileHash));
        if (payload instanceof ManifestResponse response) {
            return response.manifest();
        }
        throw unexpectedPayload("ManifestResponse", payload);
    }

    public ReconstructionPlan reconstructionPlan(ContentHash fileHash) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new ReconstructionPlanRequest(fileHash));
        if (payload instanceof ReconstructionPlanResponse response) {
            return response.plan();
        }
        throw unexpectedPayload("ReconstructionPlanResponse", payload);
    }

    public ReadPlan readPlan(String path, long offset, long length) throws IOException {
        ReadFileRangeResponse response = readPlanResponse(path, offset, length);
        validateReadResponse(path, offset, length, response);
        return response.plan();
    }

    private ReadFileRangeResponse readPlanResponse(String path, long offset, long length) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new ReadFileRangeRequest(path, offset, length));
        if (payload instanceof ReadFileRangeResponse response) {
            return response;
        }
        throw unexpectedPayload("ReadFileRangeResponse", payload);
    }

    public MountStrategy negotiateMount(
            MountBackend backend,
            Set<MountCapability> requestedCapabilities
    ) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new MountNegotiateRequest(backend, requestedCapabilities));
        if (payload instanceof MountNegotiatedResponse response) {
            return response.strategy();
        }
        throw unexpectedPayload("MountNegotiatedResponse", payload);
    }

    public PutFileResponse preparePutFile(PutFileRequest request) throws IOException {
        ControlPayload payload = exchangeAuthenticated(request);
        if (payload instanceof PutFileResponse response) {
            return response;
        }
        throw unexpectedPayload("PutFileResponse", payload);
    }

    public RenamePathResponse renamePath(String sourcePath, String targetPath) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new RenamePathRequest(sourcePath, targetPath));
        if (payload instanceof RenamePathResponse response) {
            return response;
        }
        throw unexpectedPayload("RenamePathResponse", payload);
    }

    public DeletePathResponse deletePath(String path, boolean recursive) throws IOException {
        ControlPayload payload = exchangeAuthenticated(new DeletePathRequest(path, recursive));
        if (payload instanceof DeletePathResponse response) {
            return response;
        }
        throw unexpectedPayload("DeletePathResponse", payload);
    }

    public byte[] readRange(String path, long offset, long length) throws IOException {
        return readRange(path, offset, length, TransferPayloadFormat.MDS_SHARDS);
    }

    public byte[] readRange(String path, long offset, long length, TransferPayloadFormat payloadFormat)
            throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        readRangeTo(path, offset, length, payloadFormat, output);
        return output.toByteArray();
    }

    public long readRangeTo(
            String path,
            long offset,
            long length,
            TransferPayloadFormat payloadFormat,
            OutputStream output
    ) throws IOException {
        ensureOpen();
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        requireAuthenticated();
        TransferAcceptedResponse transfer = bootstrapRead(path, payloadFormat);
        ReadFileRangeResponse response = readPlanResponse(path, offset, length);
        validateReadResponse(path, offset, length, response);
        ReadPlan plan = response.plan();
        long plannedLength = plan.requestedRange().length();
        long bytesRead = bulkChannel.read(
                new BulkReadRequest(
                        sessionId,
                        transfer.transferId(),
                        path,
                        plan.requestedRange().offset(),
                        plan.requestedRange().length(),
                        transfer.acceptedInitialCords()
                ),
                transfer,
                plan,
                output
        );
        if (bytesRead != plannedLength) {
            throw new IOException("bulk read completed with " + bytesRead
                    + " bytes for requested length " + plannedLength);
        }
        return bytesRead;
    }

    public TransferAcceptedResponse bootstrapRead(String path) throws IOException {
        return bootstrapRead(path, TransferPayloadFormat.MDS_SHARDS);
    }

    public TransferAcceptedResponse bootstrapRead(String path, TransferPayloadFormat payloadFormat) throws IOException {
        ensureOpen();
        requireAuthenticated();
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
        ControlPayload payload = exchangeAuthenticated(new TransferBootstrapRequest(
                UUID.randomUUID(),
                TransferDirection.READ,
                path,
                payloadFormat,
                config.initialCords(),
                config.maxCords(),
                config.provisionalContainerSizeBytes(),
                config.expectedSizeLimitBytes()
        ));
        if (payload instanceof TransferAcceptedResponse response) {
            validateReadTransferAdmission(response);
            return response;
        }
        throw unexpectedPayload("TransferAcceptedResponse", payload);
    }

    public TransferAcceptedResponse bootstrapWrite(String rootPath) throws IOException {
        return bootstrapWrite(rootPath, TransferPayloadFormat.SAFETENSORS);
    }

    public TransferAcceptedResponse bootstrapWrite(String rootPath, TransferPayloadFormat payloadFormat) throws IOException {
        ensureOpen();
        requireAuthenticated();
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
        ControlPayload payload = exchangeAuthenticated(new TransferBootstrapRequest(
                UUID.randomUUID(),
                TransferDirection.WRITE,
                rootPath,
                payloadFormat,
                config.initialCords(),
                config.maxCords(),
                config.provisionalContainerSizeBytes(),
                config.expectedSizeLimitBytes()
        ));
        if (payload instanceof TransferAcceptedResponse response) {
            validateWriteTransferAdmission(response);
            return response;
        }
        throw unexpectedPayload("TransferAcceptedResponse", payload);
    }

    public TransferEntryAcceptedResponse acceptWriteEntry(TransferEntryUpdate update) throws IOException {
        ControlPayload payload = exchangeAuthenticated(update);
        if (payload instanceof TransferEntryAcceptedResponse response) {
            validateWriteEntryAccepted(update, response);
            return response;
        }
        throw unexpectedPayload("TransferEntryAcceptedResponse", payload);
    }

    public void sendWriteFrame(BulkDataFrame frame) throws IOException {
        ensureOpen();
        requireAuthenticated();
        BulkWriteAck ack = bulkChannel.sendWriteFrame(frame);
        requireAckMatchesFrame(frame, ack);
    }

    public void sendWriteFrames(List<BulkDataFrame> frames) throws IOException {
        ensureOpen();
        requireAuthenticated();
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        if (frames.isEmpty()) {
            return;
        }
        List<BulkWriteAck> acks = bulkChannel.sendWriteDataFrames(frames);
        if (acks.size() != frames.size()) {
            throw new IOException("bulk write returned " + acks.size()
                    + " acks for " + frames.size() + " frames");
        }
        for (int index = 0; index < frames.size(); index++) {
            requireAckMatchesFrame(frames.get(index), acks.get(index));
        }
    }

    public void sendEncodedWriteFrame(BulkDataFrame expectedFrame, byte[] frameBytes) throws IOException {
        ensureOpen();
        requireAuthenticated();
        if (expectedFrame == null) {
            throw new IllegalArgumentException("expectedFrame is required");
        }
        if (frameBytes == null) {
            throw new IllegalArgumentException("frameBytes is required");
        }
        BulkWriteAck ack = bulkChannel.sendWriteFrames(
                List.of(frameBytes),
                List.of(BulkWriteFrameInfo.from(expectedFrame))
        ).getFirst();
        requireAckMatchesFrame(expectedFrame, ack);
    }

    public void sendEncodedWriteFrames(List<EncodedWriteFrame> frames) throws IOException {
        ensureOpen();
        requireAuthenticated();
        if (frames == null) {
            throw new IllegalArgumentException("frames are required");
        }
        if (frames.isEmpty()) {
            return;
        }
        List<byte[]> encodedFrames = new ArrayList<>(frames.size());
        List<BulkWriteFrameInfo> expectedFrames = new ArrayList<>(frames.size());
        for (EncodedWriteFrame frame : frames) {
            encodedFrames.add(frame.encodedBytes());
            expectedFrames.add(BulkWriteFrameInfo.from(frame.frame()));
        }
        List<BulkWriteAck> acks = bulkChannel.sendWriteFrames(encodedFrames, expectedFrames);
        if (acks.size() != frames.size()) {
            throw new IOException("bulk write returned " + acks.size()
                    + " acks for " + frames.size() + " frames");
        }
        for (int index = 0; index < frames.size(); index++) {
            requireAckMatchesFrame(frames.get(index).frame(), acks.get(index));
        }
    }

    public TransferMetadataAcceptedResponse acceptWriteMetadata(TransferMetadataUpdate update) throws IOException {
        ControlPayload payload = exchangeAuthenticated(update);
        if (payload instanceof TransferMetadataAcceptedResponse response) {
            validateWriteMetadataAccepted(update, response);
            return response;
        }
        throw unexpectedPayload("TransferMetadataAcceptedResponse", payload);
    }

    public TransferEntryCommittedResponse commitWriteEntry(TransferEntryCommitRequest request) throws IOException {
        ControlPayload payload = exchangeAuthenticated(request);
        if (payload instanceof TransferEntryCommittedResponse response) {
            validateWriteEntryCommitted(request, response);
            return response;
        }
        throw unexpectedPayload("TransferEntryCommittedResponse", payload);
    }

    public TransferCommittedResponse commitWriteTransfer(TransferCommitRequest request) throws IOException {
        ControlPayload payload = exchangeAuthenticated(request);
        if (payload instanceof TransferCommittedResponse response) {
            validateWriteTransferCommitted(request, response);
            return response;
        }
        throw unexpectedPayload("TransferCommittedResponse", payload);
    }

    private ControlPayload exchangeUnauthenticated(ControlPayload payload) throws IOException {
        ensureOpen();
        return exchange(ControlEnvelope.unauthenticated(payload));
    }

    private ControlPayload exchangeAuthenticated(ControlPayload payload) throws IOException {
        ensureOpen();
        return exchange(ControlEnvelope.authenticated(requireAuthenticated(), payload));
    }

    private ControlPayload exchange(ControlEnvelope<? extends ControlPayload> envelope) throws IOException {
        ensureOpen();
        synchronized (controlLock) {
            byte[] payloadBytes = ControlPayloadCodec.encode(envelope.payload());
            ControlFrameStream.write(controlOutput, ControlFrameCodec.encode(ControlFrame.fromEnvelope(envelope, payloadBytes)));
            ControlFrame responseFrame = ControlFrameCodec.decode(
                    ControlFrameStream.read(controlInput)
                            .orElseThrow(() -> new IOException("control stream ended before response"))
            );
            if (envelope.sessionId().isPresent() && !responseFrame.sessionId().equals(envelope.sessionId())) {
                throw new IOException("control response session does not match authenticated request session");
            }
            ControlPayload response = ControlPayloadCodec.decode(responseFrame.operation(), responseFrame.payload());
            if (response instanceof ErrorResponse error) {
                throw new CxnFileClientException(error.status(), error.message());
            }
            return response;
        }
    }

    private UUID requireAuthenticated() {
        if (sessionId == null) {
            throw new IllegalStateException("authenticate before sending control requests");
        }
        return sessionId;
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("network client is closed");
        }
    }

    private static IOException unexpectedPayload(String expected, ControlPayload actual) {
        return new IOException("expected " + expected + ", got " + actual.getClass().getSimpleName());
    }

    private static void validateReadResponse(
            String requestedPath,
            long requestedOffset,
            long requestedLength,
            ReadFileRangeResponse response
    ) throws IOException {
        String normalizedRequestedPath = normalizeControlPath(requestedPath);
        String normalizedResponsePath = normalizeControlPath(response.path());
        if (!normalizedRequestedPath.equals(normalizedResponsePath)) {
            throw new IOException("control read response path does not match requested read");
        }
        ReadPlan plan = response.plan();
        long fileSize = plan.fileSize();
        long expectedOffset;
        long expectedLength;
        if (requestedOffset >= fileSize) {
            expectedOffset = fileSize;
            expectedLength = 0L;
        } else {
            expectedOffset = requestedOffset;
            expectedLength = Math.min(requestedLength, fileSize - requestedOffset);
        }
        if (plan.requestedRange().offset() != expectedOffset
                || plan.requestedRange().length() != expectedLength) {
            throw new IOException("control read plan range does not match requested read");
        }
    }

    private static void validateReadTransferAdmission(TransferAcceptedResponse response) throws IOException {
        if (response.direction() != TransferDirection.READ || response.writePayloadAdmissionGranted()) {
            throw new IOException("control read transfer admission does not match requested read");
        }
    }

    private static void validateWriteTransferAdmission(TransferAcceptedResponse response) throws IOException {
        if (response.direction() != TransferDirection.WRITE || !response.writePayloadAdmissionGranted()) {
            throw new IOException("control write transfer admission does not match requested write");
        }
    }

    private static void validateWriteEntryAccepted(
            TransferEntryUpdate request,
            TransferEntryAcceptedResponse response
    ) throws IOException {
        if (!response.transferId().equals(request.transferId())
                || response.entrySequence() != request.entrySequence()
                || !normalizeControlPath(response.path()).equals(normalizeControlPath(request.path()))) {
            throw new IOException("write entry acknowledgement does not match request");
        }
    }

    private static void validateWriteMetadataAccepted(
            TransferMetadataUpdate request,
            TransferMetadataAcceptedResponse response
    ) throws IOException {
        if (!response.transferId().equals(request.transferId())
                || response.entrySequence() != request.entrySequence()
                || response.metadataSequence() != request.metadataSequence()
                || response.chunkHashesAccepted() != request.chunkHashes().size()
                || response.stagedPayloadsVerified() > response.chunkHashesAccepted()
                || response.finalMetadata() != request.finalMetadata()) {
            throw new IOException("write metadata acknowledgement does not match request");
        }
    }

    private static void validateWriteEntryCommitted(
            TransferEntryCommitRequest request,
            TransferEntryCommittedResponse response
    ) throws IOException {
        if (!response.transferId().equals(request.transferId())
                || response.entrySequence() != request.entrySequence()
                || !normalizeControlPath(response.path()).equals(normalizeControlPath(request.path()))
                || !response.contentHash().equals(request.contentHash())
                || response.bytesWritten() != request.bytesWritten()
                || !response.checksum().equals(request.checksum())) {
            throw new IOException("write entry commit acknowledgement does not match request");
        }
    }

    private static void validateWriteTransferCommitted(
            TransferCommitRequest request,
            TransferCommittedResponse response
    ) throws IOException {
        if (!response.transferId().equals(request.transferId())
                || !response.contentHash().equals(request.contentHash())
                || response.bytesWritten() != request.bytesWritten()) {
            throw new IOException("write transfer commit acknowledgement does not match request");
        }
    }

    private static String normalizeControlPath(String path) {
        return NamespacePath.normalize(path);
    }

    private static void requireAckMatchesFrame(BulkDataFrame frame, BulkWriteAck ack) throws IOException {
        if (!frame.key().equals(ack.key())) {
            throw new IOException("bulk write ack key does not match sent frame");
        }
        if (frame.payloadLength() != ack.payloadLength()) {
            throw new IOException("bulk write ack length does not match sent frame");
        }
    }

    private static final class LengthPrefixedBulkTransferClientChannel implements BulkTransferClientChannel {
        private final InputStream input;
        private final OutputStream output;
        private final BulkFrameSender frameSender;
        private final Object lock = new Object();

        private LengthPrefixedBulkTransferClientChannel(
                InputStream input,
                OutputStream output,
                BulkFrameSender frameSender
        ) {
            if (input == null) {
                throw new IllegalArgumentException("bulk input is required");
            }
            if (output == null) {
                throw new IllegalArgumentException("bulk output is required");
            }
            this.input = input;
            this.output = output;
            this.frameSender = frameSender;
        }

        @Override
        public long read(
                BulkReadRequest request,
                TransferAcceptedResponse transfer,
                ReadPlan plan,
                OutputStream destination
        ) throws IOException {
            synchronized (lock) {
                BulkReadRequestStream.write(output, BulkReadRequestCodec.encode(request));
                BulkReadStreamingReceiver receiver = new BulkReadStreamingReceiver(transfer, plan, destination);
                for (int index = 0; index < plan.chunks().size(); index++) {
                    byte[] frameBytes = BulkFrameStream.read(input)
                            .orElseThrow(() -> new IOException("bulk stream ended before read completed"));
                    BulkDataFrame frame = BulkDataFrameCodec.decode(frameBytes);
                    receiver.accept(frame);
                }
                return receiver.complete();
            }
        }

        @Override
        public BulkWriteAck sendWriteFrame(byte[] frameBytes) throws IOException {
            if (frameSender != null) {
                return frameSender.send(frameBytes);
            }
            synchronized (lock) {
                BulkFrameStream.write(output, frameBytes);
                return BulkWriteAckCodec.decode(BulkWriteAckStream.read(input)
                        .orElseThrow(() -> new IOException("bulk write stream ended before ack")));
            }
        }

        @Override
        public BulkWriteAck sendWriteFrame(BulkDataFrame frame) throws IOException {
            if (frame == null) {
                throw new IllegalArgumentException("frame is required");
            }
            if (frameSender != null) {
                return frameSender.send(frame);
            }
            synchronized (lock) {
                BulkFrameStream.write(output, frame);
                return BulkWriteAckCodec.decode(BulkWriteAckStream.read(input)
                        .orElseThrow(() -> new IOException("bulk write stream ended before ack")));
            }
        }

        @Override
        public List<BulkWriteAck> sendWriteDataFrames(List<BulkDataFrame> frames) throws IOException {
            if (frames == null) {
                throw new IllegalArgumentException("frames are required");
            }
            if (frames.isEmpty()) {
                return List.of();
            }
            if (frameSender != null) {
                return frameSender.sendFrames(frames);
            }
            synchronized (lock) {
                for (BulkDataFrame frame : frames) {
                    BulkFrameStream.write(output, frame);
                }
                List<BulkWriteAck> acks = new ArrayList<>(frames.size());
                for (int index = 0; index < frames.size(); index++) {
                    acks.add(BulkWriteAckCodec.decode(BulkWriteAckStream.read(input)
                            .orElseThrow(() -> new IOException("bulk write stream ended before ack"))));
                }
                return List.copyOf(acks);
            }
        }

        @Override
        public List<BulkWriteAck> sendWriteFrames(
                List<byte[]> frameBytes,
                List<BulkWriteFrameInfo> expectedFrames
        ) throws IOException {
            if (frameBytes == null) {
                throw new IllegalArgumentException("frameBytes are required");
            }
            if (expectedFrames != null && expectedFrames.size() != frameBytes.size()) {
                throw new IllegalArgumentException("expectedFrames must match frameBytes size");
            }
            if (frameBytes.isEmpty()) {
                return List.of();
            }
            List<BulkWriteAck> acks = frameSender == null
                    ? sendEncodedFramesOnPrimary(frameBytes)
                    : frameSender.sendEncodedFrames(frameBytes);
            if (expectedFrames != null) {
                for (int index = 0; index < expectedFrames.size(); index++) {
                    expectedFrames.get(index).verify(acks.get(index));
                }
            }
            return acks;
        }

        private List<BulkWriteAck> sendEncodedFramesOnPrimary(List<byte[]> frameBytes) throws IOException {
            synchronized (lock) {
                for (byte[] frame : frameBytes) {
                    BulkFrameStream.write(output, frame);
                }
                List<BulkWriteAck> acks = new ArrayList<>(frameBytes.size());
                for (int index = 0; index < frameBytes.size(); index++) {
                    acks.add(BulkWriteAckCodec.decode(BulkWriteAckStream.read(input)
                            .orElseThrow(() -> new IOException("bulk write stream ended before ack"))));
                }
                return List.copyOf(acks);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class BulkWriteSocketPool implements BulkFrameSender, AutoCloseable {
        private final SocketFactory socketFactory;
        private final InetSocketAddress address;
        private final CxnFileNetworkClientConfig config;
        private final int maxSockets;
        private final BlockingQueue<Socket> availableSockets;
        private final Set<Socket> openedSockets = ConcurrentHashMap.newKeySet();
        private final Object createLock = new Object();
        private int openedCount;
        private volatile boolean closed;

        private BulkWriteSocketPool(
                SocketFactory socketFactory,
                InetSocketAddress address,
                CxnFileNetworkClientConfig config,
                int maxSockets
        ) {
            if (maxSockets <= 0) {
                throw new IllegalArgumentException("maxSockets must be positive");
            }
            this.socketFactory = socketFactory;
            this.address = address;
            this.config = config;
            this.maxSockets = maxSockets;
            this.availableSockets = new ArrayBlockingQueue<>(maxSockets);
        }

        @Override
        public BulkWriteAck send(byte[] frameBytes) throws IOException {
            Socket socket = lease();
            boolean reusable = false;
            try {
                BulkFrameStream.write(socket.getOutputStream(), frameBytes);
                BulkWriteAck ack = BulkWriteAckCodec.decode(BulkWriteAckStream.read(socket.getInputStream())
                        .orElseThrow(() -> new IOException("bulk write stream ended before ack")));
                reusable = true;
                return ack;
            } finally {
                if (reusable) {
                    release(socket);
                } else {
                    discard(socket);
                }
            }
        }

        @Override
        public BulkWriteAck send(BulkDataFrame frame) throws IOException {
            Socket socket = lease();
            boolean reusable = false;
            try {
                BulkFrameStream.write(socket.getOutputStream(), frame);
                BulkWriteAck ack = BulkWriteAckCodec.decode(BulkWriteAckStream.read(socket.getInputStream())
                        .orElseThrow(() -> new IOException("bulk write stream ended before ack")));
                reusable = true;
                return ack;
            } finally {
                if (reusable) {
                    release(socket);
                } else {
                    discard(socket);
                }
            }
        }

        @Override
        public List<BulkWriteAck> sendFrames(List<BulkDataFrame> frames) throws IOException {
            if (frames == null) {
                throw new IllegalArgumentException("frames are required");
            }
            if (frames.isEmpty()) {
                return List.of();
            }
            Socket socket = lease();
            boolean reusable = false;
            try {
                for (BulkDataFrame frame : frames) {
                    BulkFrameStream.write(socket.getOutputStream(), frame);
                }
                List<BulkWriteAck> acks = new ArrayList<>(frames.size());
                for (int index = 0; index < frames.size(); index++) {
                    acks.add(BulkWriteAckCodec.decode(BulkWriteAckStream.read(socket.getInputStream())
                            .orElseThrow(() -> new IOException("bulk write stream ended before ack"))));
                }
                reusable = true;
                return List.copyOf(acks);
            } finally {
                if (reusable) {
                    release(socket);
                } else {
                    discard(socket);
                }
            }
        }

        @Override
        public List<BulkWriteAck> sendEncodedFrames(List<byte[]> frameBytes) throws IOException {
            if (frameBytes == null) {
                throw new IllegalArgumentException("frameBytes are required");
            }
            if (frameBytes.isEmpty()) {
                return List.of();
            }
            Socket socket = lease();
            boolean reusable = false;
            try {
                for (byte[] frame : frameBytes) {
                    BulkFrameStream.write(socket.getOutputStream(), frame);
                }
                List<BulkWriteAck> acks = new ArrayList<>(frameBytes.size());
                for (int index = 0; index < frameBytes.size(); index++) {
                    acks.add(BulkWriteAckCodec.decode(BulkWriteAckStream.read(socket.getInputStream())
                            .orElseThrow(() -> new IOException("bulk write stream ended before ack"))));
                }
                reusable = true;
                return List.copyOf(acks);
            } finally {
                if (reusable) {
                    release(socket);
                } else {
                    discard(socket);
                }
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            IOException failure = null;
            availableSockets.clear();
            for (Socket socket : openedSockets) {
                try {
                    socket.close();
                } catch (IOException e) {
                    failure = e;
                }
            }
            openedSockets.clear();
            synchronized (createLock) {
                openedCount = 0;
            }
            if (failure != null) {
                throw failure;
            }
        }

        private Socket lease() throws IOException {
            if (closed) {
                throw new IOException("bulk write socket pool is closed");
            }
            Socket socket = availableSockets.poll();
            if (socket != null) {
                return socket;
            }
            synchronized (createLock) {
                if (closed) {
                    throw new IOException("bulk write socket pool is closed");
                }
                if (openedCount < maxSockets) {
                    Socket created = openConnectedSocket(socketFactory, address, config);
                    openedSockets.add(created);
                    openedCount++;
                    return created;
                }
            }
            while (!closed) {
                try {
                    socket = availableSockets.poll(250, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting for a bulk write socket", e);
                }
                if (socket != null) {
                    if (closed) {
                        discard(socket);
                        throw new IOException("bulk write socket pool is closed");
                    }
                    return socket;
                }
            }
            throw new IOException("bulk write socket pool is closed");
        }

        private void release(Socket socket) {
            if (closed || !availableSockets.offer(socket)) {
                discard(socket);
            }
        }

        private void discard(Socket socket) {
            boolean removed = openedSockets.remove(socket);
            if (removed) {
                synchronized (createLock) {
                    openedCount--;
                }
            }
            closeQuietly(socket);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IOException failure = null;
        try {
            closeAction.close();
        } catch (IOException e) {
            failure = e;
        } catch (Exception e) {
            failure = new IOException("failed to close network client", e);
        }
        try {
            bulkChannel.close();
        } catch (IOException e) {
            failure = e;
        } catch (Exception e) {
            failure = new IOException("failed to close bulk channel", e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeClientResources(Socket first, Socket second, AutoCloseable third) throws IOException {
        IOException failure = null;
        try {
            first.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            second.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            third.close();
        } catch (IOException e) {
            failure = e;
        } catch (Exception e) {
            failure = new IOException("failed to close bulk write socket pool", e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best effort while preserving the original connection failure.
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best effort while preserving the original connection failure.
        }
    }

    @FunctionalInterface
    private interface BulkFrameSender {
        BulkWriteAck send(byte[] frameBytes) throws IOException;

        default BulkWriteAck send(BulkDataFrame frame) throws IOException {
            return send(BulkDataFrameCodec.encode(frame));
        }

        default List<BulkWriteAck> sendFrames(List<BulkDataFrame> frames) throws IOException {
            if (frames == null) {
                throw new IllegalArgumentException("frames are required");
            }
            List<BulkWriteAck> acks = new ArrayList<>(frames.size());
            for (BulkDataFrame frame : frames) {
                acks.add(send(frame));
            }
            return List.copyOf(acks);
        }

        default List<BulkWriteAck> sendEncodedFrames(List<byte[]> frameBytes) throws IOException {
            if (frameBytes == null) {
                throw new IllegalArgumentException("frameBytes are required");
            }
            List<BulkWriteAck> acks = new ArrayList<>(frameBytes.size());
            for (byte[] frame : frameBytes) {
                acks.add(send(frame));
            }
            return List.copyOf(acks);
        }
    }
}
