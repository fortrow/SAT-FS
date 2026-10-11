package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.protocol.TransferAcceptedResponse;
import com.fortrow.cxnfs.transport.BulkDataFrame;
import com.fortrow.cxnfs.transport.BulkDataFrameCodec;
import com.fortrow.cxnfs.transport.BulkReadRequest;
import com.fortrow.cxnfs.transport.BulkWriteAck;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class StripedBulkTransferClientChannel implements BulkTransferClientChannel {
    private final List<BulkTransferClientChannel> channels;

    public StripedBulkTransferClientChannel(List<? extends BulkTransferClientChannel> channels) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("channels are required");
        }
        this.channels = List.copyOf(channels);
        for (BulkTransferClientChannel channel : this.channels) {
            if (channel == null) {
                throw new IllegalArgumentException("channels must not contain null entries");
            }
        }
    }

    public int stripeCount() {
        return channels.size();
    }

    @Override
    public long read(
            BulkReadRequest request,
            TransferAcceptedResponse transfer,
            ReadPlan plan,
            OutputStream output
    ) throws IOException {
        return channels.getFirst().read(request, transfer, plan, output);
    }

    @Override
    public BulkWriteAck sendWriteFrame(byte[] frameBytes) throws IOException {
        return sendWriteFrames(List.of(frameBytes)).getFirst();
    }

    @Override
    public BulkWriteAck sendWriteFrame(BulkDataFrame frame) throws IOException {
        if (frame == null) {
            throw new IllegalArgumentException("frame is required");
        }
        int stripe = stripeForFrame(frame);
        try {
            return channels.get(stripe).sendWriteFrame(frame);
        } catch (IOException | RuntimeException e) {
            throw stripeFailure(stripe, List.of(BulkWriteFrameInfo.from(frame)), e);
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
        List<List<IndexedFrame>> groups = indexedFrameGroups(frames);
        BulkWriteAck[] acks = new BulkWriteAck[frames.size()];
        runGroups(groups, group -> {
            List<BulkDataFrame> groupFrames = group.stream().map(IndexedFrame::frame).toList();
            int stripe = stripeForFrame(group.getFirst().frame());
            List<BulkWriteAck> groupAcks;
            try {
                groupAcks = channels.get(stripe).sendWriteDataFrames(groupFrames);
            } catch (IOException | RuntimeException e) {
                throw stripeFailure(stripe, groupFrames.stream().map(BulkWriteFrameInfo::from).toList(), e);
            }
            if (groupAcks.size() != group.size()) {
                throw new IOException("striped bulk write returned " + groupAcks.size()
                        + " acks for " + group.size() + " frames");
            }
            for (int index = 0; index < group.size(); index++) {
                acks[group.get(index).index()] = groupAcks.get(index);
            }
        });
        return immutableAcks(acks);
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
        List<BulkWriteFrameInfo> safeExpected = expectedFrames == null
                ? decodeFrameInfo(frameBytes)
                : List.copyOf(expectedFrames);
        List<List<IndexedEncodedFrame>> groups = indexedEncodedFrameGroups(frameBytes, safeExpected);
        BulkWriteAck[] acks = new BulkWriteAck[frameBytes.size()];
        runGroups(groups, group -> {
            List<byte[]> groupBytes = group.stream().map(IndexedEncodedFrame::frameBytes).toList();
            List<BulkWriteFrameInfo> groupExpected = group.stream().map(IndexedEncodedFrame::expectedFrame).toList();
            int stripe = stripeForFrame(group.getFirst().expectedFrame());
            List<BulkWriteAck> groupAcks;
            try {
                groupAcks = channels.get(stripe).sendWriteFrames(groupBytes, groupExpected);
            } catch (IOException | RuntimeException e) {
                throw stripeFailure(stripe, groupExpected, e);
            }
            if (groupAcks.size() != group.size()) {
                throw new IOException("striped bulk write returned " + groupAcks.size()
                        + " acks for " + group.size() + " frames");
            }
            for (int index = 0; index < group.size(); index++) {
                acks[group.get(index).index()] = groupAcks.get(index);
            }
        });
        return immutableAcks(acks);
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (BulkTransferClientChannel channel : channels) {
            try {
                channel.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private int stripeForFrame(BulkDataFrame frame) {
        return Math.floorMod(frame.chunkSequence(), channels.size());
    }

    private int stripeForFrame(BulkWriteFrameInfo frame) {
        return Math.floorMod(frame.chunkSequence(), channels.size());
    }

    private IOException stripeFailure(int stripe, List<BulkWriteFrameInfo> frames, Throwable cause) {
        String message = "striped QUIC bulk write failed stripe=" + (stripe + 1) + "/" + channels.size()
                + BulkWriteFrameInfo.describeRange(frames)
                + ": " + cause.getMessage();
        if (cause instanceof IOException ioException) {
            return new IOException(message, ioException);
        }
        return new IOException(message, cause);
    }

    private List<List<IndexedFrame>> indexedFrameGroups(List<BulkDataFrame> frames) {
        List<List<IndexedFrame>> groups = emptyGroups();
        for (int index = 0; index < frames.size(); index++) {
            BulkDataFrame frame = frames.get(index);
            groups.get(stripeForFrame(frame)).add(new IndexedFrame(index, frame));
        }
        return groups;
    }

    private List<List<IndexedEncodedFrame>> indexedEncodedFrameGroups(
            List<byte[]> frameBytes,
            List<BulkWriteFrameInfo> expectedFrames
    ) {
        List<List<IndexedEncodedFrame>> groups = emptyGroups();
        for (int index = 0; index < frameBytes.size(); index++) {
            BulkWriteFrameInfo expectedFrame = expectedFrames.get(index);
            groups.get(stripeForFrame(expectedFrame))
                    .add(new IndexedEncodedFrame(index, frameBytes.get(index), expectedFrame));
        }
        return groups;
    }

    private <T> List<List<T>> emptyGroups() {
        List<List<T>> groups = new ArrayList<>(channels.size());
        for (int index = 0; index < channels.size(); index++) {
            groups.add(new ArrayList<>());
        }
        return groups;
    }

    private static List<BulkWriteFrameInfo> decodeFrameInfo(List<byte[]> frameBytes) throws IOException {
        List<BulkWriteFrameInfo> frames = new ArrayList<>(frameBytes.size());
        for (byte[] bytes : frameBytes) {
            frames.add(BulkWriteFrameInfo.from(BulkDataFrameCodec.decode(bytes)));
        }
        return List.copyOf(frames);
    }

    private static List<BulkWriteAck> immutableAcks(BulkWriteAck[] acks) throws IOException {
        List<BulkWriteAck> result = new ArrayList<>(acks.length);
        for (BulkWriteAck ack : acks) {
            if (ack == null) {
                throw new IOException("striped bulk write completed without an ack for one or more frames");
            }
            result.add(ack);
        }
        return List.copyOf(result);
    }

    private static <T> void runGroups(List<List<T>> groups, GroupWriter<T> writer) throws IOException {
        List<List<T>> activeGroups = groups.stream().filter(group -> !group.isEmpty()).toList();
        if (activeGroups.isEmpty()) {
            return;
        }
        if (activeGroups.size() == 1) {
            writer.write(activeGroups.getFirst());
            return;
        }
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(activeGroups.size());
            for (List<T> group : activeGroups) {
                futures.add(executor.submit(() -> {
                    writer.write(group);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                awaitGroup(future);
            }
        }
    }

    private static void awaitGroup(Future<?> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for striped bulk write", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("striped bulk write failed", cause);
        }
    }

    @FunctionalInterface
    private interface GroupWriter<T> {
        void write(List<T> group) throws IOException;
    }

    private record IndexedFrame(int index, BulkDataFrame frame) {
    }

    private record IndexedEncodedFrame(int index, byte[] frameBytes, BulkWriteFrameInfo expectedFrame) {
    }
}
