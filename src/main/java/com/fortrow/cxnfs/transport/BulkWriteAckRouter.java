package com.fortrow.cxnfs.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class BulkWriteAckRouter {
    private final ConcurrentMap<UUID, AckSubscriber> subscribers = new ConcurrentHashMap<>();

    void serve(BulkWriteAckSubscription subscription, InputStream input, OutputStream output) throws IOException {
        if (subscription == null) {
            throw new IllegalArgumentException("subscription is required");
        }
        if (input == null) {
            throw new IllegalArgumentException("input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("output is required");
        }
        AckSubscriber subscriber = new AckSubscriber(output);
        AckSubscriber prior = subscribers.put(subscription.transferId(), subscriber);
        if (prior != null) {
            prior.close();
        }
        try {
            BulkWriteAckStream.write(output, BulkWriteAckSubscriptionCodec.encodeReady(subscription.transferId()));
            while (BulkTransferMessageStream.read(input).isPresent()) {
                // The client keeps this stream open while data streams publish ACKs to it.
            }
        } finally {
            subscribers.remove(subscription.transferId(), subscriber);
            subscriber.close();
        }
    }

    boolean publish(BulkWriteAck ack) throws IOException {
        if (ack == null) {
            throw new IllegalArgumentException("ack is required");
        }
        AckSubscriber subscriber = subscribers.get(ack.key().transferId());
        if (subscriber == null) {
            return false;
        }
        subscriber.write(ack);
        return true;
    }

    int subscriberCount() {
        return subscribers.size();
    }

    private static final class AckSubscriber {
        private final OutputStream output;
        private boolean closed;

        private AckSubscriber(OutputStream output) {
            this.output = output;
        }

        private synchronized void write(BulkWriteAck ack) throws IOException {
            if (closed) {
                throw new IOException("bulk write ack subscriber is closed");
            }
            BulkWriteAckStream.write(output, BulkWriteAckCodec.encode(ack));
        }

        private synchronized void close() {
            closed = true;
        }
    }
}
