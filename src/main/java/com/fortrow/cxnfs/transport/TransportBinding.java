package com.fortrow.cxnfs.transport;

import java.net.InetSocketAddress;
import java.util.Optional;

public record TransportBinding(
        TransportRole role,
        InetSocketAddress address,
        TransportSecurityConfig security,
        Optional<CordPoolConfig> cordPool,
        Optional<QuicHandshakePolicy> quicHandshakePolicy,
        Optional<FastStartTransferPolicy> fastStartTransferPolicy,
        Optional<WanTransferSecurityPolicy> wanTransferSecurityPolicy,
        Optional<WriteAdmissionPolicy> writeAdmissionPolicy,
        Optional<BulkReliabilityPolicy> bulkReliabilityPolicy
) {
    public TransportBinding(TransportRole role, InetSocketAddress address, TransportSecurityConfig security) {
        this(role, address, security, Optional.empty());
    }

    public TransportBinding(
            TransportRole role,
            InetSocketAddress address,
            TransportSecurityConfig security,
            Optional<CordPoolConfig> cordPool
    ) {
        this(role, address, security, cordPool, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public TransportBinding {
        if (role == null) {
            throw new IllegalArgumentException("role is required");
        }
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        if (security == null) {
            throw new IllegalArgumentException("security is required");
        }
        cordPool = cordPool == null ? Optional.empty() : cordPool;
        quicHandshakePolicy = quicHandshakePolicy == null ? Optional.empty() : quicHandshakePolicy;
        fastStartTransferPolicy = fastStartTransferPolicy == null ? Optional.empty() : fastStartTransferPolicy;
        wanTransferSecurityPolicy = wanTransferSecurityPolicy == null ? Optional.empty() : wanTransferSecurityPolicy;
        writeAdmissionPolicy = writeAdmissionPolicy == null ? Optional.empty() : writeAdmissionPolicy;
        bulkReliabilityPolicy = bulkReliabilityPolicy == null ? Optional.empty() : bulkReliabilityPolicy;
        if (role == TransportRole.CONTAINER_TRANSFER && cordPool.isEmpty()) {
            cordPool = Optional.of(CordPoolConfig.xetLikeDefaults());
        }
        if (role == TransportRole.CONTAINER_TRANSFER && security.mode() == SecureTransportMode.QUIC_TLS_1_3_UDP) {
            quicHandshakePolicy = quicHandshakePolicy.or(() -> Optional.of(QuicHandshakePolicy.fastStartDefaults()));
            fastStartTransferPolicy = fastStartTransferPolicy.or(() -> Optional.of(FastStartTransferPolicy.defaults()));
            wanTransferSecurityPolicy = wanTransferSecurityPolicy.or(() -> Optional.of(WanTransferSecurityPolicy.defaults()));
            writeAdmissionPolicy = writeAdmissionPolicy.or(() -> Optional.of(WriteAdmissionPolicy.defaults()));
            bulkReliabilityPolicy = bulkReliabilityPolicy.or(() -> Optional.of(BulkReliabilityPolicy.defaults()));
        }
        if (role != TransportRole.CONTAINER_TRANSFER && cordPool.isPresent()) {
            throw new IllegalArgumentException("cord pools are only valid for container transfer bindings");
        }
        if (role != TransportRole.CONTAINER_TRANSFER && quicHandshakePolicy.isPresent()) {
            throw new IllegalArgumentException("QUIC handshake policy is only valid for container transfer bindings");
        }
        if (role != TransportRole.CONTAINER_TRANSFER && fastStartTransferPolicy.isPresent()) {
            throw new IllegalArgumentException("fast-start transfer policy is only valid for container transfer bindings");
        }
        if (role != TransportRole.CONTAINER_TRANSFER && wanTransferSecurityPolicy.isPresent()) {
            throw new IllegalArgumentException("WAN transfer security policy is only valid for container transfer bindings");
        }
        if (role != TransportRole.CONTAINER_TRANSFER && writeAdmissionPolicy.isPresent()) {
            throw new IllegalArgumentException("write admission policy is only valid for container transfer bindings");
        }
        if (role != TransportRole.CONTAINER_TRANSFER && bulkReliabilityPolicy.isPresent()) {
            throw new IllegalArgumentException("bulk reliability policy is only valid for container transfer bindings");
        }
    }
}
