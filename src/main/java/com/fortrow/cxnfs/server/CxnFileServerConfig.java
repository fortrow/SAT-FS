package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.auth.AuthenticationPolicy;
import com.fortrow.cxnfs.transport.PayloadStagingConfig;
import com.fortrow.cxnfs.transport.TransportBinding;

import java.nio.file.Path;
import java.util.List;

public record CxnFileServerConfig(
        Path servedDirectory,
        List<ServedRoot> servedRoots,
        AuthenticationPolicy authenticationPolicy,
        TransportBinding controlBinding,
        TransportBinding containerTransferBinding,
        PayloadStagingConfig payloadStagingConfig,
        LiveWriteMode liveWriteMode
) {
    public CxnFileServerConfig(
            Path servedDirectory,
            AuthenticationPolicy authenticationPolicy,
            TransportBinding controlBinding,
            TransportBinding containerTransferBinding
    ) {
        this(
                servedDirectory,
                authenticationPolicy,
                controlBinding,
                containerTransferBinding,
                PayloadStagingConfig.serverDefault(),
                LiveWriteMode.DEFAULT
        );
    }

    public CxnFileServerConfig(
            Path servedDirectory,
            AuthenticationPolicy authenticationPolicy,
            TransportBinding controlBinding,
            TransportBinding containerTransferBinding,
            PayloadStagingConfig payloadStagingConfig
    ) {
        this(
                servedDirectory,
                authenticationPolicy,
                controlBinding,
                containerTransferBinding,
                payloadStagingConfig,
                LiveWriteMode.DEFAULT
        );
    }

    public CxnFileServerConfig(
            Path servedDirectory,
            AuthenticationPolicy authenticationPolicy,
            TransportBinding controlBinding,
            TransportBinding containerTransferBinding,
            PayloadStagingConfig payloadStagingConfig,
            LiveWriteMode liveWriteMode
    ) {
        this(
                servedDirectory,
                List.of(ServedRoot.root(servedDirectory)),
                authenticationPolicy,
                controlBinding,
                containerTransferBinding,
                payloadStagingConfig,
                liveWriteMode
        );
    }

    public CxnFileServerConfig(
            List<ServedRoot> servedRoots,
            AuthenticationPolicy authenticationPolicy,
            TransportBinding controlBinding,
            TransportBinding containerTransferBinding,
            PayloadStagingConfig payloadStagingConfig
    ) {
        this(
                firstRoot(servedRoots).localDirectory(),
                servedRoots,
                authenticationPolicy,
                controlBinding,
                containerTransferBinding,
                payloadStagingConfig,
                LiveWriteMode.DEFAULT
        );
    }

    public CxnFileServerConfig(
            List<ServedRoot> servedRoots,
            AuthenticationPolicy authenticationPolicy,
            TransportBinding controlBinding,
            TransportBinding containerTransferBinding,
            PayloadStagingConfig payloadStagingConfig,
            LiveWriteMode liveWriteMode
    ) {
        this(
                firstRoot(servedRoots).localDirectory(),
                servedRoots,
                authenticationPolicy,
                controlBinding,
                containerTransferBinding,
                payloadStagingConfig,
                liveWriteMode
        );
    }

    public CxnFileServerConfig {
        if (servedDirectory == null) {
            throw new IllegalArgumentException("servedDirectory is required");
        }
        if (servedRoots == null || servedRoots.isEmpty()) {
            throw new IllegalArgumentException("servedRoots is required");
        }
        servedRoots = List.copyOf(servedRoots);
        if (authenticationPolicy == null) {
            throw new IllegalArgumentException("authenticationPolicy is required");
        }
        if (controlBinding == null) {
            throw new IllegalArgumentException("controlBinding is required");
        }
        if (containerTransferBinding == null) {
            throw new IllegalArgumentException("containerTransferBinding is required");
        }
        if (payloadStagingConfig == null) {
            throw new IllegalArgumentException("payloadStagingConfig is required");
        }
        if (liveWriteMode == null) {
            throw new IllegalArgumentException("liveWriteMode is required");
        }
    }

    private static ServedRoot firstRoot(List<ServedRoot> servedRoots) {
        if (servedRoots == null || servedRoots.isEmpty()) {
            throw new IllegalArgumentException("servedRoots is required");
        }
        return servedRoots.getFirst();
    }
}
