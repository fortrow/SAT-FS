package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.performance.ReadAheadPolicy;
import com.fortrow.cxnfs.transport.CordPoolConfig;

import java.nio.file.Path;

public record EarlyReadStoragePolicy(
        Path osStructureRoot,
        ReadAheadPolicy readAheadPolicy,
        CordPoolConfig cordPool,
        boolean structuresLiveOnOsDisk,
        boolean allowPayloadBeforeTrailingMetadata,
        boolean requireTrailingMetadataBeforePublish
) {
    public static EarlyReadStoragePolicy serverHostDefaults(Path osStructureRoot) {
        return new EarlyReadStoragePolicy(
                osStructureRoot,
                ReadAheadPolicy.linuxAiTrainingDataset(),
                CordPoolConfig.xetLikeDefaults(),
                true,
                true,
                true
        );
    }

    public EarlyReadStoragePolicy {
        if (osStructureRoot == null) {
            throw new IllegalArgumentException("osStructureRoot is required");
        }
        if (readAheadPolicy == null) {
            throw new IllegalArgumentException("readAheadPolicy is required");
        }
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        osStructureRoot = osStructureRoot.normalize();
        if (allowPayloadBeforeTrailingMetadata && !structuresLiveOnOsDisk) {
            throw new IllegalArgumentException("early reads require trailing structures from the OS disk");
        }
        if (allowPayloadBeforeTrailingMetadata && !requireTrailingMetadataBeforePublish) {
            throw new IllegalArgumentException("early reads must require trailing metadata before publish");
        }
    }
}
