package com.fortrow.cxnfs.mount;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

public record MountAdapterLaunchPlan(
        Path handoffPath,
        MountAdapterHandoff handoff,
        String adapterBinary
) {
    public static final String FORMAT = "cxnfs.mount.launch-plan.v1";

    public MountAdapterLaunchPlan {
        if (handoffPath == null) {
            throw new IllegalArgumentException("handoffPath is required");
        }
        handoffPath = handoffPath.toAbsolutePath().normalize();
        if (handoff == null) {
            throw new IllegalArgumentException("handoff is required");
        }
        if (adapterBinary == null || adapterBinary.isBlank()) {
            throw new IllegalArgumentException("adapterBinary is required");
        }
    }

    public List<String> argv() {
        return List.of(adapterBinary, "--handoff", handoffPath.toString());
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        StringBuilder builder = new StringBuilder();
        builder.append(FORMAT).append(lineSeparator);
        builder.append("handoff_report=").append(handoffPath).append(lineSeparator);
        builder.append("backend=").append(handoff.strategy().backend().name().toLowerCase(Locale.ROOT)).append(lineSeparator);
        builder.append("adapter_kind=").append(handoff.adapterKindName()).append(lineSeparator);
        builder.append("adapter_binary=").append(adapterBinary).append(lineSeparator);
        builder.append("remote_root=").append(handoff.remoteRoot()).append(lineSeparator);
        builder.append("mount_point=").append(handoff.mountPoint()).append(lineSeparator);
        builder.append("host=").append(handoff.host()).append(lineSeparator);
        builder.append("control_port=").append(handoff.controlPort()).append(lineSeparator);
        builder.append("transport_port=").append(handoff.transportPort()).append(lineSeparator);
        builder.append("max_cords=").append(handoff.clientConfig().maxCords()).append(lineSeparator);
        List<String> argv = argv();
        builder.append("argv_count=").append(argv.size()).append(lineSeparator);
        for (int index = 0; index < argv.size(); index++) {
            builder.append("argv_").append(index).append('=').append(argv.get(index)).append(lineSeparator);
        }
        builder.append("auth_password_inline=false").append(lineSeparator);
        builder.append("truststore_password_inline=false").append(lineSeparator);
        return builder.toString();
    }
}
