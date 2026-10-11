package com.fortrow.cxnfs.mount;

import com.fortrow.cxnfs.client.CxnFileNetworkClientConfig;
import com.fortrow.cxnfs.namespace.NamespacePath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public record MountAdapterHandoff(
        String host,
        int controlPort,
        int transportPort,
        MountBackend requestedBackend,
        Set<MountCapability> requestedCapabilities,
        MountStrategy strategy,
        String remoteRoot,
        String mountPoint,
        CxnFileNetworkClientConfig clientConfig,
        Optional<Path> trustStorePath
) {
    public static final String FORMAT = "cxnfs.mount.handoff.v1";

    public MountAdapterHandoff {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (controlPort <= 0 || controlPort > 65535) {
            throw new IllegalArgumentException("controlPort must be between 1 and 65535");
        }
        if (transportPort <= 0 || transportPort > 65535) {
            throw new IllegalArgumentException("transportPort must be between 1 and 65535");
        }
        if (requestedBackend == null) {
            throw new IllegalArgumentException("requestedBackend is required");
        }
        requestedCapabilities = Set.copyOf(requestedCapabilities);
        if (requestedCapabilities.isEmpty()) {
            throw new IllegalArgumentException("requestedCapabilities is required");
        }
        if (strategy == null) {
            throw new IllegalArgumentException("strategy is required");
        }
        remoteRoot = NamespacePath.normalize(remoteRoot == null || remoteRoot.isBlank() ? "/" : remoteRoot);
        if (mountPoint == null || mountPoint.isBlank()) {
            throw new IllegalArgumentException("mountPoint is required");
        }
        if (clientConfig == null) {
            throw new IllegalArgumentException("clientConfig is required");
        }
        trustStorePath = trustStorePath == null ? Optional.empty() : trustStorePath;
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        return FORMAT + lineSeparator
                + "host=" + host + lineSeparator
                + "control_port=" + controlPort + lineSeparator
                + "transport_port=" + transportPort + lineSeparator
                + "requested_backend=" + backendName(requestedBackend) + lineSeparator
                + "requested_capabilities=" + capabilities(requestedCapabilities) + lineSeparator
                + "backend=" + backendName(strategy.backend()) + lineSeparator
                + "capabilities=" + capabilities(strategy.capabilities()) + lineSeparator
                + "adapter_kind=" + adapterKind(strategy.backend()) + lineSeparator
                + "adapter_binary=" + adapterBinary(strategy.backend()) + lineSeparator
                + "adapter_argument_style=handoff-file" + lineSeparator
                + "remote_root=" + remoteRoot + lineSeparator
                + "mount_point=" + mountPoint + lineSeparator
                + "initial_cords=" + clientConfig.initialCords() + lineSeparator
                + "max_cords=" + clientConfig.maxCords() + lineSeparator
                + "provisional_container_size_bytes=" + clientConfig.provisionalContainerSizeBytes() + lineSeparator
                + "expected_size_limit_bytes=" + clientConfig.expectedSizeLimitBytes() + lineSeparator
                + "connect_timeout_ms=" + clientConfig.connectTimeoutMillis() + lineSeparator
                + "read_timeout_ms=" + clientConfig.readTimeoutMillis() + lineSeparator
                + "truststore_path=" + trustStorePath.map(Path::toString).orElse("") + lineSeparator
                + "truststore_password_inline=false" + lineSeparator
                + "auth_password_inline=false" + lineSeparator;
    }

    public String adapterBinaryName() {
        return adapterBinary(strategy.backend());
    }

    public String adapterKindName() {
        return adapterKind(strategy.backend());
    }

    public static MountAdapterHandoff read(Path path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        return parse(Files.readString(path, StandardCharsets.UTF_8));
    }

    public static MountAdapterHandoff parse(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("mount handoff is empty");
        }
        List<String> lines = text.lines().toList();
        if (lines.isEmpty() || !FORMAT.equals(lines.getFirst().trim())) {
            throw new IllegalArgumentException("unsupported mount handoff format");
        }
        Map<String, String> fields = parseFields(lines);
        requireFalse(fields, "truststore_password_inline");
        requireFalse(fields, "auth_password_inline");
        MountBackend requestedBackend = backend(required(fields, "requested_backend"));
        Set<MountCapability> requestedCapabilities = capabilities(required(fields, "requested_capabilities"));
        MountBackend backend = backend(required(fields, "backend"));
        Set<MountCapability> acceptedCapabilities = capabilities(required(fields, "capabilities"));
        return new MountAdapterHandoff(
                required(fields, "host"),
                integer(fields, "control_port"),
                integer(fields, "transport_port"),
                requestedBackend,
                requestedCapabilities,
                new MountStrategy(backend, acceptedCapabilities),
                required(fields, "remote_root"),
                required(fields, "mount_point"),
                new CxnFileNetworkClientConfig(
                        integer(fields, "initial_cords"),
                        integer(fields, "max_cords"),
                        integer(fields, "provisional_container_size_bytes"),
                        longValue(fields, "expected_size_limit_bytes"),
                        integer(fields, "connect_timeout_ms"),
                        integer(fields, "read_timeout_ms")
                ),
                optionalPath(fields.get("truststore_path"))
        );
    }

    private static String adapterKind(MountBackend backend) {
        return switch (backend) {
            case LINUX_FUSE, MACOS_FUSE_T, MACOS_MACFUSE -> "fuse";
            case WINDOWS_WINFSP -> "winfsp";
            case WINDOWS_PROJFS -> "projfs";
            case CLI_LIST_ONLY -> "cli";
        };
    }

    private static String adapterBinary(MountBackend backend) {
        return switch (backend) {
            case LINUX_FUSE -> "cxnfs-fuse";
            case MACOS_FUSE_T -> "cxnfs-fuse-t";
            case MACOS_MACFUSE -> "cxnfs-macfuse";
            case WINDOWS_WINFSP -> "cxnfs-winfsp";
            case WINDOWS_PROJFS -> "cxnfs-projfs";
            case CLI_LIST_ONLY -> "cxnfs";
        };
    }

    private static String backendName(MountBackend backend) {
        return backend.name().toLowerCase(Locale.ROOT);
    }

    private static MountBackend backend(String value) {
        String normalized = normalizeToken(value);
        for (MountBackend backend : MountBackend.values()) {
            if (normalizeToken(backend.name()).equals(normalized)) {
                return backend;
            }
        }
        throw new IllegalArgumentException("unknown mount backend: " + value);
    }

    private static String capabilities(Set<MountCapability> capabilities) {
        return capabilities.stream()
                .sorted(Comparator.comparing(Enum::ordinal))
                .map(capability -> capability.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(","));
    }

    private static Set<MountCapability> capabilities(String value) {
        EnumSet<MountCapability> capabilities = EnumSet.noneOf(MountCapability.class);
        for (String token : value.split(",")) {
            String normalized = normalizeToken(token);
            if (normalized.isBlank()) {
                continue;
            }
            boolean matched = false;
            for (MountCapability capability : MountCapability.values()) {
                if (normalizeToken(capability.name()).equals(normalized)) {
                    capabilities.add(capability);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new IllegalArgumentException("unknown mount capability: " + token);
            }
        }
        if (capabilities.isEmpty()) {
            throw new IllegalArgumentException("mount handoff requires at least one capability");
        }
        return capabilities;
    }

    private static Map<String, String> parseFields(List<String> lines) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int index = 1; index < lines.size(); index++) {
            String line = lines.get(index);
            if (line == null || line.isBlank()) {
                continue;
            }
            int separator = line.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("invalid mount handoff field on line " + (index + 1));
            }
            fields.put(line.substring(0, separator), line.substring(separator + 1));
        }
        return fields;
    }

    private static String required(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mount handoff missing field: " + key);
        }
        return value;
    }

    private static int integer(Map<String, String> fields, String key) {
        return Integer.parseInt(required(fields, key));
    }

    private static long longValue(Map<String, String> fields, String key) {
        return Long.parseLong(required(fields, key));
    }

    private static void requireFalse(Map<String, String> fields, String key) {
        String value = required(fields, key);
        if (!"false".equals(value)) {
            throw new IllegalArgumentException("mount handoff rejects inline secret field: " + key);
        }
    }

    private static Optional<Path> optionalPath(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(Path.of(value));
    }

    private static String normalizeToken(String value) {
        return value == null
                ? ""
                : value.trim().toLowerCase(Locale.ROOT).replace('-', '_');
    }
}
