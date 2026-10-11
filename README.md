# SAT-FS: Saturated Async Transfer File System

[![SAT-FS CI](https://github.com/fortrow/sat-fs/actions/workflows/ci.yml/badge.svg)](https://github.com/fortrow/sat-fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-23-orange.svg)](https://openjdk.org/projects/jdk/23/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)

**SAT-FS (Saturated Async Transfer File System)** is an ultra-high-throughput, cryptographically secure (TLS 1.3 only), content-addressed distributed file system engineered to **saturate 10GbE, 25GbE, 40GbE, and 100GbE network interfaces**.

Designed for high-performance AI/ML model training (PyTorch, Safetensors, MDS-shards) and high-concurrency operating system mounts (Linux FUSE, macOS FUSE-T, Windows WinFsp), SAT-FS eliminates traditional network file system bottlenecks through multi-cord parallel transport, in-transit compression with receiver CPU expansion, and zero-staging demand streaming.

---

## Key Architecture

### 1. Saturated Asynchronous Multi-Cord Transport
Bulk data transfer is split across multiple parallel **QUIC cords over UDP**, completely decoupled from the low-latency **TLS 1.3 TCP control plane**. This prevents head-of-line blocking and allows simultaneous saturating transfers across high-bandwidth network links.

### 2. In-Transit Compression with Receiver-Side CPU Expansion
- Frames support wire-level **LZ4** and **ZSTD** block compression.
- The receiving side (client CPU / PyTorch workers / mount threads) expands the compressed data in memory across available CPU cores.
- For compressible training datasets, this achieves **2x to 3.5x effective throughput multipliers**, delivering 25–70 Gbps of in-memory data over a 10–25 Gbps physical line rate.

### 3. Native C Client (`libsatfs`) & Zero-Staging Mount
- **High-Speed Unix Domain Socket (`AF_UNIX`) & TCP**: Bypasses subprocess stdio pipes with a native C client library.
- **Connection Pooling**: Supports concurrent worker threads without global lock serialization.
- **Demand-Driven Range Streaming**: Completely drops the whole-file `/tmp` staging ("hydration") anti-pattern. Chunk ranges stream on-demand directly into OS page cache buffers.

### 4. SAT-FS Desktop GUI
Cross-platform Electron GUI (formerly Cordshock) for probing endpoints, supervising mounts, inspecting repository manifests, and visualizing real-time multi-cord throughput saturation.

---

## Repository Structure

```
.
├── src/main/java/com/fortrow/
│   ├── satfs/             # SAT-FS Java CLI entrypoint (Satfs.java)
│   └── cxnfs/             # Core server, client, chunking, and transport engine
├── python/
│   ├── src/satfs/         # SAT-FS Python package (PyTorch streaming, CLI tools)
│   └── src/cxnfs/         # Backward-compatibility shims
├── native/
│   ├── satfs_client/      # libsatfs native C client library (connection pooled)
│   ├── fuse-linux/        # Linux FUSE driver (satfs_fuse.c)
│   ├── fuse-t/            # macOS FUSE-T driver (cxnfs_fuse_t.c)
│   └── winfsp/            # Windows WinFsp driver (cxnfs_winfsp.cs)
├── gui/cordshock/         # SAT-FS Desktop Electron GUI
└── packaging/
    └── bin/               # CLI launchers (sat-fs, sat-fs.cmd)
```

---

## Quickstart

### Prerequisites
- **Java**: OpenJDK 23+
- **Maven**: 3.9+
- **Python**: 3.10+
- **C Compiler**: Clang / GCC (for native client and FUSE mounts)

### 1. Build SAT-FS
```bash
# Compile and test Java runtime
mvn clean test-compile test

# Compile native C client
gcc -Wall -Wextra -c native/satfs_client/satfs_client.c -I native/satfs_client -o /tmp/satfs_client.o
```

### 2. Run SAT-FS CLI
```bash
# Display SAT-FS version and command surface
./packaging/bin/sat-fs help

# Start a local SAT-FS server facade
./packaging/bin/sat-fs serve --control-port 4509 --bulk-port 7799 /path/to/datasets

# Mount a remote namespace directly into filesystem
./packaging/bin/sat-fs mount 127.0.0.1:4509/datasets /mnt/satfs
```

### 3. Python Integration & AI Model Training
```bash
cd python
pip install -e .

# Test protocol compatibility and transport
python -m unittest tests/test_protocol_report.py tests/test_protocol_fixtures.py tests/test_bulk.py
```

```python
import satfs

# Connect directly to SAT-FS daemon
client = satfs.SatfsControlClient.connect("127.0.0.1", 4509)
metadata = client.stat("/datasets/train-001.safetensors")
print(f"Dataset size: {metadata.size} bytes")
```

---

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [LICENSE](LICENSE).
