# Production Deployment Runbook

This runbook documents installation and deployment of SAT-FS / CXNFS across supported platforms.

## Installation

### Linux
Run the Linux installer:
```bash
sudo ./install-linux.sh --jar target/cxnfs-0.1.0.jar --lib-dir target/runtime-libs
```
This will:
- Install the launcher: `install -m 0755 packaging/bin/cxnfs /usr/local/bin/cxnfs`
- Install native FUSE adapter if present: `install -m 0755 target/native/cxnfs-fuse /usr/local/bin/cxnfs-fuse`
- Configure runtime classpath: `CXNFS_CLASSPATH=/opt/cxnfs/cxnfs.jar:/opt/cxnfs/lib/*`
- Prepare configuration from `packaging/systemd/server.properties.example`
- Initialize runtime directories and clear stale ready, health, shutdown markers

### macOS
Run the macOS installer:
```bash
sudo ./install-macos.sh --jar target/cxnfs-0.1.0.jar --lib-dir target/runtime-libs
```
To automatically download FUSE-T (headers and dylib), compile the adapter locally, and install everything in one step:
```bash
sudo ./install-macos.sh --download-fuse-t
```
Or if FUSE-T is already installed (via Homebrew `brew install --cask fuse-t` or official pkg) and you want to build the native adapter from source:
```bash
sudo ./install-macos.sh --build-fuse-t
```
Optionally installs the macOS FUSE-T adapter:
```bash
install -m 0755 target/native/cxnfs-fuse-t /usr/local/bin/cxnfs-fuse-t
```

### Windows
Run the Windows installer:
```bat
install-windows.bat --jar target\cxnfs-0.1.0.jar --lib-dir target\runtime-libs
```

## Service Registration
To install the service on the current platform using JFlux `PlatformUtils`:
```bash
cxnfs install-service
```

## Certificates And Secrets
Provision server certificates and authentication secrets securely into the configuration directory. Ensure credentials are protected and do not use insecure defaults.

## Serving A Dataset Safely
Configure read-only or staged mounts to prevent unauthorized modification of source datasets.
Monitor service logs with:
```bash
journalctl -u cxnfs.service
```

## Runtime Health Markers And Logs
The server manages state and readiness through file markers and logs:
- `/run/cxnfs/server.ready`: indicates the server has completed initialization and is accepting connections.
- `/run/cxnfs/server.health`: contains serialized health checks and metrics.
- `/run/cxnfs/server.lock`: daemon lock file preventing duplicate server instances.
- `/var/log/cxnfs/server-health.txt`: periodic health report log.
- `/var/log/cxnfs/repository-audit.txt`: repository audit status report.
- `/var/log/cxnfs/repository-maintenance.txt`: repository compaction and maintenance report.
- `/var/log/cxnfs/payload-staging-cleanup.txt`: payload staging cleanup log.

## Production Verification
Run release verification and live QUIC checks before rollout:
```bash
./packaging/release-check.sh
./packaging/live-quic-check.sh
```
