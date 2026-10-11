from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
import socket
import ssl
import struct
from threading import RLock
from typing import Protocol
from uuid import UUID, uuid4

from .auth import (
    AuthChallenge,
    AuthMechanism,
    AuthProof,
    AuthStart,
    ClientAuthenticator,
    ClientCredentials,
)
from .namespace import normalize_namespace_path
from .tls import create_client_tls_context


FRAME_MAGIC = b"CXNFSPRO"
PAYLOAD_MAGIC = b"CXNFSPAY"
FRAME_VERSION = 1
PAYLOAD_VERSION = 1
PROTOCOL_MAJOR = 0
PROTOCOL_MINOR = 1
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MAX_ENCODED_FRAME_BYTES = MAX_PAYLOAD_BYTES + 1024
MAX_TEXT_BYTES = 64 * 1024
MAX_AUTH_BYTES = 4 * 1024
MAX_LIST_ENTRIES = 100_000
MAX_CHUNK_REFS = 1_000_000
MAX_TERMS = 1_000_000
HASH_BYTES = 32
ABSOLUTE_MAX_CORDS = 128


class ControlOperation(IntEnum):
    AUTH_START = 1
    AUTH_ACCEPTED = 2
    AUTH_CHALLENGE = 3
    AUTH_PROOF = 4
    LIST_DIRECTORY = 10
    STAT_PATH = 11
    LIST_DIRECTORY_RESULT = 12
    STAT_PATH_RESULT = 13
    PUT_FILE = 20
    PUT_FILE_RESULT = 21
    GET_MANIFEST = 30
    MANIFEST_RESULT = 31
    GET_RECONSTRUCTION_PLAN = 40
    RECONSTRUCTION_PLAN_RESULT = 41
    READ_FILE_RANGE = 42
    READ_FILE_RANGE_RESULT = 43
    READ_CONTAINER_RANGE = 50
    RENAME_PATH = 60
    DELETE_PATH = 61
    RENAME_PATH_RESULT = 62
    DELETE_PATH_RESULT = 63
    MOUNT_NEGOTIATE = 70
    MOUNT_NEGOTIATED = 71
    TRANSFER_BOOTSTRAP = 80
    TRANSFER_ACCEPTED = 81
    TRANSFER_METADATA = 82
    TRANSFER_COMMIT = 83
    TRANSFER_COMMITTED = 84
    TRANSFER_ENTRY_UPDATE = 85
    TRANSFER_ENTRY_COMMIT = 86
    TRANSFER_ENTRY_COMMITTED = 87
    TRANSFER_METADATA_ACCEPTED = 88
    TRANSFER_ENTRY_ACCEPTED = 89
    ERROR = 100


class TransferDirection(IntEnum):
    READ = 1
    WRITE = 2


class TransferPayloadFormat(IntEnum):
    CONTAINER = 1
    MDS_SHARDS = 2
    SAFETENSORS = 3


class TransferEntryAction(IntEnum):
    CREATE_OR_REPLACE = 1
    DELETE = 2
    RENAME = 3


class ResponseStatus(IntEnum):
    OK = 0
    UNAUTHENTICATED = 1
    UNAUTHORIZED = 2
    NOT_FOUND = 3
    CONFLICT = 4
    INVALID_REQUEST = 5
    UNSUPPORTED = 6
    SERVER_ERROR = 7


class FileType(IntEnum):
    DIRECTORY = 1
    REGULAR_FILE = 2


class MountBackend(IntEnum):
    LINUX_FUSE = 1
    MACOS_FUSE_T = 2
    MACOS_MACFUSE = 3
    WINDOWS_WINFSP = 4
    WINDOWS_PROJFS = 5
    CLI_LIST_ONLY = 6


class MountCapability(IntEnum):
    LIST = 1
    READ = 2
    WRITE = 3


@dataclass(frozen=True)
class AuthAccepted:
    session_id: UUID
    mechanisms: tuple[AuthMechanism, ...]

    def __post_init__(self) -> None:
        if self.session_id is None:
            raise ValueError("session_id is required")
        if not self.mechanisms:
            raise ValueError("mechanisms are required")


@dataclass(frozen=True)
class ErrorResponse:
    status: ResponseStatus
    message: str = ""

    def __post_init__(self) -> None:
        if self.status == ResponseStatus.OK:
            raise ValueError("error status must be non-OK")


@dataclass(frozen=True)
class ChunkRef:
    chunk_hash: bytes
    size: int

    def __post_init__(self) -> None:
        if len(self.chunk_hash) != HASH_BYTES:
            raise ValueError("chunk_hash must be 32 bytes")
        if self.size < 0:
            raise ValueError("chunk size must be non-negative")


@dataclass(frozen=True)
class FileManifest:
    path: str
    file_hash: bytes
    size: int
    chunks: tuple[ChunkRef, ...]

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        object.__setattr__(self, "chunks", tuple(self.chunks))


@dataclass(frozen=True)
class FileMetadata:
    path: str
    name: str
    type: FileType
    size: int
    content_hash: bytes | None = None
    modified_at_seconds: int = 0
    modified_at_nanos: int = 0

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.name is None:
            raise ValueError("name is required")
        if self.type is None:
            raise ValueError("type is required")
        if self.size < 0:
            raise ValueError("size must be non-negative")
        if self.content_hash is not None and len(self.content_hash) != HASH_BYTES:
            raise ValueError("content_hash must be 32 bytes")
        if self.modified_at_nanos < 0 or self.modified_at_nanos >= 1_000_000_000:
            raise ValueError("modified_at_nanos must be in [0, 1000000000)")


@dataclass(frozen=True)
class DirectoryListing:
    path: str
    entries: tuple[FileMetadata, ...]

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        object.__setattr__(self, "entries", tuple(self.entries))


@dataclass(frozen=True)
class ManifestRequest:
    file_hash: bytes

    def __post_init__(self) -> None:
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")


@dataclass(frozen=True)
class ManifestResponse:
    manifest: FileManifest | None = None


@dataclass(frozen=True)
class ReconstructionTerm:
    container_hash: bytes
    first_chunk_index: int
    chunk_count: int

    def __post_init__(self) -> None:
        if len(self.container_hash) != HASH_BYTES:
            raise ValueError("container_hash must be 32 bytes")
        if self.first_chunk_index < 0:
            raise ValueError("first_chunk_index must be non-negative")
        if self.chunk_count <= 0:
            raise ValueError("chunk_count must be positive")


@dataclass(frozen=True)
class ReconstructionPlan:
    file_hash: bytes
    file_size: int
    terms: tuple[ReconstructionTerm, ...]

    def __post_init__(self) -> None:
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")
        if self.file_size < 0:
            raise ValueError("file_size must be non-negative")
        object.__setattr__(self, "terms", tuple(self.terms))


@dataclass(frozen=True)
class ReconstructionPlanRequest:
    file_hash: bytes

    def __post_init__(self) -> None:
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")


@dataclass(frozen=True)
class ReconstructionPlanResponse:
    plan: ReconstructionPlan

    def __post_init__(self) -> None:
        if self.plan is None:
            raise ValueError("plan is required")


@dataclass(frozen=True)
class DirectoryListRequest:
    path: str

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))


@dataclass(frozen=True)
class DirectoryListResponse:
    listing: DirectoryListing

    def __post_init__(self) -> None:
        if self.listing is None:
            raise ValueError("listing is required")


@dataclass(frozen=True)
class StatPathRequest:
    path: str

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))


@dataclass(frozen=True)
class StatPathResponse:
    metadata: FileMetadata | None = None


@dataclass(frozen=True)
class PutFileRequest:
    path: str
    size: int
    file_hash: bytes
    chunks: tuple[ChunkRef, ...]

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.size < 0:
            raise ValueError("size must be non-negative")
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")
        object.__setattr__(self, "chunks", tuple(self.chunks))


@dataclass(frozen=True)
class PutFileResponse:
    manifest: FileManifest
    missing_chunks: tuple[bytes, ...]

    def __post_init__(self) -> None:
        if self.manifest is None:
            raise ValueError("manifest is required")
        for chunk_hash in self.missing_chunks:
            if len(chunk_hash) != HASH_BYTES:
                raise ValueError("missing chunk hash must be 32 bytes")
        object.__setattr__(self, "missing_chunks", tuple(self.missing_chunks))


@dataclass(frozen=True)
class ByteRange:
    offset: int
    length: int

    def __post_init__(self) -> None:
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length < 0:
            raise ValueError("length must be non-negative")

    @property
    def end_exclusive(self) -> int:
        return self.offset + self.length


@dataclass(frozen=True)
class PlannedChunkRead:
    chunk_index: int
    chunk_hash: bytes
    file_offset: int
    chunk_size: int
    offset_in_chunk: int
    bytes_to_read: int

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ValueError("chunk_index must be non-negative")
        if len(self.chunk_hash) != HASH_BYTES:
            raise ValueError("chunk_hash must be 32 bytes")
        if self.file_offset < 0:
            raise ValueError("file_offset must be non-negative")
        if self.chunk_size < 0:
            raise ValueError("chunk_size must be non-negative")
        if self.offset_in_chunk < 0 or self.offset_in_chunk > self.chunk_size:
            raise ValueError("offset_in_chunk must be inside chunk")
        if self.bytes_to_read < 0 or self.offset_in_chunk + self.bytes_to_read > self.chunk_size:
            raise ValueError("bytes_to_read must fit inside chunk")

    @property
    def read_file_offset(self) -> int:
        return self.file_offset + self.offset_in_chunk


@dataclass(frozen=True)
class ReadPlan:
    file_hash: bytes
    file_size: int
    requested_range: ByteRange
    chunks: tuple[PlannedChunkRead, ...]

    def __post_init__(self) -> None:
        if len(self.file_hash) != HASH_BYTES:
            raise ValueError("file_hash must be 32 bytes")
        if self.file_size < 0:
            raise ValueError("file_size must be non-negative")
        if self.requested_range is None:
            raise ValueError("requested_range is required")
        object.__setattr__(self, "chunks", tuple(self.chunks))


@dataclass(frozen=True)
class ReadFileRangeRequest:
    path: str
    offset: int
    length: int

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length < 0:
            raise ValueError("length must be non-negative")


@dataclass(frozen=True)
class ReadFileRangeResponse:
    path: str
    plan: ReadPlan

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.plan is None:
            raise ValueError("plan is required")


@dataclass(frozen=True)
class ReadContainerRangeRequest:
    container_hash: bytes
    first_chunk_index: int
    chunk_count: int

    def __post_init__(self) -> None:
        if len(self.container_hash) != HASH_BYTES:
            raise ValueError("container_hash must be 32 bytes")
        if self.first_chunk_index < 0:
            raise ValueError("first_chunk_index must be non-negative")
        if self.chunk_count <= 0:
            raise ValueError("chunk_count must be positive")


@dataclass(frozen=True)
class RenamePathRequest:
    source_path: str
    target_path: str

    def __post_init__(self) -> None:
        if not self.source_path:
            raise ValueError("source_path is required")
        if not self.target_path:
            raise ValueError("target_path is required")
        object.__setattr__(self, "source_path", normalize_namespace_path(self.source_path, label="source_path"))
        object.__setattr__(self, "target_path", normalize_namespace_path(self.target_path, label="target_path"))


@dataclass(frozen=True)
class RenamePathResponse:
    source_path: str
    target_path: str
    metadata: FileMetadata

    def __post_init__(self) -> None:
        if not self.source_path:
            raise ValueError("source_path is required")
        if not self.target_path:
            raise ValueError("target_path is required")
        object.__setattr__(self, "source_path", normalize_namespace_path(self.source_path, label="source_path"))
        object.__setattr__(self, "target_path", normalize_namespace_path(self.target_path, label="target_path"))
        if self.metadata is None:
            raise ValueError("metadata is required")


@dataclass(frozen=True)
class DeletePathRequest:
    path: str
    recursive: bool = False

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))


@dataclass(frozen=True)
class DeletePathResponse:
    path: str
    deleted_entries: int

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.deleted_entries < 0:
            raise ValueError("deleted_entries must be non-negative")


@dataclass(frozen=True)
class MountStrategy:
    backend: MountBackend
    capabilities: tuple[MountCapability, ...]

    def __post_init__(self) -> None:
        if self.backend is None:
            raise ValueError("backend is required")
        if not self.capabilities:
            raise ValueError("mount strategy requires at least one capability")
        object.__setattr__(self, "capabilities", _normalize_mount_capabilities(self.capabilities))


@dataclass(frozen=True)
class MountNegotiateRequest:
    backend: MountBackend
    requested_capabilities: tuple[MountCapability, ...]

    def __post_init__(self) -> None:
        if self.backend is None:
            raise ValueError("backend is required")
        if not self.requested_capabilities:
            raise ValueError("at least one capability must be requested")
        object.__setattr__(
            self,
            "requested_capabilities",
            _normalize_mount_capabilities(self.requested_capabilities),
        )


@dataclass(frozen=True)
class MountNegotiatedResponse:
    strategy: MountStrategy

    def __post_init__(self) -> None:
        if self.strategy is None:
            raise ValueError("strategy is required")


@dataclass(frozen=True)
class TransferBootstrapRequest:
    transfer_id: UUID
    direction: TransferDirection
    path: str
    payload_format: TransferPayloadFormat
    initial_cords: int
    max_cords: int
    provisional_container_size_bytes: int
    expected_size_limit_bytes: int

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.direction is None:
            raise ValueError("direction is required")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if self.payload_format is None:
            raise ValueError("payload_format is required")
        if self.initial_cords <= 0:
            raise ValueError("initial_cords must be positive")
        if self.max_cords <= 0 or self.max_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"max_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if self.initial_cords > self.max_cords:
            raise ValueError("initial_cords must be <= max_cords")
        if self.provisional_container_size_bytes <= 0:
            raise ValueError("provisional_container_size_bytes must be positive")
        if self.expected_size_limit_bytes < 0:
            raise ValueError("expected_size_limit_bytes must be non-negative")
        if self.direction == TransferDirection.READ and self.payload_format == TransferPayloadFormat.SAFETENSORS:
            raise ValueError("read transfers do not use safetensors payloads")
        if self.direction == TransferDirection.WRITE and self.payload_format == TransferPayloadFormat.MDS_SHARDS:
            raise ValueError("write transfers do not use mds-shards payloads")


@dataclass(frozen=True)
class TransferAcceptedResponse:
    transfer_id: UUID
    direction: TransferDirection
    accepted_initial_cords: int
    accepted_max_cords: int
    flow_control_window_bytes: int
    write_payload_admission_granted: bool

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.direction is None:
            raise ValueError("direction is required")
        if self.accepted_initial_cords <= 0:
            raise ValueError("accepted_initial_cords must be positive")
        if self.accepted_max_cords <= 0 or self.accepted_max_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"accepted_max_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if self.accepted_initial_cords > self.accepted_max_cords:
            raise ValueError("accepted_initial_cords must be <= accepted_max_cords")
        if self.flow_control_window_bytes <= 0:
            raise ValueError("flow_control_window_bytes must be positive")
        if self.direction == TransferDirection.READ and self.write_payload_admission_granted:
            raise ValueError("read transfers cannot admit write payloads")


@dataclass(frozen=True)
class TransferEntryUpdate:
    transfer_id: UUID
    entry_sequence: int
    action: TransferEntryAction
    path: str
    target_path: str = ""
    size_hint_bytes: int = 0
    payload_format: TransferPayloadFormat = TransferPayloadFormat.SAFETENSORS

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if self.action is None:
            raise ValueError("action is required")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        target_path = self.target_path or ""
        if target_path:
            target_path = normalize_namespace_path(target_path, label="target_path")
        object.__setattr__(self, "target_path", target_path)
        if self.action == TransferEntryAction.RENAME and not target_path:
            raise ValueError("target_path is required for rename")
        if self.size_hint_bytes < 0:
            raise ValueError("size_hint_bytes must be non-negative")
        if self.payload_format is None:
            raise ValueError("payload_format is required")


@dataclass(frozen=True)
class TransferEntryAcceptedResponse:
    transfer_id: UUID
    entry_sequence: int
    path: str

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))


@dataclass(frozen=True)
class TransferMetadataUpdate:
    transfer_id: UUID
    entry_sequence: int
    metadata_sequence: int
    final_metadata: bool
    bytes_covered: int
    chunk_hashes: tuple[bytes, ...]

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if self.metadata_sequence < 0:
            raise ValueError("metadata_sequence must be non-negative")
        if self.bytes_covered < 0:
            raise ValueError("bytes_covered must be non-negative")
        for chunk_hash in self.chunk_hashes:
            if len(chunk_hash) != HASH_BYTES:
                raise ValueError("chunk hash must be 32 bytes")
        if self.final_metadata and not self.chunk_hashes and self.bytes_covered > 0:
            raise ValueError("final metadata for non-empty data must include chunk hashes")
        object.__setattr__(self, "chunk_hashes", tuple(self.chunk_hashes))


@dataclass(frozen=True)
class TransferMetadataAcceptedResponse:
    transfer_id: UUID
    entry_sequence: int
    metadata_sequence: int
    chunk_hashes_accepted: int
    staged_payloads_verified: int
    final_metadata: bool

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if self.metadata_sequence < 0:
            raise ValueError("metadata_sequence must be non-negative")
        if self.chunk_hashes_accepted < 0:
            raise ValueError("chunk_hashes_accepted must be non-negative")
        if self.staged_payloads_verified < 0:
            raise ValueError("staged_payloads_verified must be non-negative")


@dataclass(frozen=True)
class TransferChecksum:
    algorithm: str = ""
    value: int = 0

    @classmethod
    def none(cls) -> "TransferChecksum":
        return cls("", 0)

    @classmethod
    def crc32c(cls, value: int) -> "TransferChecksum":
        return cls("crc32c", value & 0xFFFFFFFF)

    @property
    def present(self) -> bool:
        return bool(self.algorithm and self.algorithm.strip())


@dataclass(frozen=True)
class TransferEntryCommitRequest:
    transfer_id: UUID
    entry_sequence: int
    path: str
    content_hash: bytes
    bytes_written: int
    checksum: TransferChecksum = field(default_factory=TransferChecksum.none)

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if len(self.content_hash) != HASH_BYTES:
            raise ValueError("content_hash must be 32 bytes")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")


@dataclass(frozen=True)
class TransferEntryCommittedResponse:
    transfer_id: UUID
    entry_sequence: int
    path: str
    content_hash: bytes
    bytes_written: int
    checksum: TransferChecksum = field(default_factory=TransferChecksum.none)

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if len(self.content_hash) != HASH_BYTES:
            raise ValueError("content_hash must be 32 bytes")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")


@dataclass(frozen=True)
class TransferCommitRequest:
    transfer_id: UUID
    path: str
    content_hash: bytes
    bytes_written: int

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if not self.path:
            raise ValueError("path is required")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))
        if len(self.content_hash) != HASH_BYTES:
            raise ValueError("content_hash must be 32 bytes")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")


@dataclass(frozen=True)
class TransferCommittedResponse:
    transfer_id: UUID
    content_hash: bytes
    bytes_written: int

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if len(self.content_hash) != HASH_BYTES:
            raise ValueError("content_hash must be 32 bytes")
        if self.bytes_written < 0:
            raise ValueError("bytes_written must be non-negative")


@dataclass(frozen=True)
class ControlFrame:
    operation: ControlOperation
    payload: bytes
    message_id: UUID
    session_id: UUID | None = None
    protocol_major: int = PROTOCOL_MAJOR
    protocol_minor: int = PROTOCOL_MINOR

    def __post_init__(self) -> None:
        if len(self.payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("control frame payload exceeds maximum size")


class ControlTransport(Protocol):
    def exchange(self, request_frame: bytes) -> bytes:
        """Send one encoded frame and return one encoded response frame."""

    def close(self) -> None:
        """Close transport resources."""


class SocketControlTransport:
    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock
        self._lock = RLock()

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        *,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> SocketControlTransport:
        raw_socket = socket.create_connection((host, port), timeout=timeout)
        context = tls_context if tls_context is not None else create_client_tls_context()
        try:
            tls_socket = context.wrap_socket(raw_socket, server_hostname=server_hostname or host)
        except Exception:
            raw_socket.close()
            raise
        return cls(tls_socket)

    def exchange(self, request_frame: bytes) -> bytes:
        with self._lock:
            write_encoded_frame(self._socket, request_frame)
            return read_encoded_frame(self._socket)

    def close(self) -> None:
        with self._lock:
            self._socket.close()


class CxnfsControlClient:
    def __init__(self, transport: ControlTransport) -> None:
        self._transport = transport
        self.session_id: UUID | None = None

    @classmethod
    def connect(
        cls,
        host: str,
        port: int = 4509,
        *,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> CxnfsControlClient:
        return cls(SocketControlTransport.connect(
            host,
            port,
            timeout=timeout,
            tls_context=tls_context,
            server_hostname=server_hostname,
        ))

    def authenticate(self, credentials: ClientCredentials) -> AuthAccepted:
        authenticator = ClientAuthenticator()
        challenge = self.exchange(authenticator.start(credentials))
        if not isinstance(challenge, AuthChallenge):
            raise TypeError(f"expected AuthChallenge, got {type(challenge).__name__}")
        accepted = self.exchange(authenticator.answer(challenge, credentials))
        if not isinstance(accepted, AuthAccepted):
            raise TypeError(f"expected AuthAccepted, got {type(accepted).__name__}")
        self.session_id = accepted.session_id
        return accepted

    def read_file_range(self, path: str, offset: int, length: int) -> ReadFileRangeResponse:
        if self.session_id is None:
            raise ValueError("authenticate before requesting read plans")
        request = ReadFileRangeRequest(path, offset, length)
        response = self.exchange(
            request,
            session_id=self.session_id,
        )
        if not isinstance(response, ReadFileRangeResponse):
            raise TypeError(f"expected ReadFileRangeResponse, got {type(response).__name__}")
        if response.path != request.path:
            raise ValueError("control read response path does not match requested read")
        if (
            response.plan.requested_range.offset != request.offset
            or response.plan.requested_range.length != request.length
        ):
            raise ValueError("control read plan range does not match requested read")
        return response

    def list_directory(self, path: str) -> DirectoryListResponse:
        if self.session_id is None:
            raise ValueError("authenticate before listing directories")
        response = self.exchange(DirectoryListRequest(path), session_id=self.session_id)
        if not isinstance(response, DirectoryListResponse):
            raise TypeError(f"expected DirectoryListResponse, got {type(response).__name__}")
        return response

    def stat_path(self, path: str) -> StatPathResponse:
        if self.session_id is None:
            raise ValueError("authenticate before stat requests")
        response = self.exchange(StatPathRequest(path), session_id=self.session_id)
        if not isinstance(response, StatPathResponse):
            raise TypeError(f"expected StatPathResponse, got {type(response).__name__}")
        return response

    def manifest(self, file_hash: bytes | bytearray | memoryview) -> ManifestResponse:
        if self.session_id is None:
            raise ValueError("authenticate before manifest requests")
        response = self.exchange(ManifestRequest(bytes(file_hash)), session_id=self.session_id)
        if not isinstance(response, ManifestResponse):
            raise TypeError(f"expected ManifestResponse, got {type(response).__name__}")
        return response

    def reconstruction_plan(self, file_hash: bytes | bytearray | memoryview) -> ReconstructionPlanResponse:
        if self.session_id is None:
            raise ValueError("authenticate before reconstruction plan requests")
        response = self.exchange(ReconstructionPlanRequest(bytes(file_hash)), session_id=self.session_id)
        if not isinstance(response, ReconstructionPlanResponse):
            raise TypeError(f"expected ReconstructionPlanResponse, got {type(response).__name__}")
        return response

    def prepare_put_file(
        self,
        path: str,
        size: int,
        file_hash: bytes | bytearray | memoryview,
        chunks: tuple[ChunkRef, ...],
    ) -> PutFileResponse:
        if self.session_id is None:
            raise ValueError("authenticate before put file preflight")
        response = self.exchange(
            PutFileRequest(path, size, bytes(file_hash), chunks),
            session_id=self.session_id,
        )
        if not isinstance(response, PutFileResponse):
            raise TypeError(f"expected PutFileResponse, got {type(response).__name__}")
        return response

    def rename_path(self, source_path: str, target_path: str) -> RenamePathResponse:
        if self.session_id is None:
            raise ValueError("authenticate before rename requests")
        response = self.exchange(RenamePathRequest(source_path, target_path), session_id=self.session_id)
        if not isinstance(response, RenamePathResponse):
            raise TypeError(f"expected RenamePathResponse, got {type(response).__name__}")
        return response

    def delete_path(self, path: str, recursive: bool = False) -> DeletePathResponse:
        if self.session_id is None:
            raise ValueError("authenticate before delete requests")
        response = self.exchange(DeletePathRequest(path, recursive), session_id=self.session_id)
        if not isinstance(response, DeletePathResponse):
            raise TypeError(f"expected DeletePathResponse, got {type(response).__name__}")
        return response

    def mount_negotiate(
        self,
        backend: MountBackend,
        requested_capabilities: tuple[MountCapability, ...],
    ) -> MountNegotiatedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before mount negotiation")
        response = self.exchange(
            MountNegotiateRequest(backend, requested_capabilities),
            session_id=self.session_id,
        )
        if not isinstance(response, MountNegotiatedResponse):
            raise TypeError(f"expected MountNegotiatedResponse, got {type(response).__name__}")
        return response

    def bootstrap_read_transfer(
        self,
        path: str,
        *,
        payload_format: TransferPayloadFormat = TransferPayloadFormat.MDS_SHARDS,
        initial_cords: int = 1,
        max_cords: int = ABSOLUTE_MAX_CORDS,
        provisional_container_size_bytes: int = 64 * 1024 * 1024,
        expected_size_limit_bytes: int = 0,
        transfer_id: UUID | None = None,
    ) -> TransferAcceptedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before bootstrapping transfers")
        request = TransferBootstrapRequest(
            transfer_id or uuid4(),
            TransferDirection.READ,
            path,
            payload_format,
            initial_cords,
            max_cords,
            provisional_container_size_bytes,
            expected_size_limit_bytes,
        )
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferAcceptedResponse):
            raise TypeError(f"expected TransferAcceptedResponse, got {type(response).__name__}")
        _require_transfer_admission_matches_request(request, response)
        return response

    def bootstrap_write_transfer(
        self,
        path: str,
        *,
        payload_format: TransferPayloadFormat = TransferPayloadFormat.SAFETENSORS,
        initial_cords: int = 1,
        max_cords: int = ABSOLUTE_MAX_CORDS,
        provisional_container_size_bytes: int = 64 * 1024 * 1024,
        expected_size_limit_bytes: int = 0,
        transfer_id: UUID | None = None,
    ) -> TransferAcceptedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before bootstrapping transfers")
        request = TransferBootstrapRequest(
            transfer_id or uuid4(),
            TransferDirection.WRITE,
            path,
            payload_format,
            initial_cords,
            max_cords,
            provisional_container_size_bytes,
            expected_size_limit_bytes,
        )
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferAcceptedResponse):
            raise TypeError(f"expected TransferAcceptedResponse, got {type(response).__name__}")
        _require_transfer_admission_matches_request(request, response)
        return response

    def accept_write_entry(
        self,
        transfer_id: UUID,
        entry_sequence: int,
        action: TransferEntryAction,
        path: str,
        *,
        target_path: str = "",
        size_hint_bytes: int = 0,
        payload_format: TransferPayloadFormat = TransferPayloadFormat.SAFETENSORS,
    ) -> TransferEntryAcceptedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before sending write metadata")
        request = TransferEntryUpdate(
            transfer_id,
            entry_sequence,
            action,
            path,
            target_path,
            size_hint_bytes,
            payload_format,
        )
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferEntryAcceptedResponse):
            raise TypeError(f"expected TransferEntryAcceptedResponse, got {type(response).__name__}")
        _require_write_entry_ack_matches_request(request, response)
        return response

    def accept_write_metadata(
        self,
        transfer_id: UUID,
        entry_sequence: int,
        metadata_sequence: int,
        final_metadata: bool,
        bytes_covered: int,
        chunk_hashes: tuple[bytes, ...],
    ) -> TransferMetadataAcceptedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before sending write metadata")
        request = TransferMetadataUpdate(
            transfer_id,
            entry_sequence,
            metadata_sequence,
            final_metadata,
            bytes_covered,
            chunk_hashes,
        )
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferMetadataAcceptedResponse):
            raise TypeError(f"expected TransferMetadataAcceptedResponse, got {type(response).__name__}")
        _require_write_metadata_ack_matches_request(request, response)
        return response

    def commit_write_entry(
        self,
        transfer_id: UUID,
        entry_sequence: int,
        path: str,
        content_hash: bytes | bytearray | memoryview,
        bytes_written: int,
    ) -> TransferEntryCommittedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before committing write entries")
        request = TransferEntryCommitRequest(
            transfer_id,
            entry_sequence,
            path,
            bytes(content_hash),
            bytes_written,
        )
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferEntryCommittedResponse):
            raise TypeError(f"expected TransferEntryCommittedResponse, got {type(response).__name__}")
        _require_write_entry_commit_matches_request(request, response)
        return response

    def commit_write_transfer(
        self,
        transfer_id: UUID,
        path: str,
        content_hash: bytes | bytearray | memoryview,
        bytes_written: int,
    ) -> TransferCommittedResponse:
        if self.session_id is None:
            raise ValueError("authenticate before committing write transfers")
        request = TransferCommitRequest(transfer_id, path, bytes(content_hash), bytes_written)
        response = self.exchange(request, session_id=self.session_id)
        if not isinstance(response, TransferCommittedResponse):
            raise TypeError(f"expected TransferCommittedResponse, got {type(response).__name__}")
        _require_write_transfer_commit_matches_request(request, response)
        return response

    def exchange(
        self,
        payload: AuthStart | AuthProof | DirectoryListRequest | StatPathRequest | ManifestRequest | ReconstructionPlanRequest | PutFileRequest | ReadFileRangeRequest | ReadContainerRangeRequest | RenamePathRequest | DeletePathRequest | MountNegotiateRequest | TransferBootstrapRequest | TransferEntryUpdate | TransferMetadataUpdate | TransferEntryCommitRequest | TransferCommitRequest,
        *,
        session_id: UUID | None = None,
    ) -> AuthChallenge | AuthAccepted | DirectoryListResponse | StatPathResponse | ManifestResponse | ReconstructionPlanResponse | PutFileResponse | ReadFileRangeResponse | RenamePathResponse | DeletePathResponse | MountNegotiatedResponse | TransferAcceptedResponse | TransferEntryAcceptedResponse | TransferMetadataAcceptedResponse | TransferEntryCommittedResponse | TransferCommittedResponse | ErrorResponse:
        encoded_payload = encode_control_payload(payload)
        frame = ControlFrame(
            operation_for_payload(payload),
            encoded_payload,
            uuid4(),
            session_id,
        )
        response_frame = decode_control_frame(self._transport.exchange(encode_control_frame(frame)))
        response_payload = decode_control_payload(response_frame.operation, response_frame.payload)
        if isinstance(response_payload, ErrorResponse):
            raise CxnfsControlError(response_payload.status, response_payload.message)
        return response_payload

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> CxnfsControlClient:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


class CxnfsControlError(RuntimeError):
    def __init__(self, status: ResponseStatus, message: str) -> None:
        super().__init__(f"{status.name}: {message}")
        self.status = status
        self.message = message


def _require_transfer_admission_matches_request(
    request: TransferBootstrapRequest,
    response: TransferAcceptedResponse,
) -> None:
    if response.transfer_id != request.transfer_id:
        raise ValueError("control transfer admission id does not match requested transfer")
    if request.direction == TransferDirection.READ:
        if response.direction != TransferDirection.READ or response.write_payload_admission_granted:
            raise ValueError("control read transfer admission does not match requested read")
        return
    if request.direction == TransferDirection.WRITE:
        if response.direction != TransferDirection.WRITE or not response.write_payload_admission_granted:
            raise ValueError("control write transfer admission does not match requested write")
        return
    raise ValueError("unsupported transfer direction")


def _require_write_entry_ack_matches_request(
    request: TransferEntryUpdate,
    response: TransferEntryAcceptedResponse,
) -> None:
    if (
        response.transfer_id != request.transfer_id
        or response.entry_sequence != request.entry_sequence
        or response.path != request.path
    ):
        raise ValueError("write entry acknowledgement does not match request")


def _require_write_metadata_ack_matches_request(
    request: TransferMetadataUpdate,
    response: TransferMetadataAcceptedResponse,
) -> None:
    if (
        response.transfer_id != request.transfer_id
        or response.entry_sequence != request.entry_sequence
        or response.metadata_sequence != request.metadata_sequence
        or response.chunk_hashes_accepted != len(request.chunk_hashes)
        or response.staged_payloads_verified > response.chunk_hashes_accepted
        or response.final_metadata != request.final_metadata
    ):
        raise ValueError("write metadata acknowledgement does not match request")


def _require_write_entry_commit_matches_request(
    request: TransferEntryCommitRequest,
    response: TransferEntryCommittedResponse,
) -> None:
    if (
        response.transfer_id != request.transfer_id
        or response.entry_sequence != request.entry_sequence
        or response.path != request.path
        or response.content_hash != request.content_hash
        or response.bytes_written != request.bytes_written
    ):
        raise ValueError("write entry commit acknowledgement does not match request")


def _require_write_transfer_commit_matches_request(
    request: TransferCommitRequest,
    response: TransferCommittedResponse,
) -> None:
    if (
        response.transfer_id != request.transfer_id
        or response.content_hash != request.content_hash
        or response.bytes_written != request.bytes_written
    ):
        raise ValueError("write transfer commit acknowledgement does not match request")


def operation_for_payload(
    payload: AuthStart
    | AuthChallenge
    | AuthProof
    | AuthAccepted
    | DirectoryListRequest
    | DirectoryListResponse
    | StatPathRequest
    | StatPathResponse
    | ManifestRequest
    | ManifestResponse
    | ReconstructionPlanRequest
    | ReconstructionPlanResponse
    | PutFileRequest
    | PutFileResponse
    | ReadFileRangeRequest
    | ReadFileRangeResponse
    | ReadContainerRangeRequest
    | RenamePathRequest
    | RenamePathResponse
    | DeletePathRequest
    | DeletePathResponse
    | MountNegotiateRequest
    | MountNegotiatedResponse
    | TransferBootstrapRequest
    | TransferAcceptedResponse
    | TransferEntryUpdate
    | TransferEntryAcceptedResponse
    | TransferMetadataUpdate
    | TransferMetadataAcceptedResponse
    | TransferEntryCommitRequest
    | TransferEntryCommittedResponse
    | TransferCommitRequest
    | TransferCommittedResponse
    | ErrorResponse,
) -> ControlOperation:
    if isinstance(payload, AuthStart):
        return ControlOperation.AUTH_START
    if isinstance(payload, AuthChallenge):
        return ControlOperation.AUTH_CHALLENGE
    if isinstance(payload, AuthProof):
        return ControlOperation.AUTH_PROOF
    if isinstance(payload, AuthAccepted):
        return ControlOperation.AUTH_ACCEPTED
    if isinstance(payload, DirectoryListRequest):
        return ControlOperation.LIST_DIRECTORY
    if isinstance(payload, DirectoryListResponse):
        return ControlOperation.LIST_DIRECTORY_RESULT
    if isinstance(payload, StatPathRequest):
        return ControlOperation.STAT_PATH
    if isinstance(payload, StatPathResponse):
        return ControlOperation.STAT_PATH_RESULT
    if isinstance(payload, ManifestRequest):
        return ControlOperation.GET_MANIFEST
    if isinstance(payload, ManifestResponse):
        return ControlOperation.MANIFEST_RESULT
    if isinstance(payload, ReconstructionPlanRequest):
        return ControlOperation.GET_RECONSTRUCTION_PLAN
    if isinstance(payload, ReconstructionPlanResponse):
        return ControlOperation.RECONSTRUCTION_PLAN_RESULT
    if isinstance(payload, PutFileRequest):
        return ControlOperation.PUT_FILE
    if isinstance(payload, PutFileResponse):
        return ControlOperation.PUT_FILE_RESULT
    if isinstance(payload, ReadFileRangeRequest):
        return ControlOperation.READ_FILE_RANGE
    if isinstance(payload, ReadFileRangeResponse):
        return ControlOperation.READ_FILE_RANGE_RESULT
    if isinstance(payload, ReadContainerRangeRequest):
        return ControlOperation.READ_CONTAINER_RANGE
    if isinstance(payload, RenamePathRequest):
        return ControlOperation.RENAME_PATH
    if isinstance(payload, RenamePathResponse):
        return ControlOperation.RENAME_PATH_RESULT
    if isinstance(payload, DeletePathRequest):
        return ControlOperation.DELETE_PATH
    if isinstance(payload, DeletePathResponse):
        return ControlOperation.DELETE_PATH_RESULT
    if isinstance(payload, MountNegotiateRequest):
        return ControlOperation.MOUNT_NEGOTIATE
    if isinstance(payload, MountNegotiatedResponse):
        return ControlOperation.MOUNT_NEGOTIATED
    if isinstance(payload, TransferBootstrapRequest):
        return ControlOperation.TRANSFER_BOOTSTRAP
    if isinstance(payload, TransferAcceptedResponse):
        return ControlOperation.TRANSFER_ACCEPTED
    if isinstance(payload, TransferEntryUpdate):
        return ControlOperation.TRANSFER_ENTRY_UPDATE
    if isinstance(payload, TransferEntryAcceptedResponse):
        return ControlOperation.TRANSFER_ENTRY_ACCEPTED
    if isinstance(payload, TransferMetadataUpdate):
        return ControlOperation.TRANSFER_METADATA
    if isinstance(payload, TransferMetadataAcceptedResponse):
        return ControlOperation.TRANSFER_METADATA_ACCEPTED
    if isinstance(payload, TransferEntryCommitRequest):
        return ControlOperation.TRANSFER_ENTRY_COMMIT
    if isinstance(payload, TransferEntryCommittedResponse):
        return ControlOperation.TRANSFER_ENTRY_COMMITTED
    if isinstance(payload, TransferCommitRequest):
        return ControlOperation.TRANSFER_COMMIT
    if isinstance(payload, TransferCommittedResponse):
        return ControlOperation.TRANSFER_COMMITTED
    if isinstance(payload, ErrorResponse):
        return ControlOperation.ERROR
    raise TypeError(f"unsupported control payload: {type(payload).__name__}")


def encode_control_payload(
    payload: AuthStart
    | AuthChallenge
    | AuthProof
    | AuthAccepted
    | DirectoryListRequest
    | DirectoryListResponse
    | StatPathRequest
    | StatPathResponse
    | ManifestRequest
    | ManifestResponse
    | ReconstructionPlanRequest
    | ReconstructionPlanResponse
    | PutFileRequest
    | PutFileResponse
    | ReadFileRangeRequest
    | ReadFileRangeResponse
    | ReadContainerRangeRequest
    | RenamePathRequest
    | RenamePathResponse
    | DeletePathRequest
    | DeletePathResponse
    | MountNegotiateRequest
    | MountNegotiatedResponse
    | TransferBootstrapRequest
    | TransferAcceptedResponse
    | TransferEntryUpdate
    | TransferEntryAcceptedResponse
    | TransferMetadataUpdate
    | TransferMetadataAcceptedResponse
    | TransferEntryCommitRequest
    | TransferEntryCommittedResponse
    | TransferCommitRequest
    | TransferCommittedResponse
    | ErrorResponse,
) -> bytes:
    writer = _BinaryWriter()
    writer.write(PAYLOAD_MAGIC)
    writer.write_int(PAYLOAD_VERSION)
    writer.write_int(operation_for_payload(payload).value)

    if isinstance(payload, AuthStart):
        writer.write_int(_auth_mechanism_code(payload.mechanism))
        writer.write_utf8(payload.principal)
    elif isinstance(payload, AuthChallenge):
        writer.write_uuid(payload.challenge_id)
        writer.write_int(_auth_mechanism_code(payload.mechanism))
        writer.write_utf8(payload.principal)
        writer.write_bytes(payload.nonce)
        writer.write_utf8(payload.proof_algorithm)
        writer.write_int(payload.iterations)
    elif isinstance(payload, AuthProof):
        writer.write_uuid(payload.challenge_id)
        writer.write_utf8(payload.principal)
        writer.write_bytes(payload.proof)
    elif isinstance(payload, AuthAccepted):
        writer.write_uuid(payload.session_id)
        writer.write_int(len(payload.mechanisms))
        for mechanism in payload.mechanisms:
            writer.write_int(_auth_mechanism_code(mechanism))
    elif isinstance(payload, DirectoryListRequest):
        writer.write_utf8(payload.path)
    elif isinstance(payload, DirectoryListResponse):
        _write_directory_listing(writer, payload.listing)
    elif isinstance(payload, StatPathRequest):
        writer.write_utf8(payload.path)
    elif isinstance(payload, StatPathResponse):
        _write_optional_metadata(writer, payload.metadata)
    elif isinstance(payload, ManifestRequest):
        _write_hash(writer, payload.file_hash)
    elif isinstance(payload, ManifestResponse):
        _write_optional_manifest(writer, payload.manifest)
    elif isinstance(payload, ReconstructionPlanRequest):
        _write_hash(writer, payload.file_hash)
    elif isinstance(payload, ReconstructionPlanResponse):
        _write_reconstruction_plan(writer, payload.plan)
    elif isinstance(payload, PutFileRequest):
        writer.write_utf8(payload.path)
        writer.write_long(payload.size)
        _write_hash(writer, payload.file_hash)
        _write_chunk_refs(writer, payload.chunks)
    elif isinstance(payload, PutFileResponse):
        _write_manifest(writer, payload.manifest)
        _write_hashes(writer, payload.missing_chunks)
    elif isinstance(payload, ReadFileRangeRequest):
        writer.write_utf8(payload.path)
        writer.write_long(payload.offset)
        writer.write_long(payload.length)
    elif isinstance(payload, ReadFileRangeResponse):
        writer.write_utf8(payload.path)
        _write_read_plan(writer, payload.plan)
    elif isinstance(payload, ReadContainerRangeRequest):
        _write_hash(writer, payload.container_hash)
        writer.write_int(payload.first_chunk_index)
        writer.write_int(payload.chunk_count)
    elif isinstance(payload, RenamePathRequest):
        writer.write_utf8(payload.source_path)
        writer.write_utf8(payload.target_path)
    elif isinstance(payload, RenamePathResponse):
        writer.write_utf8(payload.source_path)
        writer.write_utf8(payload.target_path)
        _write_metadata(writer, payload.metadata)
    elif isinstance(payload, DeletePathRequest):
        writer.write_utf8(payload.path)
        writer.write_bool(payload.recursive)
    elif isinstance(payload, DeletePathResponse):
        writer.write_utf8(payload.path)
        writer.write_long(payload.deleted_entries)
    elif isinstance(payload, MountNegotiateRequest):
        writer.write_int(payload.backend.value)
        _write_mount_capabilities(writer, payload.requested_capabilities)
    elif isinstance(payload, MountNegotiatedResponse):
        _write_mount_strategy(writer, payload.strategy)
    elif isinstance(payload, TransferBootstrapRequest):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.direction.value)
        writer.write_utf8(payload.path)
        writer.write_int(payload.payload_format.value)
        writer.write_int(payload.initial_cords)
        writer.write_int(payload.max_cords)
        writer.write_int(payload.provisional_container_size_bytes)
        writer.write_long(payload.expected_size_limit_bytes)
    elif isinstance(payload, TransferAcceptedResponse):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.direction.value)
        writer.write_int(payload.accepted_initial_cords)
        writer.write_int(payload.accepted_max_cords)
        writer.write_long(payload.flow_control_window_bytes)
        writer.write_bool(payload.write_payload_admission_granted)
    elif isinstance(payload, TransferEntryUpdate):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_int(payload.action.value)
        writer.write_utf8(payload.path)
        writer.write_utf8(payload.target_path)
        writer.write_long(payload.size_hint_bytes)
        writer.write_int(payload.payload_format.value)
    elif isinstance(payload, TransferEntryAcceptedResponse):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_utf8(payload.path)
    elif isinstance(payload, TransferMetadataUpdate):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_int(payload.metadata_sequence)
        writer.write_bool(payload.final_metadata)
        writer.write_long(payload.bytes_covered)
        _write_hashes(writer, payload.chunk_hashes)
    elif isinstance(payload, TransferMetadataAcceptedResponse):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_int(payload.metadata_sequence)
        writer.write_int(payload.chunk_hashes_accepted)
        writer.write_int(payload.staged_payloads_verified)
        writer.write_bool(payload.final_metadata)
    elif isinstance(payload, TransferEntryCommitRequest):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_utf8(payload.path)
        _write_hash(writer, payload.content_hash)
        writer.write_long(payload.bytes_written)
        _write_checksum(writer, payload.checksum)
    elif isinstance(payload, TransferEntryCommittedResponse):
        writer.write_uuid(payload.transfer_id)
        writer.write_int(payload.entry_sequence)
        writer.write_utf8(payload.path)
        _write_hash(writer, payload.content_hash)
        writer.write_long(payload.bytes_written)
        _write_checksum(writer, payload.checksum)
    elif isinstance(payload, TransferCommitRequest):
        writer.write_uuid(payload.transfer_id)
        writer.write_utf8(payload.path)
        _write_hash(writer, payload.content_hash)
        writer.write_long(payload.bytes_written)
    elif isinstance(payload, TransferCommittedResponse):
        writer.write_uuid(payload.transfer_id)
        _write_hash(writer, payload.content_hash)
        writer.write_long(payload.bytes_written)
    elif isinstance(payload, ErrorResponse):
        writer.write_int(payload.status.value)
        writer.write_utf8(payload.message)
    else:
        raise TypeError(f"unsupported control payload: {type(payload).__name__}")

    return writer.bytes()


def decode_control_payload(
    operation: ControlOperation,
    payload_bytes: bytes,
) -> AuthStart | AuthChallenge | AuthProof | AuthAccepted | DirectoryListRequest | DirectoryListResponse | StatPathRequest | StatPathResponse | ManifestRequest | ManifestResponse | ReconstructionPlanRequest | ReconstructionPlanResponse | PutFileRequest | PutFileResponse | ReadFileRangeRequest | ReadFileRangeResponse | ReadContainerRangeRequest | RenamePathRequest | RenamePathResponse | DeletePathRequest | DeletePathResponse | MountNegotiateRequest | MountNegotiatedResponse | TransferBootstrapRequest | TransferAcceptedResponse | TransferEntryUpdate | TransferEntryAcceptedResponse | TransferMetadataUpdate | TransferMetadataAcceptedResponse | TransferEntryCommitRequest | TransferEntryCommittedResponse | TransferCommitRequest | TransferCommittedResponse | ErrorResponse:
    reader = _BinaryReader(payload_bytes)
    reader.require(PAYLOAD_MAGIC)
    reader.require_int(PAYLOAD_VERSION)
    encoded_operation = ControlOperation(reader.read_int())
    if encoded_operation != operation:
        raise ValueError("payload operation does not match frame operation")

    if operation == ControlOperation.AUTH_START:
        payload = AuthStart(
            _auth_mechanism(reader.read_int()),
            reader.read_utf8(MAX_TEXT_BYTES),
        )
    elif operation == ControlOperation.AUTH_CHALLENGE:
        payload = AuthChallenge(
            reader.read_uuid(),
            _auth_mechanism(reader.read_int()),
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_bytes(MAX_AUTH_BYTES),
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_int(),
        )
    elif operation == ControlOperation.AUTH_PROOF:
        payload = AuthProof(
            reader.read_uuid(),
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_bytes(MAX_AUTH_BYTES),
        )
    elif operation == ControlOperation.AUTH_ACCEPTED:
        session_id = reader.read_uuid()
        count = reader.read_int()
        if count < 0 or count > 16:
            raise ValueError(f"invalid auth mechanisms count: {count}")
        payload = AuthAccepted(session_id, tuple(_auth_mechanism(reader.read_int()) for _ in range(count)))
    elif operation == ControlOperation.LIST_DIRECTORY:
        payload = DirectoryListRequest(reader.read_utf8(MAX_TEXT_BYTES))
    elif operation == ControlOperation.LIST_DIRECTORY_RESULT:
        payload = DirectoryListResponse(_read_directory_listing(reader))
    elif operation == ControlOperation.STAT_PATH:
        payload = StatPathRequest(reader.read_utf8(MAX_TEXT_BYTES))
    elif operation == ControlOperation.STAT_PATH_RESULT:
        payload = StatPathResponse(_read_optional_metadata(reader))
    elif operation == ControlOperation.GET_MANIFEST:
        payload = ManifestRequest(_read_hash(reader))
    elif operation == ControlOperation.MANIFEST_RESULT:
        payload = ManifestResponse(_read_optional_manifest(reader))
    elif operation == ControlOperation.GET_RECONSTRUCTION_PLAN:
        payload = ReconstructionPlanRequest(_read_hash(reader))
    elif operation == ControlOperation.RECONSTRUCTION_PLAN_RESULT:
        payload = ReconstructionPlanResponse(_read_reconstruction_plan(reader))
    elif operation == ControlOperation.PUT_FILE:
        payload = PutFileRequest(
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_long(),
            _read_hash(reader),
            _read_chunk_refs(reader),
        )
    elif operation == ControlOperation.PUT_FILE_RESULT:
        payload = PutFileResponse(_read_manifest(reader), _read_hashes(reader))
    elif operation == ControlOperation.READ_FILE_RANGE:
        payload = ReadFileRangeRequest(
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_long(),
            reader.read_long(),
        )
    elif operation == ControlOperation.READ_FILE_RANGE_RESULT:
        payload = ReadFileRangeResponse(
            reader.read_utf8(MAX_TEXT_BYTES),
            _read_read_plan(reader),
        )
    elif operation == ControlOperation.READ_CONTAINER_RANGE:
        payload = ReadContainerRangeRequest(
            _read_hash(reader),
            reader.read_int(),
            reader.read_int(),
        )
    elif operation == ControlOperation.RENAME_PATH:
        payload = RenamePathRequest(
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_utf8(MAX_TEXT_BYTES),
        )
    elif operation == ControlOperation.RENAME_PATH_RESULT:
        payload = RenamePathResponse(
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_utf8(MAX_TEXT_BYTES),
            _read_metadata(reader),
        )
    elif operation == ControlOperation.DELETE_PATH:
        payload = DeletePathRequest(reader.read_utf8(MAX_TEXT_BYTES), reader.read_bool())
    elif operation == ControlOperation.DELETE_PATH_RESULT:
        payload = DeletePathResponse(reader.read_utf8(MAX_TEXT_BYTES), reader.read_long())
    elif operation == ControlOperation.MOUNT_NEGOTIATE:
        payload = MountNegotiateRequest(
            MountBackend(reader.read_int()),
            _read_mount_capabilities(reader),
        )
    elif operation == ControlOperation.MOUNT_NEGOTIATED:
        payload = MountNegotiatedResponse(_read_mount_strategy(reader))
    elif operation == ControlOperation.TRANSFER_BOOTSTRAP:
        payload = TransferBootstrapRequest(
            reader.read_uuid(),
            TransferDirection(reader.read_int()),
            reader.read_utf8(MAX_TEXT_BYTES),
            TransferPayloadFormat(reader.read_int()),
            reader.read_int(),
            reader.read_int(),
            reader.read_int(),
            reader.read_long(),
        )
    elif operation == ControlOperation.TRANSFER_ACCEPTED:
        payload = TransferAcceptedResponse(
            reader.read_uuid(),
            TransferDirection(reader.read_int()),
            reader.read_int(),
            reader.read_int(),
            reader.read_long(),
            reader.read_bool(),
        )
    elif operation == ControlOperation.TRANSFER_ENTRY_UPDATE:
        payload = TransferEntryUpdate(
            reader.read_uuid(),
            reader.read_int(),
            TransferEntryAction(reader.read_int()),
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_utf8(MAX_TEXT_BYTES),
            reader.read_long(),
            TransferPayloadFormat(reader.read_int()),
        )
    elif operation == ControlOperation.TRANSFER_ENTRY_ACCEPTED:
        payload = TransferEntryAcceptedResponse(
            reader.read_uuid(),
            reader.read_int(),
            reader.read_utf8(MAX_TEXT_BYTES),
        )
    elif operation == ControlOperation.TRANSFER_METADATA:
        payload = TransferMetadataUpdate(
            reader.read_uuid(),
            reader.read_int(),
            reader.read_int(),
            reader.read_bool(),
            reader.read_long(),
            _read_hashes(reader),
        )
    elif operation == ControlOperation.TRANSFER_METADATA_ACCEPTED:
        payload = TransferMetadataAcceptedResponse(
            reader.read_uuid(),
            reader.read_int(),
            reader.read_int(),
            reader.read_int(),
            reader.read_int(),
            reader.read_bool(),
        )
    elif operation == ControlOperation.TRANSFER_ENTRY_COMMIT:
        payload = TransferEntryCommitRequest(
            reader.read_uuid(),
            reader.read_int(),
            reader.read_utf8(MAX_TEXT_BYTES),
            _read_hash(reader),
            reader.read_long(),
            _read_checksum(reader),
        )
    elif operation == ControlOperation.TRANSFER_ENTRY_COMMITTED:
        payload = TransferEntryCommittedResponse(
            reader.read_uuid(),
            reader.read_int(),
            reader.read_utf8(MAX_TEXT_BYTES),
            _read_hash(reader),
            reader.read_long(),
            _read_checksum(reader),
        )
    elif operation == ControlOperation.TRANSFER_COMMIT:
        payload = TransferCommitRequest(
            reader.read_uuid(),
            reader.read_utf8(MAX_TEXT_BYTES),
            _read_hash(reader),
            reader.read_long(),
        )
    elif operation == ControlOperation.TRANSFER_COMMITTED:
        payload = TransferCommittedResponse(
            reader.read_uuid(),
            _read_hash(reader),
            reader.read_long(),
        )
    elif operation == ControlOperation.ERROR:
        payload = ErrorResponse(
            ResponseStatus(reader.read_int()),
            reader.read_utf8(MAX_TEXT_BYTES),
        )
    else:
        raise ValueError(f"unsupported control operation for Python client: {operation}")

    reader.require_eof()
    return payload


def encode_control_frame(frame: ControlFrame) -> bytes:
    writer = _BinaryWriter()
    writer.write(FRAME_MAGIC)
    writer.write_int(FRAME_VERSION)
    writer.write_int(frame.protocol_major)
    writer.write_int(frame.protocol_minor)
    writer.write_uuid(frame.message_id)
    writer.write_bool(frame.session_id is not None)
    if frame.session_id is not None:
        writer.write_uuid(frame.session_id)
    writer.write_int(frame.operation.value)
    writer.write_bytes(frame.payload)
    return writer.bytes()


def decode_control_frame(frame_bytes: bytes) -> ControlFrame:
    reader = _BinaryReader(frame_bytes)
    reader.require(FRAME_MAGIC)
    reader.require_int(FRAME_VERSION)
    protocol_major = reader.read_int()
    protocol_minor = reader.read_int()
    message_id = reader.read_uuid()
    session_id = reader.read_uuid() if reader.read_bool() else None
    operation = ControlOperation(reader.read_int())
    payload = reader.read_bytes(MAX_PAYLOAD_BYTES)
    reader.require_eof()
    return ControlFrame(operation, payload, message_id, session_id, protocol_major, protocol_minor)


def write_encoded_frame(sock: socket.socket, frame_bytes: bytes) -> None:
    if not frame_bytes or len(frame_bytes) > MAX_ENCODED_FRAME_BYTES:
        raise ValueError(f"invalid encoded control frame length: {len(frame_bytes)}")
    sock.sendall(struct.pack(">i", len(frame_bytes)) + frame_bytes)


def read_encoded_frame(sock: socket.socket) -> bytes:
    length = struct.unpack(">i", _read_socket_exact(sock, 4))[0]
    if length <= 0 or length > MAX_ENCODED_FRAME_BYTES:
        raise ValueError(f"invalid encoded control frame length: {length}")
    return _read_socket_exact(sock, length)


def _auth_mechanism_code(mechanism: AuthMechanism) -> int:
    if mechanism == AuthMechanism.USERNAME_PASSWORD:
        return 1
    if mechanism == AuthMechanism.BEARER_TOKEN:
        return 2
    raise ValueError(f"unknown auth mechanism: {mechanism}")


def _auth_mechanism(code: int) -> AuthMechanism:
    if code == 1:
        return AuthMechanism.USERNAME_PASSWORD
    if code == 2:
        return AuthMechanism.BEARER_TOKEN
    raise ValueError(f"unknown auth mechanism code: {code}")


def _normalize_mount_capabilities(capabilities: tuple[MountCapability, ...]) -> tuple[MountCapability, ...]:
    normalized = tuple(sorted({MountCapability(capability) for capability in capabilities}, key=lambda item: item.value))
    if not normalized:
        raise ValueError("at least one mount capability is required")
    return normalized


def _write_mount_capabilities(writer: _BinaryWriter, capabilities: tuple[MountCapability, ...]) -> None:
    normalized = _normalize_mount_capabilities(capabilities)
    writer.write_int(len(normalized))
    for capability in normalized:
        writer.write_int(capability.value)


def _read_mount_capabilities(reader: _BinaryReader) -> tuple[MountCapability, ...]:
    count = reader.read_int()
    if count < 0 or count > len(MountCapability):
        raise ValueError(f"invalid mount capabilities count: {count}")
    return _normalize_mount_capabilities(tuple(MountCapability(reader.read_int()) for _ in range(count)))


def _write_mount_strategy(writer: _BinaryWriter, strategy: MountStrategy) -> None:
    writer.write_int(strategy.backend.value)
    _write_mount_capabilities(writer, strategy.capabilities)


def _read_mount_strategy(reader: _BinaryReader) -> MountStrategy:
    return MountStrategy(MountBackend(reader.read_int()), _read_mount_capabilities(reader))


def _write_hash(writer: _BinaryWriter, value: bytes | bytearray | memoryview) -> None:
    hash_bytes = bytes(value)
    if len(hash_bytes) != HASH_BYTES:
        raise ValueError("hash must be 32 bytes")
    writer.write(hash_bytes)


def _read_hash(reader: _BinaryReader) -> bytes:
    return reader.read_exact(HASH_BYTES)


def _write_chunk_refs(writer: _BinaryWriter, chunks: tuple[ChunkRef, ...]) -> None:
    writer.write_int(len(chunks))
    for chunk in chunks:
        _write_hash(writer, chunk.chunk_hash)
        writer.write_int(chunk.size)


def _read_chunk_refs(reader: _BinaryReader) -> tuple[ChunkRef, ...]:
    count = reader.read_int()
    if count < 0 or count > MAX_CHUNK_REFS:
        raise ValueError(f"invalid chunk refs count: {count}")
    return tuple(ChunkRef(_read_hash(reader), reader.read_int()) for _ in range(count))


def _write_hashes(writer: _BinaryWriter, hashes: tuple[bytes, ...]) -> None:
    writer.write_int(len(hashes))
    for hash_value in hashes:
        _write_hash(writer, hash_value)


def _read_hashes(reader: _BinaryReader) -> tuple[bytes, ...]:
    count = reader.read_int()
    if count < 0 or count > MAX_CHUNK_REFS:
        raise ValueError(f"invalid hashes count: {count}")
    return tuple(_read_hash(reader) for _ in range(count))


def _write_checksum(writer: _BinaryWriter, checksum: TransferChecksum | None) -> None:
    cs = checksum if checksum is not None else TransferChecksum.none()
    writer.write_bool(cs.present)
    if cs.present:
        writer.write_utf8(cs.algorithm)
        writer.write_long(cs.value)


def _read_checksum(reader: _BinaryReader) -> TransferChecksum:
    if reader.remaining <= 0:
        return TransferChecksum.none()
    if not reader.read_bool():
        return TransferChecksum.none()
    return TransferChecksum(reader.read_utf8(MAX_TEXT_BYTES), reader.read_long())


def _write_manifest(writer: _BinaryWriter, manifest: FileManifest) -> None:
    nested = _BinaryWriter()
    nested.write(b"CXNFSMAN")
    nested.write_int(1)
    nested.write_utf8(manifest.path)
    _write_hash(nested, manifest.file_hash)
    nested.write_long(manifest.size)
    _write_chunk_refs(nested, manifest.chunks)
    writer.write_bytes(nested.bytes())


def _read_manifest(reader: _BinaryReader) -> FileManifest:
    nested = _BinaryReader(reader.read_bytes(MAX_PAYLOAD_BYTES))
    nested.require(b"CXNFSMAN")
    nested.require_int(1)
    manifest = FileManifest(
        nested.read_utf8(16 * 1024),
        _read_hash(nested),
        nested.read_long(),
        _read_chunk_refs(nested),
    )
    nested.require_eof()
    return manifest


def _write_optional_manifest(writer: _BinaryWriter, manifest: FileManifest | None) -> None:
    writer.write_bool(manifest is not None)
    if manifest is not None:
        _write_manifest(writer, manifest)


def _read_optional_manifest(reader: _BinaryReader) -> FileManifest | None:
    return _read_manifest(reader) if reader.read_bool() else None


def _write_metadata(writer: _BinaryWriter, metadata: FileMetadata) -> None:
    writer.write_utf8(metadata.path)
    writer.write_utf8(metadata.name)
    writer.write_int(metadata.type.value)
    writer.write_long(metadata.size)
    writer.write_bool(metadata.content_hash is not None)
    if metadata.content_hash is not None:
        _write_hash(writer, metadata.content_hash)
    writer.write_long(metadata.modified_at_seconds)
    writer.write_int(metadata.modified_at_nanos)


def _read_metadata(reader: _BinaryReader) -> FileMetadata:
    path = reader.read_utf8(MAX_TEXT_BYTES)
    name = reader.read_utf8(MAX_TEXT_BYTES)
    file_type = FileType(reader.read_int())
    size = reader.read_long()
    content_hash = _read_hash(reader) if reader.read_bool() else None
    modified_at_seconds = reader.read_long()
    modified_at_nanos = reader.read_int()
    return FileMetadata(path, name, file_type, size, content_hash, modified_at_seconds, modified_at_nanos)


def _write_optional_metadata(writer: _BinaryWriter, metadata: FileMetadata | None) -> None:
    writer.write_bool(metadata is not None)
    if metadata is not None:
        _write_metadata(writer, metadata)


def _read_optional_metadata(reader: _BinaryReader) -> FileMetadata | None:
    return _read_metadata(reader) if reader.read_bool() else None


def _write_directory_listing(writer: _BinaryWriter, listing: DirectoryListing) -> None:
    writer.write_utf8(listing.path)
    writer.write_int(len(listing.entries))
    for entry in listing.entries:
        _write_metadata(writer, entry)


def _read_directory_listing(reader: _BinaryReader) -> DirectoryListing:
    path = reader.read_utf8(MAX_TEXT_BYTES)
    entry_count = reader.read_int()
    if entry_count < 0 or entry_count > MAX_LIST_ENTRIES:
        raise ValueError(f"invalid directory listing entries count: {entry_count}")
    return DirectoryListing(path, tuple(_read_metadata(reader) for _ in range(entry_count)))


def _write_reconstruction_plan(writer: _BinaryWriter, plan: ReconstructionPlan) -> None:
    _write_hash(writer, plan.file_hash)
    writer.write_long(plan.file_size)
    writer.write_int(len(plan.terms))
    for term in plan.terms:
        _write_hash(writer, term.container_hash)
        writer.write_int(term.first_chunk_index)
        writer.write_int(term.chunk_count)


def _read_reconstruction_plan(reader: _BinaryReader) -> ReconstructionPlan:
    file_hash = _read_hash(reader)
    file_size = reader.read_long()
    term_count = reader.read_int()
    if term_count < 0 or term_count > MAX_TERMS:
        raise ValueError(f"invalid reconstruction terms count: {term_count}")
    return ReconstructionPlan(
        file_hash,
        file_size,
        tuple(ReconstructionTerm(_read_hash(reader), reader.read_int(), reader.read_int()) for _ in range(term_count)),
    )


def _write_read_plan(writer: _BinaryWriter, plan: ReadPlan) -> None:
    _write_hash(writer, plan.file_hash)
    writer.write_long(plan.file_size)
    writer.write_long(plan.requested_range.offset)
    writer.write_long(plan.requested_range.length)
    writer.write_int(len(plan.chunks))
    for chunk in plan.chunks:
        writer.write_int(chunk.chunk_index)
        _write_hash(writer, chunk.chunk_hash)
        writer.write_long(chunk.file_offset)
        writer.write_int(chunk.chunk_size)
        writer.write_int(chunk.offset_in_chunk)
        writer.write_int(chunk.bytes_to_read)


def _read_read_plan(reader: _BinaryReader) -> ReadPlan:
    file_hash = _read_hash(reader)
    file_size = reader.read_long()
    requested_range = ByteRange(reader.read_long(), reader.read_long())
    chunk_count = reader.read_int()
    if chunk_count < 0 or chunk_count > MAX_CHUNK_REFS:
        raise ValueError(f"invalid read plan chunks count: {chunk_count}")
    chunks = tuple(
        PlannedChunkRead(
            reader.read_int(),
            _read_hash(reader),
            reader.read_long(),
            reader.read_int(),
            reader.read_int(),
            reader.read_int(),
        )
        for _ in range(chunk_count)
    )
    return ReadPlan(file_hash, file_size, requested_range, chunks)


def _read_socket_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise EOFError("control socket closed while reading frame")
        chunks.extend(chunk)
    return bytes(chunks)


class _BinaryWriter:
    def __init__(self) -> None:
        self._data = bytearray()

    def write(self, data: bytes) -> None:
        self._data.extend(data)

    def write_bool(self, value: bool) -> None:
        self._data.extend(b"\x01" if value else b"\x00")

    def write_int(self, value: int) -> None:
        self._data.extend(struct.pack(">i", value))

    def write_long(self, value: int) -> None:
        self._data.extend(struct.pack(">q", value))

    def write_uuid(self, value: UUID) -> None:
        self._data.extend(struct.pack(">QQ", value.int >> 64, value.int & ((1 << 64) - 1)))

    def write_bytes(self, value: bytes | bytearray | memoryview) -> None:
        bytes_value = bytes(value)
        self.write_int(len(bytes_value))
        self._data.extend(bytes_value)

    def write_utf8(self, value: str) -> None:
        self.write_bytes(value.encode("utf-8"))

    def bytes(self) -> bytes:
        return bytes(self._data)


class _BinaryReader:
    def __init__(self, data: bytes) -> None:
        self._data = memoryview(data)
        self._offset = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._offset

    def require(self, expected: bytes) -> None:
        actual = self.read_exact(len(expected))
        if actual != expected:
            raise ValueError("invalid binary format magic")

    def require_int(self, expected: int) -> None:
        actual = self.read_int()
        if actual != expected:
            raise ValueError(f"unsupported binary format version: {actual}")

    def read_bool(self) -> bool:
        return self.read_exact(1) != b"\x00"

    def read_int(self) -> int:
        return struct.unpack(">i", self.read_exact(4))[0]

    def read_long(self) -> int:
        return struct.unpack(">q", self.read_exact(8))[0]

    def read_uuid(self) -> UUID:
        high, low = struct.unpack(">QQ", self.read_exact(16))
        return UUID(int=(high << 64) | low)

    def read_bytes(self, max_length: int) -> bytes:
        length = self.read_int()
        if length < 0 or length > max_length:
            raise ValueError(f"invalid byte array length: {length}")
        return self.read_exact(length)

    def read_utf8(self, max_length: int) -> str:
        return self.read_bytes(max_length).decode("utf-8")

    def read_exact(self, length: int) -> bytes:
        if length < 0:
            raise ValueError(f"invalid fixed byte length: {length}")
        end = self._offset + length
        if end > len(self._data):
            raise ValueError("unexpected end of binary input")
        value = bytes(self._data[self._offset:end])
        self._offset = end
        return value

    def require_eof(self) -> None:
        if self._offset != len(self._data):
            raise ValueError("trailing bytes after control structure")
