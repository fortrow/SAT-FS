from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import hashlib
import hmac
import importlib.util
from pathlib import Path
from queue import Empty, Queue
import socket
import ssl
import struct
from threading import Event, RLock, Thread
from time import sleep
from typing import Any, Callable, Iterable, Mapping, Protocol
from uuid import UUID

from .control import (
    ByteRange,
    HASH_BYTES,
    MAX_PAYLOAD_BYTES,
    PlannedChunkRead,
    ReadPlan,
    TransferAcceptedResponse,
    TransferDirection,
    TransferEntryAction,
    TransferPayloadFormat,
)
from .dataset import MdsShardReadOperation, SafetensorsSendOperation, SendResult
from .dataset import (
    BufferReadOperation,
    DirectWriteOperation,
    ReadIntoTensorOperation,
    TensorDestination,
    TensorMemoryTarget,
    TensorReadOperation,
)
from .namespace import (
    join_namespace_path,
    normalize_namespace_file_path,
    normalize_namespace_root,
    normalize_namespace_path,
)
from .tensor_memory import (
    copy_staging_to_tensor,
    readable_source_memoryview,
    require_tensor_read_nbytes_match as _require_tensor_read_nbytes_match,
    require_torch as _require_torch,
    tensor_shape as _tensor_shape,
    tensor_writable_memoryview,
    torch_dtype as _torch_dtype,
    torch_empty as _torch_empty,
)
from .tls import CXNFS_TLS_CIPHER_SUITE, create_client_tls_context


BULK_MAGIC = b"CXNFSDAT"
READ_REQUEST_MAGIC = b"CXNFSRDQ"
WRITE_ACK_MAGIC = b"CXNFSWAK"
BULK_VERSION = 1
READ_REQUEST_VERSION = 1
WRITE_ACK_VERSION = 1
ABSOLUTE_MAX_CORDS = 128
MAX_ENCODED_BULK_FRAME_BYTES = MAX_PAYLOAD_BYTES + 1024
MAX_BULK_READ_REQUEST_BYTES = 64 * 1024 + 128
MAX_ENCODED_BULK_WRITE_ACK_BYTES = 256
PYTHON_BULK_TRANSPORT_STREAM = "stream"
PYTHON_BULK_TRANSPORT_QUIC = "quic"
QUIC_BULK_APPLICATION_PROTOCOL = "cxnfs-bulk/1"
ZERO_FILL_BLOCK = b"\x00" * (1024 * 1024)


def normalize_python_bulk_transport(value: str | None) -> str:
    transport = (value or PYTHON_BULK_TRANSPORT_STREAM).strip().lower().replace("_", "-")
    if transport not in {PYTHON_BULK_TRANSPORT_STREAM, PYTHON_BULK_TRANSPORT_QUIC}:
        raise ValueError("--bulk-transport must be stream or quic")
    return transport


def require_supported_python_bulk_transport(value: str | None) -> str:
    transport = normalize_python_bulk_transport(value)
    if transport == PYTHON_BULK_TRANSPORT_QUIC and importlib.util.find_spec("aioquic") is None:
        raise RuntimeError(
            "Python QUIC bulk transport requires the optional aioquic dependency; "
            "install cxnfs[quic] or use --bulk-transport stream"
        )
    return transport


@dataclass(frozen=True)
class BulkDataFrame:
    transfer_id: UUID
    cord_index: int
    entry_sequence: int
    chunk_sequence: int
    entry_offset: int
    final_frame_for_chunk: bool
    payload: bytes

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.cord_index < 0 or self.cord_index >= ABSOLUTE_MAX_CORDS:
            raise ValueError(f"cord_index must be between 0 and {ABSOLUTE_MAX_CORDS - 1}")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if self.chunk_sequence < 0:
            raise ValueError("chunk_sequence must be non-negative")
        if self.entry_offset < 0:
            raise ValueError("entry_offset must be non-negative")
        if not self.payload:
            raise ValueError("payload must not be empty")
        if len(self.payload) > MAX_PAYLOAD_BYTES:
            raise ValueError("bulk data frame payload exceeds maximum size")
        object.__setattr__(self, "payload", bytes(self.payload))


@dataclass(frozen=True)
class BulkWriteAck:
    transfer_id: UUID
    entry_sequence: int
    chunk_sequence: int
    payload_length: int
    metadata_verified: bool
    storage: str

    def __post_init__(self) -> None:
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if self.entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        if self.chunk_sequence < 0:
            raise ValueError("chunk_sequence must be non-negative")
        if self.payload_length <= 0:
            raise ValueError("payload_length must be positive")
        if self.storage not in {"MEMORY", "SPILL"}:
            raise ValueError(f"unsupported bulk write ack storage: {self.storage}")


@dataclass(frozen=True)
class BulkWriteFrameStats:
    frame_count: int = 0
    payload_bytes: int = 0
    ack_count: int = 0
    ack_frame_verified_count: int = 0
    ack_metadata_verified_count: int = 0
    ack_memory_count: int = 0
    ack_spill_count: int = 0
    max_cord_index: int = -1

    def merge(self, other: BulkWriteFrameStats) -> BulkWriteFrameStats:
        return BulkWriteFrameStats(
            frame_count=self.frame_count + other.frame_count,
            payload_bytes=self.payload_bytes + other.payload_bytes,
            ack_count=self.ack_count + other.ack_count,
            ack_frame_verified_count=self.ack_frame_verified_count + other.ack_frame_verified_count,
            ack_metadata_verified_count=self.ack_metadata_verified_count + other.ack_metadata_verified_count,
            ack_memory_count=self.ack_memory_count + other.ack_memory_count,
            ack_spill_count=self.ack_spill_count + other.ack_spill_count,
            max_cord_index=max(self.max_cord_index, other.max_cord_index),
        )


@dataclass(frozen=True)
class BulkReadRequest:
    session_id: UUID
    transfer_id: UUID
    path: str
    offset: int
    length: int
    active_cords: int

    def __post_init__(self) -> None:
        if self.session_id is None:
            raise ValueError("session_id is required")
        if self.transfer_id is None:
            raise ValueError("transfer_id is required")
        if not self.path:
            raise ValueError("path is required")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.length < 0:
            raise ValueError("length must be non-negative")
        if self.active_cords <= 0 or self.active_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"active_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        object.__setattr__(self, "path", normalize_namespace_path(self.path, label="path"))


class BulkFrameTransport(Protocol):
    def read_frames(
        self,
        session_id: UUID,
        transfer: TransferAcceptedResponse,
        path: str,
        plan: ReadPlan,
    ) -> Iterable[BulkDataFrame]:
        """Return full touched chunk frames for a previously authorized read plan."""

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck | None:
        """Send one admitted write payload frame on the protected bulk lane."""


@dataclass(frozen=True)
class CxnfsBulkReadConfig:
    initial_cords: int = 1
    max_cords: int = ABSOLUTE_MAX_CORDS
    provisional_container_size_bytes: int = 64 * 1024 * 1024
    expected_size_limit_bytes: int = 0
    write_frame_payload_bytes: int = MAX_PAYLOAD_BYTES

    def __post_init__(self) -> None:
        if self.initial_cords <= 0 or self.initial_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"initial_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if self.max_cords <= 0 or self.max_cords > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"max_cords must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if self.initial_cords > self.max_cords:
            raise ValueError("initial_cords must be <= max_cords")
        if self.provisional_container_size_bytes <= 0:
            raise ValueError("provisional_container_size_bytes must be positive")
        if self.expected_size_limit_bytes < 0:
            raise ValueError("expected_size_limit_bytes must be non-negative")
        if self.write_frame_payload_bytes <= 0 or self.write_frame_payload_bytes > MAX_PAYLOAD_BYTES:
            raise ValueError("write_frame_payload_bytes must be between 1 and MAX_PAYLOAD_BYTES")


@dataclass(frozen=True)
class CxnfsBulkRetryConfig:
    read_retries: int = 0
    write_retries: int = 0

    def __post_init__(self) -> None:
        if self.read_retries < 0:
            raise ValueError("read_retries must be non-negative")
        if self.read_retries > 8:
            raise ValueError("read_retries must be <= 8")
        if self.write_retries < 0:
            raise ValueError("write_retries must be non-negative")
        if self.write_retries > 8:
            raise ValueError("write_retries must be <= 8")

    @property
    def read_attempts(self) -> int:
        return self.read_retries + 1

    @property
    def write_attempts(self) -> int:
        return self.write_retries + 1


class LengthPrefixedBulkFrameTransport:
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
    ) -> LengthPrefixedBulkFrameTransport:
        raw_socket = socket.create_connection((host, port), timeout=timeout)
        context = tls_context if tls_context is not None else create_client_tls_context()
        try:
            tls_socket = context.wrap_socket(raw_socket, server_hostname=server_hostname or host)
        except Exception:
            raw_socket.close()
            raise
        return cls(tls_socket)

    def read_frames(
        self,
        session_id: UUID,
        transfer: TransferAcceptedResponse,
        path: str,
        plan: ReadPlan,
    ) -> Iterable[BulkDataFrame]:
        with self._lock:
            request = BulkReadRequest(
                session_id,
                transfer.transfer_id,
                path,
                plan.requested_range.offset,
                plan.requested_range.length,
                transfer.accepted_initial_cords,
            )
            write_encoded_bulk_read_request(self._socket, encode_bulk_read_request(request))
            for _ in plan.chunks:
                yield decode_bulk_frame(read_encoded_bulk_frame(self._socket))

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck:
        with self._lock:
            write_encoded_bulk_frame(self._socket, encode_bulk_frame(frame))
            ack = decode_bulk_write_ack(read_encoded_bulk_write_ack(self._socket))
            _require_write_ack_matches_frame(frame, ack)
            return ack

    def close(self) -> None:
        with self._lock:
            self._socket.close()

    def __enter__(self) -> LengthPrefixedBulkFrameTransport:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


class PooledLengthPrefixedBulkFrameTransport:
    def __init__(
        self,
        read_transport: BulkFrameTransport,
        write_transports: Iterable[BulkFrameTransport],
    ) -> None:
        if read_transport is None:
            raise ValueError("read_transport is required")
        writers = tuple(write_transports)
        if not writers:
            raise ValueError("at least one write transport is required")
        self._read_transport = read_transport
        self._write_transports = writers
        self._available_writers: Queue[BulkFrameTransport] = Queue(maxsize=len(writers))
        for writer in writers:
            self._available_writers.put(writer)
        self._closed = Event()
        self._read_lock = RLock()
        self._last_read_transport_count = 0

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        *,
        write_connections: int,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> PooledLengthPrefixedBulkFrameTransport:
        if write_connections <= 0 or write_connections > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"write_connections must be between 1 and {ABSOLUTE_MAX_CORDS}")
        opened: list[BulkFrameTransport] = []
        try:
            read_transport = LengthPrefixedBulkFrameTransport.connect(
                host,
                port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
            )
            opened.append(read_transport)
            writers = tuple(
                LengthPrefixedBulkFrameTransport.connect(
                    host,
                    port,
                    timeout=timeout,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                )
                for _ in range(write_connections)
            )
            opened.extend(writers)
            return cls(read_transport, writers)
        except Exception:
            _close_transports(opened)
            raise

    def read_frames(
        self,
        session_id: UUID,
        transfer: TransferAcceptedResponse,
        path: str,
        plan: ReadPlan,
    ) -> Iterable[BulkDataFrame]:
        with self._read_lock:
            available_transports = 1 + len(self._write_transports)
            active_transports = max(1, min(transfer.accepted_initial_cords, len(plan.chunks), available_transports))
            if active_transports == 1:
                self._last_read_transport_count = 1
                yield from self._read_transport.read_frames(session_id, transfer, path, plan)
                return

            leased = [self._lease_writer() for _ in range(active_transports - 1)]
            transports = (self._read_transport, *leased)
            self._last_read_transport_count = len(transports)
            try:
                partitions = _partition_read_plan(plan, len(transports))
                with ThreadPoolExecutor(max_workers=len(partitions)) as executor:
                    submitted = tuple(
                        executor.submit(
                            _read_partition,
                            transport,
                            session_id,
                            transfer,
                            path,
                            partition,
                        )
                        for transport, partition in zip(transports, partitions)
                    )
                    for future in as_completed(submitted):
                        yield from future.result()
            finally:
                for writer in leased:
                    if self._closed.is_set():
                        _close_transports((writer,))
                    else:
                        self._available_writers.put(writer)

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck | None:
        writer = self._lease_writer()
        try:
            return writer.write_frame(frame)
        finally:
            if self._closed.is_set():
                _close_transports((writer,))
            else:
                self._available_writers.put(writer)

    def close(self) -> None:
        self._closed.set()
        _close_transports((self._read_transport, *self._write_transports))

    def _lease_writer(self) -> BulkFrameTransport:
        while not self._closed.is_set():
            try:
                return self._available_writers.get(timeout=0.25)
            except Empty:
                continue
        raise OSError("bulk write transport pool is closed")

    @property
    def last_read_transport_count(self) -> int:
        return self._last_read_transport_count

    def __enter__(self) -> PooledLengthPrefixedBulkFrameTransport:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


class QuicBulkFrameTransport:
    def __init__(
        self,
        protocol: Any,
        run_async: Callable[[Any], Any],
        close_async: Callable[[], Any] | None = None,
        close_runner: Callable[[], None] | None = None,
        connection_attempt_count: int = 1,
    ) -> None:
        if protocol is None:
            raise ValueError("protocol is required")
        if run_async is None:
            raise ValueError("run_async is required")
        self._protocol = protocol
        self._run_async = run_async
        self._close_async = close_async
        self._close_runner = close_runner
        self._connection_attempt_count = max(1, int(connection_attempt_count))
        self._closed = Event()
        self._evidence_lock = RLock()
        self._last_read_stream_count = 0
        self._last_attempted_read_stream_count = 0
        self._attempted_read_streams = 0
        self._read_streams_created = 0
        self._read_streams_eof_verified = 0
        self._successful_read_operations = 0
        self._failed_read_operations = 0
        self._last_read_error = ""
        self._last_read_partition_evidence = _empty_quic_read_partition_evidence()
        self._attempted_write_streams = 0
        self._write_streams_created = 0
        self._active_write_streams = 0
        self._max_active_write_streams = 0
        self._successful_write_operations = 0
        self._failed_write_operations = 0
        self._write_acks_received = 0
        self._write_acks_verified = 0
        self._write_ack_verification_failures = 0
        self._write_ack_streams_eof_verified = 0
        self._write_ack_metadata_verified = 0
        self._write_ack_memory = 0
        self._write_ack_spill = 0
        self._last_write_error = ""

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        *,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        cafile: str | None = None,
        connect_attempts: int = 3,
        connect_retry_delay_seconds: float = 0.15,
    ) -> QuicBulkFrameTransport:
        attempts = max(1, int(connect_attempts))
        last_error: BaseException | None = None
        for attempt in range(attempts):
            loop_thread = _AsyncioLoopThread(timeout)
            try:
                context_manager, protocol = loop_thread.run(_connect_aioquic(
                    host,
                    port,
                    timeout=timeout,
                    tls_context=tls_context,
                    server_hostname=server_hostname,
                    cafile=cafile,
                ))
                break
            except Exception as exc:
                last_error = exc
                loop_thread.close()
                if attempt + 1 >= attempts or not _is_retryable_bulk_connect_error(exc):
                    raise
                sleep(max(0.0, float(connect_retry_delay_seconds)))
        else:
            if last_error is not None:
                raise last_error
            raise RuntimeError("QUIC bulk connect loop exited without a transport")

        async def close_async():
            await _close_aioquic(context_manager)

        return cls(
            protocol,
            loop_thread.run,
            close_async,
            loop_thread.close,
            connection_attempt_count=attempt + 1,
        )

    def read_frames(
        self,
        session_id: UUID,
        transfer: TransferAcceptedResponse,
        path: str,
        plan: ReadPlan,
    ) -> Iterable[BulkDataFrame]:
        self._require_open()
        frames: list[BulkDataFrame] = []
        self.read_frames_into(session_id, transfer, path, plan, frames.append)
        return tuple(frames)

    def read_frames_into(
        self,
        session_id: UUID,
        transfer: TransferAcceptedResponse,
        path: str,
        plan: ReadPlan,
        accept: Callable[[BulkDataFrame], Any],
    ) -> int:
        self._require_open()
        if accept is None:
            raise ValueError("accept callback is required")
        stream_count = _quic_stream_count(transfer, plan)
        partitions = _partition_read_plan(plan, stream_count)
        with self._evidence_lock:
            self._last_attempted_read_stream_count = stream_count
            self._attempted_read_streams += stream_count
            self._last_read_partition_evidence = _quic_read_partition_evidence(partitions, plan)
        try:
            count = self._run_async(_quic_read_frames_into(
                self._protocol,
                session_id,
                transfer,
                path,
                plan,
                accept,
            ))
        except Exception as exc:
            self._record_read_failure(exc, stream_count)
            self.close()
            raise
        with self._evidence_lock:
            self._last_read_stream_count = stream_count
            self._read_streams_created += stream_count
            self._read_streams_eof_verified += stream_count
            self._successful_read_operations += 1
            self._last_read_error = ""
        return count

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck:
        self._require_open()
        self._begin_write_stream()
        try:
            ack = self._run_async(_quic_write_frame(self._protocol, frame))
            self._record_write_ack_stream_eof_verified()
            self._record_write_ack_received(ack)
            _require_write_ack_matches_frame(frame, ack)
            self._record_write_ack_verified(ack)
        except Exception as exc:
            self._record_write_ack_verification_failure(exc)
            self._record_write_failure(exc)
            self.close()
            raise
        finally:
            self._finish_write_stream()
        with self._evidence_lock:
            self._write_streams_created += 1
            self._successful_write_operations += 1
            self._last_write_error = ""
        return ack

    @property
    def last_read_stream_count(self) -> int:
        return self._last_read_stream_count

    def read_path_evidence(self) -> dict[str, Any]:
        with self._evidence_lock:
            total_attempted_streams = self._attempted_read_streams + self._attempted_write_streams
            total_successful_streams = self._read_streams_created + self._write_streams_created
            read_streams_all_succeeded = (
                self._attempted_read_streams > 0
                and self._failed_read_operations == 0
                and self._read_streams_created == self._attempted_read_streams
            )
            read_stream_completion_verified = read_streams_all_succeeded
            read_stream_eof_verified = (
                self._attempted_read_streams > 0
                and self._failed_read_operations == 0
                and self._read_streams_eof_verified == self._attempted_read_streams
            )
            return {
                "format": "cxnfs.quic-bulk-transport-evidence.v1",
                "closed": self._closed.is_set(),
                "tls_version": "TLSv1.3",
                "cipher_suite": CXNFS_TLS_CIPHER_SUITE,
                "connection_opened_count": 1,
                "connection_attempt_count": self._connection_attempt_count,
                "connection_retry_count": self._connection_attempt_count - 1,
                "successful_read_operations": self._successful_read_operations,
                "failed_read_operations": self._failed_read_operations,
                "last_read_stream_count": self._last_read_stream_count,
                "last_attempted_read_stream_count": self._last_attempted_read_stream_count,
                "attempted_read_streams": self._attempted_read_streams,
                "read_streams_created": self._read_streams_created,
                "read_streams_eof_verified": self._read_streams_eof_verified,
                "read_streams_all_succeeded": read_streams_all_succeeded,
                "read_stream_completion_verified": read_stream_completion_verified,
                "read_stream_eof_verified": read_stream_eof_verified,
                **self._last_read_partition_evidence,
                "attempted_write_streams": self._attempted_write_streams,
                "write_streams_created": self._write_streams_created,
                "total_attempted_streams": total_attempted_streams,
                "total_successful_streams": total_successful_streams,
                "connection_reused_for_streams": total_successful_streams > 1,
                "last_read_error": self._last_read_error,
            }

    def write_path_evidence(self) -> dict[str, Any]:
        with self._evidence_lock:
            total_attempted_streams = self._attempted_read_streams + self._attempted_write_streams
            total_successful_streams = self._read_streams_created + self._write_streams_created
            write_streams_all_succeeded = (
                self._attempted_write_streams > 0
                and self._failed_write_operations == 0
                and self._write_streams_created == self._attempted_write_streams
            )
            write_ack_stream_completion_verified = write_streams_all_succeeded
            write_ack_stream_eof_verified = (
                self._attempted_write_streams > 0
                and self._failed_write_operations == 0
                and self._write_ack_streams_eof_verified == self._attempted_write_streams
            )
            write_ack_metadata_pending = self._write_acks_received - self._write_ack_metadata_verified
            write_ack_frame_identity_verified = (
                self._attempted_write_streams > 0
                and self._write_ack_verification_failures == 0
                and self._write_acks_received == self._attempted_write_streams
                and self._write_acks_verified == self._attempted_write_streams
            )
            concurrent_write_streams_observed = self._max_active_write_streams > 1
            return {
                "format": "cxnfs.quic-bulk-write-transport-evidence.v1",
                "closed": self._closed.is_set(),
                "tls_version": "TLSv1.3",
                "cipher_suite": CXNFS_TLS_CIPHER_SUITE,
                "connection_opened_count": 1,
                "connection_attempt_count": self._connection_attempt_count,
                "connection_retry_count": self._connection_attempt_count - 1,
                "successful_write_operations": self._successful_write_operations,
                "failed_write_operations": self._failed_write_operations,
                "attempted_write_streams": self._attempted_write_streams,
                "write_streams_created": self._write_streams_created,
                "write_ack_streams_eof_verified": self._write_ack_streams_eof_verified,
                "write_streams_all_succeeded": write_streams_all_succeeded,
                "write_ack_stream_completion_verified": write_ack_stream_completion_verified,
                "write_ack_stream_eof_verified": write_ack_stream_eof_verified,
                "write_acks_received": self._write_acks_received,
                "write_acks_verified": self._write_acks_verified,
                "write_ack_verification_failures": self._write_ack_verification_failures,
                "write_ack_frame_identity_verified": write_ack_frame_identity_verified,
                "write_ack_metadata_verified": self._write_ack_metadata_verified,
                "write_ack_metadata_pending": write_ack_metadata_pending,
                "write_ack_metadata_pending_observed": write_ack_metadata_pending > 0,
                "write_ack_memory": self._write_ack_memory,
                "write_ack_spill": self._write_ack_spill,
                "active_write_streams": self._active_write_streams,
                "max_active_write_streams": self._max_active_write_streams,
                "concurrent_write_streams_observed": concurrent_write_streams_observed,
                "attempted_read_streams": self._attempted_read_streams,
                "read_streams_created": self._read_streams_created,
                "total_attempted_streams": total_attempted_streams,
                "total_successful_streams": total_successful_streams,
                "connection_reused_for_streams": total_successful_streams > 1,
                "last_write_error": self._last_write_error,
            }

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        if self._close_async is None:
            return
        try:
            self._run_async(self._close_async())
        finally:
            if self._close_runner is not None:
                self._close_runner()

    def __enter__(self) -> QuicBulkFrameTransport:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def _require_open(self) -> None:
        if self._closed.is_set():
            raise OSError("QUIC bulk transport is closed")

    def _record_read_failure(self, exc: BaseException, stream_count: int) -> None:
        with self._evidence_lock:
            self._failed_read_operations += 1
            self._last_attempted_read_stream_count = stream_count
            self._last_read_error = f"{type(exc).__name__}: {exc}"

    def _record_write_failure(self, exc: BaseException) -> None:
        with self._evidence_lock:
            self._failed_write_operations += 1
            self._last_write_error = f"{type(exc).__name__}: {exc}"

    def _record_write_ack_received(self, ack: BulkWriteAck) -> None:
        with self._evidence_lock:
            self._write_acks_received += 1
            if ack.metadata_verified:
                self._write_ack_metadata_verified += 1
            if ack.storage == "MEMORY":
                self._write_ack_memory += 1
            elif ack.storage == "SPILL":
                self._write_ack_spill += 1

    def _record_write_ack_verified(self, ack: BulkWriteAck) -> None:
        with self._evidence_lock:
            self._write_acks_verified += 1

    def _record_write_ack_stream_eof_verified(self) -> None:
        with self._evidence_lock:
            self._write_ack_streams_eof_verified += 1

    def _record_write_ack_verification_failure(self, exc: BaseException) -> None:
        if (
            isinstance(exc, (EOFError, asyncio.IncompleteReadError))
            or "bulk write ack" in str(exc)
        ):
            with self._evidence_lock:
                self._write_ack_verification_failures += 1

    def _begin_write_stream(self) -> None:
        with self._evidence_lock:
            self._attempted_write_streams += 1
            self._active_write_streams += 1
            self._max_active_write_streams = max(
                self._max_active_write_streams,
                self._active_write_streams,
            )

    def _finish_write_stream(self) -> None:
        with self._evidence_lock:
            self._active_write_streams = max(0, self._active_write_streams - 1)


class CxnfsQuicBulkClient:
    def __init__(
        self,
        control_client,
        bulk_transport: BulkFrameTransport,
        read_config: CxnfsBulkReadConfig | None = None,
        *,
        close_control_client: bool = False,
        retry_config: CxnfsBulkRetryConfig | None = None,
        bulk_transport_reconnector: Callable[[], BulkFrameTransport] | None = None,
    ) -> None:
        if control_client is None:
            raise ValueError("control_client is required")
        if bulk_transport is None:
            raise ValueError("bulk_transport is required")
        self._control_client = control_client
        self._bulk_transport = bulk_transport
        self._read_config = read_config or CxnfsBulkReadConfig()
        self._payload_transport = CxnfsBulkPayloadTransport(
            self._control_client,
            self._bulk_transport,
            self._read_config,
            retry_config=retry_config,
            bulk_transport_reconnector=bulk_transport_reconnector,
        )
        self._close_control_client = close_control_client

    @classmethod
    def connect(
        cls,
        control_client,
        host: str,
        port: int,
        *,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        read_config: CxnfsBulkReadConfig | None = None,
        write_connections: int | None = None,
        close_control_client: bool = False,
        cafile: str | None = None,
        retry_config: CxnfsBulkRetryConfig | None = None,
    ) -> CxnfsQuicBulkClient:
        if write_connections is not None and (write_connections <= 0 or write_connections > ABSOLUTE_MAX_CORDS):
            raise ValueError(f"write_connections must be between 1 and {ABSOLUTE_MAX_CORDS}")
        def connect_bulk_transport() -> BulkFrameTransport:
            return QuicBulkFrameTransport.connect(
                host,
                port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
                cafile=cafile,
            )

        bulk_transport = connect_bulk_transport()
        return cls(
            control_client,
            bulk_transport,
            read_config=read_config or CxnfsBulkReadConfig(),
            close_control_client=close_control_client,
            retry_config=retry_config if retry_config is not None else CxnfsBulkRetryConfig(read_retries=1, write_retries=1),
            bulk_transport_reconnector=connect_bulk_transport,
        )

    def dataset_client(self):
        from .dataset import CxnfsPythonClient

        return CxnfsPythonClient(self._payload_transport)

    def read_mds_shard(self, shard_path, offset: int = 0, length: int | None = None) -> bytes:
        return self.dataset_client().read_mds_shard(shard_path, offset, length)

    def read_tensor(
        self,
        shard_path,
        offset: int,
        nbytes: int,
        dtype: str,
        shape: tuple[int, ...],
        target: TensorMemoryTarget = TensorMemoryTarget.PYTORCH_PINNED_CPU,
        device: str = "cpu",
        non_blocking: bool = True,
    ):
        return self.dataset_client().read_tensor(
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            target,
            device,
            non_blocking,
        )

    def send_safetensors_output(self, output_dir, remote_root: str | Path | None = None):
        return self.dataset_client().send_safetensors_output(output_dir, remote_root=remote_root)

    def write_tensor(self, path, tensor, nbytes: int) -> SendResult:
        return self.dataset_client().write_tensor(path, tensor, nbytes)

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck | None:
        return self._payload_transport.write_frame(frame)

    def read_path_evidence(self) -> dict[str, Any]:
        return self._payload_transport.read_path_evidence()

    def write_path_evidence(self) -> dict[str, Any]:
        return self._payload_transport.write_path_evidence()

    def read_into_buffer(self, shard_path, offset: int, buffer, nbytes: int | None = None) -> int:
        return self.dataset_client().read_into_buffer(shard_path, offset, buffer, nbytes)

    def read_into_buffers(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return self.dataset_client().read_into_buffers(reads, max_workers=max_workers)

    def read_into_tensor(
        self,
        shard_path,
        offset: int,
        tensor,
        nbytes: int,
        target=None,
        device: str | None = None,
        non_blocking: bool = True,
    ) -> int:
        return self.dataset_client().read_into_tensor(
            shard_path,
            offset,
            tensor,
            nbytes,
            target,
            device,
            non_blocking,
        )

    def read_into_tensors(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return self.dataset_client().read_into_tensors(reads, max_workers=max_workers)

    def close(self) -> None:
        self._payload_transport.close()
        if self._close_control_client:
            close = getattr(self._control_client, "close", None)
            if callable(close):
                close()

    def __enter__(self) -> CxnfsQuicBulkClient:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


class CxnfsLengthPrefixedBulkClient:
    def __init__(
        self,
        control_client,
        bulk_transport: BulkFrameTransport,
        read_config: CxnfsBulkReadConfig | None = None,
        *,
        close_control_client: bool = False,
    ) -> None:
        if control_client is None:
            raise ValueError("control_client is required")
        if bulk_transport is None:
            raise ValueError("bulk_transport is required")
        self._control_client = control_client
        self._bulk_transport = bulk_transport
        self._read_config = read_config or CxnfsBulkReadConfig()
        self._payload_transport = CxnfsBulkPayloadTransport(
            self._control_client,
            self._bulk_transport,
            self._read_config,
        )
        self._close_control_client = close_control_client

    @classmethod
    def connect(
        cls,
        control_client,
        host: str,
        port: int,
        *,
        timeout: float | None = 10.0,
        tls_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        read_config: CxnfsBulkReadConfig | None = None,
        write_connections: int | None = None,
        close_control_client: bool = False,
    ) -> CxnfsLengthPrefixedBulkClient:
        client_config = read_config or CxnfsBulkReadConfig()
        writer_count = write_connections if write_connections is not None else client_config.initial_cords
        if writer_count <= 0 or writer_count > ABSOLUTE_MAX_CORDS:
            raise ValueError(f"write_connections must be between 1 and {ABSOLUTE_MAX_CORDS}")
        if writer_count == 1:
            bulk_transport = LengthPrefixedBulkFrameTransport.connect(
                host,
                port,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
            )
        else:
            bulk_transport = PooledLengthPrefixedBulkFrameTransport.connect(
                host,
                port,
                write_connections=writer_count,
                timeout=timeout,
                tls_context=tls_context,
                server_hostname=server_hostname,
            )
        return cls(
            control_client,
            bulk_transport,
            read_config=client_config,
            close_control_client=close_control_client,
        )

    def dataset_client(self):
        from .dataset import CxnfsPythonClient

        return CxnfsPythonClient(self._payload_transport)

    def read_mds_shard(self, shard_path, offset: int = 0, length: int | None = None) -> bytes:
        return self.dataset_client().read_mds_shard(shard_path, offset, length)

    def read_tensor(
        self,
        shard_path,
        offset: int,
        nbytes: int,
        dtype: str,
        shape: tuple[int, ...],
        target: TensorMemoryTarget = TensorMemoryTarget.PYTORCH_PINNED_CPU,
        device: str = "cpu",
        non_blocking: bool = True,
    ):
        return self.dataset_client().read_tensor(
            shard_path,
            offset,
            nbytes,
            dtype,
            shape,
            target,
            device,
            non_blocking,
        )

    def send_safetensors_output(self, output_dir, remote_root: str | Path | None = None):
        return self.dataset_client().send_safetensors_output(output_dir, remote_root=remote_root)

    def write_tensor(self, path, tensor, nbytes: int) -> SendResult:
        return self.dataset_client().write_tensor(path, tensor, nbytes)

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck | None:
        return self._bulk_transport.write_frame(frame)

    def read_path_evidence(self) -> dict[str, Any]:
        return self._payload_transport.read_path_evidence()

    def write_path_evidence(self) -> dict[str, Any]:
        return self._payload_transport.write_path_evidence()

    def read_into_buffer(self, shard_path, offset: int, buffer, nbytes: int | None = None) -> int:
        return self.dataset_client().read_into_buffer(shard_path, offset, buffer, nbytes)

    def read_into_buffers(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return self.dataset_client().read_into_buffers(reads, max_workers=max_workers)

    def read_into_tensor(
        self,
        shard_path,
        offset: int,
        tensor,
        nbytes: int,
        target=None,
        device: str | None = None,
        non_blocking: bool = True,
    ) -> int:
        return self.dataset_client().read_into_tensor(
            shard_path,
            offset,
            tensor,
            nbytes,
            target,
            device,
            non_blocking,
        )

    def read_into_tensors(self, reads, *, max_workers: int | None = None) -> tuple[int, ...]:
        return self.dataset_client().read_into_tensors(reads, max_workers=max_workers)

    def close(self) -> None:
        self._bulk_transport.close()
        if self._close_control_client:
            close = getattr(self._control_client, "close", None)
            if callable(close):
                close()

    def __enter__(self) -> CxnfsLengthPrefixedBulkClient:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


class BulkReadFrameReceiver:
    def __init__(
        self,
        transfer: TransferAcceptedResponse,
        plan: ReadPlan,
        *,
        entry_sequence: int = 0,
    ) -> None:
        if transfer.direction != TransferDirection.READ:
            raise ValueError("bulk read receiver requires a read transfer")
        if entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        self._transfer = transfer
        self._plan = plan
        self._entry_sequence = entry_sequence
        self._expected = {chunk.chunk_index: chunk for chunk in plan.chunks}
        self._frames: dict[int, BulkDataFrame] = {}

    def accept(self, frame: BulkDataFrame) -> int:
        if frame.transfer_id != self._transfer.transfer_id:
            raise ValueError(f"bulk frame belongs to a different transfer: {frame.transfer_id}")
        if frame.entry_sequence != self._entry_sequence:
            raise ValueError(f"bulk frame belongs to a different entry sequence: {frame.entry_sequence}")
        if frame.cord_index >= self._transfer.accepted_max_cords:
            raise ValueError(f"bulk frame cord is outside accepted cord range: {frame.cord_index}")
        chunk = self._expected.get(frame.chunk_sequence)
        if chunk is None:
            raise ValueError(f"unexpected bulk frame chunk sequence: {frame.chunk_sequence}")
        if frame.chunk_sequence in self._frames:
            raise ValueError(f"duplicate bulk frame chunk sequence: {frame.chunk_sequence}")
        _validate_full_chunk_frame(chunk, frame)
        self._frames[frame.chunk_sequence] = frame
        return len(self._frames)

    def complete(self) -> bytes:
        missing = [chunk.chunk_index for chunk in self._plan.chunks if chunk.chunk_index not in self._frames]
        if missing:
            raise ValueError(f"missing bulk frame chunk sequence: {missing[0]}")

        assembled = bytearray()
        for chunk in self._plan.chunks:
            frame = self._frames[chunk.chunk_index]
            assembled.extend(frame.payload[chunk.offset_in_chunk:chunk.offset_in_chunk + chunk.bytes_to_read])
        return bytes(assembled)

    @property
    def received_count(self) -> int:
        return len(self._frames)


class BulkReadBufferReceiver:
    def __init__(
        self,
        transfer: TransferAcceptedResponse,
        plan: ReadPlan,
        destination,
        *,
        entry_sequence: int = 0,
    ) -> None:
        if transfer.direction != TransferDirection.READ:
            raise ValueError("bulk read buffer receiver requires a read transfer")
        if entry_sequence < 0:
            raise ValueError("entry_sequence must be non-negative")
        view = memoryview(destination)
        if view.readonly:
            raise ValueError("destination buffer must be writable")
        self._destination = view if view.format == "B" and view.ndim == 1 else view.cast("B")
        if plan.requested_range.length > self._destination.nbytes:
            raise ValueError("destination buffer is smaller than requested read range")
        self._transfer = transfer
        self._plan = plan
        self._entry_sequence = entry_sequence
        self._expected = {chunk.chunk_index: chunk for chunk in plan.chunks}
        self._received: set[int] = set()
        self._bytes_written = 0

    def accept(self, frame: BulkDataFrame) -> int:
        if frame.transfer_id != self._transfer.transfer_id:
            raise ValueError(f"bulk frame belongs to a different transfer: {frame.transfer_id}")
        if frame.entry_sequence != self._entry_sequence:
            raise ValueError(f"bulk frame belongs to a different entry sequence: {frame.entry_sequence}")
        if frame.cord_index >= self._transfer.accepted_max_cords:
            raise ValueError(f"bulk frame cord is outside accepted cord range: {frame.cord_index}")
        chunk = self._expected.get(frame.chunk_sequence)
        if chunk is None:
            raise ValueError(f"unexpected bulk frame chunk sequence: {frame.chunk_sequence}")
        if frame.chunk_sequence in self._received:
            raise ValueError(f"duplicate bulk frame chunk sequence: {frame.chunk_sequence}")
        _validate_full_chunk_frame(chunk, frame)
        destination_offset = chunk.read_file_offset - self._plan.requested_range.offset
        if destination_offset < 0 or destination_offset + chunk.bytes_to_read > self._destination.nbytes:
            raise ValueError(f"bulk frame slice is outside destination buffer: {frame.chunk_sequence}")
        source_start = chunk.offset_in_chunk
        source_end = source_start + chunk.bytes_to_read
        destination_end = destination_offset + chunk.bytes_to_read
        self._destination[destination_offset:destination_end] = frame.payload[source_start:source_end]
        self._received.add(frame.chunk_sequence)
        self._bytes_written += chunk.bytes_to_read
        return len(self._received)

    def complete(self) -> int:
        missing = [chunk.chunk_index for chunk in self._plan.chunks if chunk.chunk_index not in self._received]
        if missing:
            raise ValueError(f"missing bulk frame chunk sequence: {missing[0]}")
        return self._bytes_written

    def clear_destination(self) -> int:
        length = min(self._plan.requested_range.length, self._destination.nbytes)
        cursor = 0
        while cursor < length:
            end = min(length, cursor + len(ZERO_FILL_BLOCK))
            self._destination[cursor:end] = ZERO_FILL_BLOCK[:end - cursor]
            cursor = end
        self._received.clear()
        self._bytes_written = 0
        return length

    @property
    def received_count(self) -> int:
        return len(self._received)


def _partition_read_plan(plan: ReadPlan, partition_count: int) -> tuple[ReadPlan, ...]:
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    if not plan.chunks:
        return (plan,)
    count = min(partition_count, len(plan.chunks))
    partitions = []
    cursor = 0
    for index in range(count):
        remaining = len(plan.chunks) - cursor
        remaining_partitions = count - index
        size = (remaining + remaining_partitions - 1) // remaining_partitions
        chunks = plan.chunks[cursor:cursor + size]
        first = chunks[0]
        last = chunks[-1]
        offset = first.read_file_offset
        end = last.read_file_offset + last.bytes_to_read
        partitions.append(ReadPlan(
            plan.file_hash,
            plan.file_size,
            ByteRange(offset, end - offset),
            chunks,
        ))
        cursor += size
    return tuple(partitions)


def _quic_read_partition_evidence(partitions: Iterable[ReadPlan], plan: ReadPlan) -> dict[str, Any]:
    partition_tuple = tuple(partitions)
    entries: list[dict[str, Any]] = []
    requested_bytes = 0
    payload_bytes = 0
    chunk_count = 0
    for index, partition in enumerate(partition_tuple):
        chunks = tuple(partition.chunks)
        partition_payload_bytes = sum(chunk.bytes_to_read for chunk in chunks)
        first_chunk = chunks[0] if chunks else None
        last_chunk = chunks[-1] if chunks else None
        entry = {
            "stream_index": index,
            "offset": partition.requested_range.offset,
            "length": partition.requested_range.length,
            "chunk_count": len(chunks),
            "payload_bytes": partition_payload_bytes,
            "first_chunk_sequence": -1 if first_chunk is None else first_chunk.chunk_index,
            "last_chunk_sequence": -1 if last_chunk is None else last_chunk.chunk_index,
        }
        entries.append(entry)
        requested_bytes += partition.requested_range.length
        payload_bytes += partition_payload_bytes
        chunk_count += len(chunks)
    return {
        "read_partition_count": len(partition_tuple),
        "read_partition_chunk_count": chunk_count,
        "read_partition_requested_bytes": requested_bytes,
        "read_partition_payload_bytes": payload_bytes,
        "read_partition_covers_plan": (
            chunk_count == len(plan.chunks)
            and payload_bytes == plan.requested_range.length
        ),
        "read_partitions": entries,
    }


def _empty_quic_read_partition_evidence() -> dict[str, Any]:
    return {
        "read_partition_count": 0,
        "read_partition_chunk_count": 0,
        "read_partition_requested_bytes": 0,
        "read_partition_payload_bytes": 0,
        "read_partition_covers_plan": False,
        "read_partitions": [],
    }


def _read_partition(
    transport: BulkFrameTransport,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
) -> tuple[BulkDataFrame, ...]:
    return tuple(transport.read_frames(session_id, transfer, path, plan))


def _read_frames_into_receiver(
    transport: BulkFrameTransport,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
    receiver: BulkReadFrameReceiver | BulkReadBufferReceiver,
) -> tuple[int, bool]:
    read_frames_into = getattr(transport, "read_frames_into", None)
    if callable(read_frames_into):
        return int(read_frames_into(session_id, transfer, path, plan, receiver.accept)), True
    count = 0
    for frame in transport.read_frames(session_id, transfer, path, plan):
        receiver.accept(frame)
        count += 1
    return count, False


def _observed_bulk_read_cords(transport: BulkFrameTransport) -> int:
    for name in ("last_read_stream_count", "last_read_transport_count"):
        value = getattr(transport, name, 0)
        if isinstance(value, int) and value > 0:
            return value
    return 0


def _bulk_transport_read_path_evidence(transport: BulkFrameTransport) -> dict[str, Any] | None:
    method = getattr(transport, "read_path_evidence", None)
    if not callable(method):
        return None
    evidence = method()
    return dict(evidence) if isinstance(evidence, dict) else None


def _bulk_transport_write_path_evidence(transport: BulkFrameTransport) -> dict[str, Any] | None:
    method = getattr(transport, "write_path_evidence", None)
    if not callable(method):
        return None
    evidence = method()
    return dict(evidence) if isinstance(evidence, dict) else None


class _AsyncioLoopThread:
    def __init__(self, timeout: float | None) -> None:
        self._timeout = timeout
        self._closed = Event()
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target=self._run, name="cxnfs-quic-bulk", daemon=True)
        self._thread.start()

    def run(self, coroutine):
        if self._closed.is_set():
            raise OSError("QUIC bulk event loop is closed")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=self._timeout)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=self._timeout)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            pending = tuple(asyncio.all_tasks(self._loop))
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()


async def _connect_aioquic(
    host: str,
    port: int,
    *,
    timeout: float | None,
    tls_context: ssl.SSLContext | None,
    server_hostname: str | None,
    cafile: str | None,
):
    try:
        from aioquic.asyncio.client import connect as aioquic_connect
        from aioquic.quic.configuration import QuicConfiguration
        from aioquic.tls import CipherSuite
    except ImportError as exc:  # pragma: no cover - exercised when optional dependency is absent
        raise RuntimeError(
            "Python QUIC bulk transport requires the optional aioquic dependency; "
            "install cxnfs[quic] or use --bulk-transport stream"
        ) from exc

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=[QUIC_BULK_APPLICATION_PROTOCOL],
        server_name=server_hostname or host,
        idle_timeout=float(timeout or 60.0),
        max_data=256 * 1024 * 1024,
        max_stream_data=64 * 1024 * 1024,
    )
    configuration.cipher_suites = [CipherSuite.AES_128_GCM_SHA256]
    if cafile:
        configuration.load_verify_locations(cafile=cafile)
    if tls_context is not None and getattr(tls_context, "verify_mode", None) == ssl.CERT_NONE:
        configuration.verify_mode = ssl.CERT_NONE

    context_manager = aioquic_connect(
        host,
        port,
        configuration=configuration,
        wait_connected=True,
    )
    if timeout is None:
        protocol = await context_manager.__aenter__()
    else:
        protocol = await asyncio.wait_for(context_manager.__aenter__(), timeout=timeout)
    return context_manager, protocol


async def _close_aioquic(context_manager) -> None:
    await context_manager.__aexit__(None, None, None)


async def _quic_read_frames(
    protocol,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
) -> tuple[BulkDataFrame, ...]:
    frames: list[BulkDataFrame] = []
    await _quic_read_frames_into(protocol, session_id, transfer, path, plan, frames.append)
    return tuple(frames)


async def _quic_read_frames_into(
    protocol,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
    accept: Callable[[BulkDataFrame], Any],
) -> int:
    stream_count = _quic_stream_count(transfer, plan)
    if stream_count == 1:
        return await _quic_read_partition_into(protocol, session_id, transfer, path, plan, stream_count, accept)
    partitions = _partition_read_plan(plan, stream_count)
    tasks = [
        asyncio.create_task(
            _quic_read_partition_into(protocol, session_id, transfer, path, partition, stream_count, accept)
        )
        for partition in partitions
    ]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return sum(results)


async def _quic_read_partition(
    protocol,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
    active_cords: int,
) -> tuple[BulkDataFrame, ...]:
    frames: list[BulkDataFrame] = []
    await _quic_read_partition_into(protocol, session_id, transfer, path, plan, active_cords, frames.append)
    return tuple(frames)


async def _quic_read_partition_into(
    protocol,
    session_id: UUID,
    transfer: TransferAcceptedResponse,
    path: str,
    plan: ReadPlan,
    active_cords: int,
    accept: Callable[[BulkDataFrame], Any],
) -> int:
    reader, writer = await protocol.create_stream(is_unidirectional=False)
    request = BulkReadRequest(
        session_id,
        transfer.transfer_id,
        path,
        plan.requested_range.offset,
        plan.requested_range.length,
        active_cords,
    )
    await _write_prefixed_async(writer, encode_bulk_read_request(request), MAX_BULK_READ_REQUEST_BYTES)
    await _write_eof_async(writer)
    expected = {chunk.chunk_index: chunk for chunk in plan.chunks}
    seen: set[int] = set()
    count = 0
    for _ in plan.chunks:
        frame = decode_bulk_frame(await _read_prefixed_async(reader, MAX_ENCODED_BULK_FRAME_BYTES))
        _validate_quic_bulk_read_frame(frame, transfer, active_cords, expected, seen)
        accept(frame)
        count += 1
    await _require_quic_read_stream_complete(reader)
    return count


def _validate_quic_bulk_read_frame(
    frame: BulkDataFrame,
    transfer: TransferAcceptedResponse,
    active_cords: int,
    expected: Mapping[int, PlannedChunkRead],
    seen: set[int],
) -> None:
    if frame.transfer_id != transfer.transfer_id:
        raise ValueError(f"QUIC bulk frame belongs to a different transfer: {frame.transfer_id}")
    if frame.entry_sequence != 0:
        raise ValueError(f"QUIC bulk frame belongs to a different entry sequence: {frame.entry_sequence}")
    if frame.cord_index >= active_cords:
        raise ValueError(f"QUIC bulk frame cord is outside active cord set: {frame.cord_index}")
    if frame.cord_index >= transfer.accepted_max_cords:
        raise ValueError(f"QUIC bulk frame cord is outside accepted cord range: {frame.cord_index}")
    chunk = expected.get(frame.chunk_sequence)
    if chunk is None:
        raise ValueError(f"unexpected QUIC bulk frame chunk sequence: {frame.chunk_sequence}")
    if frame.chunk_sequence in seen:
        raise ValueError(f"duplicate QUIC bulk frame chunk sequence: {frame.chunk_sequence}")
    _validate_full_chunk_frame(chunk, frame)
    seen.add(frame.chunk_sequence)


async def _quic_write_frame(protocol, frame: BulkDataFrame) -> BulkWriteAck:
    reader, writer = await protocol.create_stream(is_unidirectional=False)
    await _write_prefixed_async(writer, encode_bulk_frame(frame), MAX_ENCODED_BULK_FRAME_BYTES)
    await _write_eof_async(writer)
    ack = decode_bulk_write_ack(await _read_prefixed_async(reader, MAX_ENCODED_BULK_WRITE_ACK_BYTES))
    await _require_quic_write_ack_stream_complete(reader)
    return ack


async def _require_quic_read_stream_complete(reader) -> None:
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise ValueError("truncated extra QUIC bulk frame after read completed") from exc
        return
    except EOFError:
        return
    length = struct.unpack(">i", header)[0]
    raise ValueError(f"extra QUIC bulk frame after read completed: {length} bytes")


async def _require_quic_write_ack_stream_complete(reader) -> None:
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise ValueError("truncated extra QUIC bulk write ack after ack completed") from exc
        return
    except EOFError:
        return
    length = struct.unpack(">i", header)[0]
    raise ValueError(f"extra QUIC bulk write ack after ack completed: {length} bytes")


async def _write_prefixed_async(writer, payload: bytes, maximum: int) -> None:
    if not payload or len(payload) > maximum:
        raise ValueError(f"invalid encoded QUIC bulk payload length: {len(payload)}")
    writer.write(struct.pack(">i", len(payload)) + payload)
    drain = getattr(writer, "drain", None)
    if callable(drain):
        await drain()


async def _write_eof_async(writer) -> None:
    write_eof = getattr(writer, "write_eof", None)
    if callable(write_eof):
        write_eof()
    else:
        close = getattr(writer, "close", None)
        if callable(close):
            close()
    drain = getattr(writer, "drain", None)
    if callable(drain):
        await drain()


async def _read_prefixed_async(reader, maximum: int) -> bytes:
    header = await reader.readexactly(4)
    length = struct.unpack(">i", header)[0]
    if length <= 0 or length > maximum:
        raise ValueError(f"invalid encoded QUIC bulk payload length: {length}")
    return await reader.readexactly(length)


def _quic_stream_count(transfer: TransferAcceptedResponse, plan: ReadPlan) -> int:
    return max(1, min(
        transfer.accepted_max_cords,
        len(plan.chunks) if plan.chunks else 1,
    ))


class CxnfsBulkPayloadTransport:
    def __init__(
        self,
        control_client,
        bulk_transport: BulkFrameTransport,
        read_config: CxnfsBulkReadConfig | None = None,
        *,
        retry_config: CxnfsBulkRetryConfig | None = None,
        bulk_transport_reconnector: Callable[[], BulkFrameTransport] | None = None,
    ) -> None:
        self._control_client = control_client
        self._bulk_transport = bulk_transport
        self._read_config = read_config or CxnfsBulkReadConfig()
        self._retry_config = retry_config or CxnfsBulkRetryConfig()
        self._bulk_transport_reconnector = bulk_transport_reconnector
        self._evidence_lock = RLock()
        self._bulk_transport_replace_lock = RLock()
        self._control_operation_lock = RLock()
        self._read_operation_count = 0
        self._byte_return_read_operations = 0
        self._direct_buffer_read_operations = 0
        self._direct_tensor_read_operations = 0
        self._direct_destination_read_operations = 0
        self._frame_sink_read_operations = 0
        self._direct_destination_frame_sink_read_operations = 0
        self._direct_destination_parallel_frame_sink_read_operations = 0
        self._direct_destination_parallel_frame_sink_frames = 0
        self._direct_destination_last_observed_cords = 0
        self._direct_destination_max_observed_cords = 0
        self._fallback_frame_iter_read_operations = 0
        self._failed_read_operations = 0
        self._failed_byte_return_read_operations = 0
        self._failed_direct_buffer_read_operations = 0
        self._failed_direct_tensor_read_operations = 0
        self._failed_direct_destination_read_operations = 0
        self._direct_destination_zeroed_bytes_on_failure = 0
        self._read_retry_attempts = 0
        self._read_retry_successes = 0
        self._bulk_transport_reconnects = 0
        self._last_read_retry_error = ""
        self._write_retry_attempts = 0
        self._write_retry_successes = 0
        self._bulk_transport_write_reconnects = 0
        self._last_write_retry_error = ""
        self._frame_sink_frames = 0
        self._fallback_frame_iter_frames = 0
        self._payload_bytes_read = 0
        self._max_observed_cords = 0
        self._last_observed_cords = 0
        self._last_read_accepted_initial_cords = 0
        self._last_read_accepted_max_cords = 0
        self._write_operation_count = 0
        self._direct_tensor_write_operations = 0
        self._directory_output_write_operations = 0
        self._payload_bytes_written = 0
        self._write_frame_count = 0
        self._write_ack_count = 0
        self._write_ack_frame_verified_count = 0
        self._write_ack_metadata_verified_count = 0
        self._write_ack_memory_count = 0
        self._write_ack_spill_count = 0
        self._metadata_after_payload_entries = 0
        self._write_entries_committed = 0
        self._write_transfers_committed = 0
        self._last_write_accepted_initial_cords = 0
        self._last_write_accepted_max_cords = 0
        self._max_observed_write_cords = 0

    def read_path_evidence(self) -> dict[str, Any]:
        bulk_transport_evidence = _bulk_transport_read_path_evidence(self._bulk_transport)
        with self._evidence_lock:
            direct_destination_reads = self._direct_destination_read_operations
            direct_sink_reads = self._direct_destination_frame_sink_read_operations
            direct_parallel_sink_reads = self._direct_destination_parallel_frame_sink_read_operations
            direct_destination_max_cords = self._direct_destination_max_observed_cords
            evidence = {
                "bulk_transport_class": type(self._bulk_transport).__name__,
                "transport_frame_sink_supported": callable(getattr(self._bulk_transport, "read_frames_into", None)),
                "transport_frame_sink_observed": self._frame_sink_read_operations > 0,
                "read_operations": self._read_operation_count,
                "byte_return_read_operations": self._byte_return_read_operations,
                "direct_buffer_read_operations": self._direct_buffer_read_operations,
                "direct_tensor_read_operations": self._direct_tensor_read_operations,
                "direct_destination_read_operations": direct_destination_reads,
                "frame_sink_read_operations": self._frame_sink_read_operations,
                "direct_destination_frame_sink_read_operations": direct_sink_reads,
                "direct_destination_parallel_frame_sink_read_operations": direct_parallel_sink_reads,
                "direct_destination_parallel_frame_sink_frames": self._direct_destination_parallel_frame_sink_frames,
                "direct_destination_last_observed_cords": self._direct_destination_last_observed_cords,
                "direct_destination_max_observed_cords": direct_destination_max_cords,
                "direct_destination_used_parallel_cords": direct_destination_max_cords > 1,
                "fallback_frame_iter_read_operations": self._fallback_frame_iter_read_operations,
                "failed_read_operations": self._failed_read_operations,
                "failed_byte_return_read_operations": self._failed_byte_return_read_operations,
                "failed_direct_buffer_read_operations": self._failed_direct_buffer_read_operations,
                "failed_direct_tensor_read_operations": self._failed_direct_tensor_read_operations,
                "failed_direct_destination_read_operations": self._failed_direct_destination_read_operations,
                "direct_destination_zeroed_bytes_on_failure": self._direct_destination_zeroed_bytes_on_failure,
                "direct_destination_failure_zeroing_enabled": True,
                "read_retry_limit": self._retry_config.read_retries,
                "read_retry_attempts": self._read_retry_attempts,
                "read_retry_successes": self._read_retry_successes,
                "bulk_transport_reconnects": self._bulk_transport_reconnects,
                "last_read_retry_error": self._last_read_retry_error,
                "frame_sink_frames": self._frame_sink_frames,
                "fallback_frame_iter_frames": self._fallback_frame_iter_frames,
                "payload_bytes_read": self._payload_bytes_read,
                "max_observed_cords": self._max_observed_cords,
                "last_observed_cords": self._last_observed_cords,
                "last_accepted_initial_cords": self._last_read_accepted_initial_cords,
                "last_accepted_max_cords": self._last_read_accepted_max_cords,
                "observed_cords_within_admission": (
                    self._max_observed_cords <= self._last_read_accepted_max_cords
                    if self._last_read_accepted_max_cords > 0
                    else True
                ),
                "frame_sink_used_for_all_direct_destination_reads": (
                    direct_destination_reads > 0 and direct_sink_reads == direct_destination_reads
                ),
                "direct_destination_frame_sink_cord_fanout_verified": (
                    direct_destination_reads > 0
                    and direct_sink_reads == direct_destination_reads
                    and direct_destination_max_cords > 1
                ),
            }
        if bulk_transport_evidence is not None:
            evidence["bulk_transport_read_evidence"] = bulk_transport_evidence
        return evidence

    def write_path_evidence(self) -> dict[str, Any]:
        bulk_transport_evidence = _bulk_transport_write_path_evidence(self._bulk_transport)
        write_ack_stream_completion_verified = bool(
            bulk_transport_evidence
            and bulk_transport_evidence.get("write_ack_stream_completion_verified")
        )
        write_ack_stream_eof_verified = bool(
            bulk_transport_evidence
            and bulk_transport_evidence.get("write_ack_stream_eof_verified")
        )
        with self._evidence_lock:
            write_ack_metadata_pending = self._write_ack_count - self._write_ack_metadata_verified_count
            evidence = {
                "format": "cxnfs.bulk-write-path-evidence.v1",
                "write_operations": self._write_operation_count,
                "direct_tensor_write_operations": self._direct_tensor_write_operations,
                "directory_output_write_operations": self._directory_output_write_operations,
                "payload_bytes_written": self._payload_bytes_written,
                "write_frames": self._write_frame_count,
                "write_acks": self._write_ack_count,
                "write_ack_frame_verified": self._write_ack_frame_verified_count,
                "write_ack_frame_identity_verified": (
                    self._write_frame_count > 0
                    and self._write_ack_count == self._write_frame_count
                    and self._write_ack_frame_verified_count == self._write_frame_count
                ),
                "write_ack_stream_completion_verified": write_ack_stream_completion_verified,
                "write_ack_stream_eof_verified": write_ack_stream_eof_verified,
                "write_ack_metadata_verified": self._write_ack_metadata_verified_count,
                "write_ack_metadata_pending": write_ack_metadata_pending,
                "write_ack_memory": self._write_ack_memory_count,
                "write_ack_spill": self._write_ack_spill_count,
                "metadata_after_payload_entries": self._metadata_after_payload_entries,
                "entries_committed": self._write_entries_committed,
                "transfers_committed": self._write_transfers_committed,
                "last_accepted_initial_cords": self._last_write_accepted_initial_cords,
                "last_accepted_max_cords": self._last_write_accepted_max_cords,
                "max_observed_write_cords": self._max_observed_write_cords,
                "write_retry_limit": self._retry_config.write_retries,
                "write_retry_attempts": self._write_retry_attempts,
                "write_retry_successes": self._write_retry_successes,
                "bulk_transport_write_reconnects": self._bulk_transport_write_reconnects,
                "last_write_retry_error": self._last_write_retry_error,
            }
        if bulk_transport_evidence is not None:
            evidence["bulk_transport_write_evidence"] = bulk_transport_evidence
        return evidence

    def read_mds_shard(self, operation: MdsShardReadOperation) -> bytes:
        if operation.length is None:
            raise ValueError("bulk payload reads require an explicit length")
        last_error: BaseException | None = None
        for attempt in range(self._retry_config.read_attempts):
            with self._bulk_transport_replace_lock:
                bulk_transport = self._bulk_transport
            try:
                payload = self._read_mds_shard_once(operation, bulk_transport)
            except Exception as exc:
                last_error = exc
                self._record_byte_return_read_failure()
                if not self._prepare_read_retry(exc, attempt, bulk_transport):
                    raise
                continue
            if attempt > 0:
                self._record_read_retry_success()
            return payload
        if last_error is not None:
            raise last_error
        raise RuntimeError("bulk read retry loop exited without result")

    def _read_mds_shard_once(self, operation: MdsShardReadOperation, bulk_transport: BulkFrameTransport) -> bytes:
        with self._control_operation_lock:
            transfer = self._control_client.bootstrap_read_transfer(
                operation.shard_path,
                payload_format=TransferPayloadFormat.MDS_SHARDS,
                initial_cords=self._read_config.initial_cords,
                max_cords=self._read_config.max_cords,
                provisional_container_size_bytes=self._read_config.provisional_container_size_bytes,
                expected_size_limit_bytes=self._read_config.expected_size_limit_bytes,
            )
            response = self._control_client.read_file_range(
                operation.shard_path,
                operation.offset,
                operation.length,
            )
            _require_read_response_matches_request(operation.shard_path, operation.offset, operation.length, response)
            session_id = getattr(self._control_client, "session_id", None)
            if session_id is None:
                raise ValueError("authenticated control session id is required for bulk reads")
        receiver = BulkReadFrameReceiver(transfer, response.plan)
        frame_count, used_frame_sink = _read_frames_into_receiver(
            bulk_transport,
            session_id,
            transfer,
            operation.shard_path,
            response.plan,
            receiver,
        )
        payload = receiver.complete()
        if len(payload) != operation.length:
            raise ValueError("short bulk payload read")
        self._record_read_path_evidence(
            byte_return=True,
            direct_buffer=False,
            direct_tensor=False,
            used_frame_sink=used_frame_sink,
            frame_count=frame_count,
            byte_count=len(payload),
            bulk_transport=bulk_transport,
            transfer=transfer,
        )
        return payload

    def send_safetensors_output(self, operation: SafetensorsSendOperation) -> SendResult:
        root = Path(operation.output_dir)
        if not root.is_dir():
            raise FileNotFoundError(f"output directory does not exist: {root}")
        session_id = getattr(self._control_client, "session_id", None)
        if session_id is None:
            raise ValueError("authenticated control session id is required for bulk writes")

        remote_root = transfer_root_path(root, operation.remote_root)
        transfer = self._control_client.bootstrap_write_transfer(
            remote_root,
            payload_format=TransferPayloadFormat.SAFETENSORS,
            initial_cords=self._read_config.initial_cords,
            max_cords=self._read_config.max_cords,
            provisional_container_size_bytes=self._read_config.provisional_container_size_bytes,
            expected_size_limit_bytes=self._read_config.expected_size_limit_bytes,
        )
        if transfer.direction != TransferDirection.WRITE or not transfer.write_payload_admission_granted:
            raise ValueError("server did not admit write payload transfer")

        total_bytes = 0
        entry_hashes: list[bytes] = []
        entry_sizes: list[int] = []
        write_stats = BulkWriteFrameStats()
        metadata_after_payload_entries = 0
        entries = operation.file_entries
        if not entries:
            entries = tuple(_entry_from_path(root, file_path, file_path in set(operation.tensor_files)) for file_path in operation.files)

        in_flight_limit = _accepted_write_frame_window(transfer)
        for entry_sequence, entry in enumerate(entries):
            source = root / entry.path
            if not source.is_file():
                raise FileNotFoundError(f"send file does not exist: {source}")
            if source.stat().st_size != entry.size:
                raise ValueError(f"send file size changed before upload: {entry.path}")
            if _sha256_hex(source) != entry.sha256:
                raise ValueError(f"send file hash changed before upload: {entry.path}")

            chunk_hashes: list[bytes] = []
            chunk_sizes: list[int] = []
            offset = 0
            chunk_sequence = 0
            with _ConcurrentWriteFrameSender(
                self._bulk_transport,
                in_flight_limit,
                write_frame=self.write_frame,
            ) as frame_sender:
                with source.open("rb") as handle:
                    while True:
                        payload = handle.read(self._read_config.write_frame_payload_bytes)
                        if not payload:
                            break
                        chunk_hash = cxnfs_chunk_hash(payload)
                        chunk_hashes.append(chunk_hash)
                        chunk_sizes.append(len(payload))
                        frame = BulkDataFrame(
                            transfer.transfer_id,
                            chunk_sequence % transfer.accepted_max_cords,
                            entry_sequence,
                            chunk_sequence,
                            offset,
                            True,
                            payload,
                        )
                        frame_sender.submit(frame)
                        offset += len(payload)
                        chunk_sequence += 1
            entry_write_stats = frame_sender.stats()
            write_stats = write_stats.merge(entry_write_stats)
            if entry_write_stats.frame_count > 0:
                metadata_after_payload_entries += 1

            file_hash = cxnfs_file_hash(chunk_hashes, chunk_sizes)
            remote_path = _join_remote_path(transfer_path_root=remote_root, relative_path=entry.path)
            entry_accepted = self._control_client.accept_write_entry(
                transfer.transfer_id,
                entry_sequence,
                TransferEntryAction.CREATE_OR_REPLACE,
                remote_path,
                size_hint_bytes=entry.size,
                payload_format=TransferPayloadFormat.SAFETENSORS,
            )
            _require_write_entry_accepted(entry_accepted, transfer.transfer_id, entry_sequence, remote_path)
            metadata_accepted = self._control_client.accept_write_metadata(
                transfer.transfer_id,
                entry_sequence,
                0,
                True,
                entry.size,
                tuple(chunk_hashes),
            )
            _require_write_metadata_accepted(
                metadata_accepted,
                transfer.transfer_id,
                entry_sequence,
                0,
                len(chunk_hashes),
                True,
            )
            entry_committed = self._control_client.commit_write_entry(
                transfer.transfer_id,
                entry_sequence,
                remote_path,
                file_hash,
                entry.size,
            )
            _require_write_entry_committed(entry_committed, transfer.transfer_id, entry_sequence, remote_path, file_hash, entry.size)
            entry_hashes.append(file_hash)
            entry_sizes.append(entry.size)
            total_bytes += entry.size

        transfer_hash = cxnfs_transfer_hash(entry_hashes, entry_sizes)
        transfer_committed = self._control_client.commit_write_transfer(
            transfer.transfer_id,
            remote_root,
            transfer_hash,
            total_bytes,
        )
        _require_write_transfer_committed(transfer_committed, transfer.transfer_id, transfer_hash, total_bytes)
        self._record_write_path_evidence(
            direct_tensor=False,
            directory_output=True,
            transfer=transfer,
            stats=write_stats,
            byte_count=total_bytes,
            entries_committed=len(entries),
            metadata_after_payload_entries=metadata_after_payload_entries,
        )
        return SendResult(len(operation.files), len(operation.tensor_files), total_bytes)

    def read_tensor(self, operation: TensorReadOperation):
        if operation.target == TensorMemoryTarget.BYTES:
            return self.read_mds_shard(MdsShardReadOperation(operation.shard_path, operation.offset, operation.nbytes))

        torch = _require_torch()
        dtype = _torch_dtype(torch, operation.dtype)
        _require_tensor_read_nbytes_match(operation.nbytes, dtype, operation.shape)
        if operation.target == TensorMemoryTarget.PYTORCH_CUDA:
            staging = _torch_empty(
                torch,
                operation.shape,
                dtype=dtype,
                device="cpu",
                pin_memory=True,
            )
            self.read_into_tensor(ReadIntoTensorOperation(
                operation.shard_path,
                operation.offset,
                operation.nbytes,
                destination=TensorDestination(
                    staging,
                    TensorMemoryTarget.PYTORCH_PINNED_CPU,
                    "cpu",
                    operation.non_blocking,
                ),
                format=operation.format,
            ))
            return staging.to(operation.device, non_blocking=operation.non_blocking)

        tensor = _torch_empty(
            torch,
            operation.shape,
            dtype=dtype,
            device="cpu",
            pin_memory=operation.target == TensorMemoryTarget.PYTORCH_PINNED_CPU,
        )
        self.read_into_tensor(ReadIntoTensorOperation(
            operation.shard_path,
            operation.offset,
            operation.nbytes,
            destination=TensorDestination(
                tensor,
                operation.target,
                "cpu",
                operation.non_blocking,
            ),
            format=operation.format,
        ))
        return tensor

    def read_into_tensor(self, operation: ReadIntoTensorOperation) -> int:
        if operation.destination.target == TensorMemoryTarget.PYTORCH_CUDA:
            tensor = operation.destination.tensor
            shape = _tensor_shape(tensor)
            dtype = getattr(tensor, "dtype", None)
            if dtype is None:
                raise TypeError("cuda tensor destination must expose dtype")
            _require_tensor_read_nbytes_match(operation.nbytes, dtype, shape)
            torch = _require_torch()
            staging = _torch_empty(
                torch,
                shape,
                dtype=dtype,
                device="cpu",
                pin_memory=True,
            )
            bytes_read = self._read_into_buffer(
                BufferReadOperation(
                    operation.shard_path,
                    operation.offset,
                    operation.nbytes,
                    tensor_writable_memoryview(staging),
                    operation.format,
                ),
                tensor_destination=True,
            )
            copy_staging_to_tensor(
                staging,
                tensor,
                non_blocking=operation.destination.non_blocking,
            )
            return bytes_read

        return self._read_into_buffer(
            BufferReadOperation(
                operation.shard_path,
                operation.offset,
                operation.nbytes,
                tensor_writable_memoryview(operation.destination.tensor),
                operation.format,
            ),
            tensor_destination=True,
        )

    def read_into_buffer(self, operation: BufferReadOperation) -> int:
        return self._read_into_buffer(operation, tensor_destination=False)

    def _read_into_buffer(self, operation: BufferReadOperation, *, tensor_destination: bool) -> int:
        last_error: BaseException | None = None
        for attempt in range(self._retry_config.read_attempts):
            with self._bulk_transport_replace_lock:
                bulk_transport = self._bulk_transport
            try:
                bytes_read = self._read_into_buffer_once(
                    operation,
                    tensor_destination=tensor_destination,
                    bulk_transport=bulk_transport,
                )
            except Exception as exc:
                last_error = exc
                if not self._prepare_read_retry(exc, attempt, bulk_transport):
                    raise
                continue
            if attempt > 0:
                self._record_read_retry_success()
            return bytes_read
        if last_error is not None:
            raise last_error
        raise RuntimeError("bulk direct read retry loop exited without result")

    def _read_into_buffer_once(
        self,
        operation: BufferReadOperation,
        *,
        tensor_destination: bool,
        bulk_transport: BulkFrameTransport,
    ) -> int:
        with self._control_operation_lock:
            transfer = self._control_client.bootstrap_read_transfer(
                operation.shard_path,
                payload_format=TransferPayloadFormat.MDS_SHARDS,
                initial_cords=self._read_config.initial_cords,
                max_cords=self._read_config.max_cords,
                provisional_container_size_bytes=self._read_config.provisional_container_size_bytes,
                expected_size_limit_bytes=self._read_config.expected_size_limit_bytes,
            )
            response = self._control_client.read_file_range(
                operation.shard_path,
                operation.offset,
                operation.nbytes,
            )
            _require_read_response_matches_request(operation.shard_path, operation.offset, operation.nbytes, response)
            session_id = getattr(self._control_client, "session_id", None)
            if session_id is None:
                raise ValueError("authenticated control session id is required for bulk reads")
        receiver = BulkReadBufferReceiver(transfer, response.plan, operation.destination)
        try:
            frame_count, used_frame_sink = _read_frames_into_receiver(
                bulk_transport,
                session_id,
                transfer,
                operation.shard_path,
                response.plan,
                receiver,
            )
            bytes_read = receiver.complete()
            if bytes_read != operation.nbytes:
                raise ValueError("short bulk payload read")
        except Exception:
            zeroed_bytes = receiver.clear_destination()
            self._record_direct_destination_read_failure(
                direct_buffer=not tensor_destination,
                direct_tensor=tensor_destination,
                zeroed_bytes=zeroed_bytes,
            )
            raise
        self._record_read_path_evidence(
            byte_return=False,
            direct_buffer=not tensor_destination,
            direct_tensor=tensor_destination,
            used_frame_sink=used_frame_sink,
            frame_count=frame_count,
            byte_count=bytes_read,
            bulk_transport=bulk_transport,
            transfer=transfer,
        )
        return bytes_read

    def _prepare_read_retry(self, exc: BaseException, attempt: int, failed_transport: BulkFrameTransport) -> bool:
        if attempt >= self._retry_config.read_retries:
            return False
        if not _is_retryable_bulk_read_error(exc):
            return False
        if self._bulk_transport_reconnector is None:
            return False
        self._record_read_retry_attempt(exc)
        old_transport: BulkFrameTransport | None = None
        with self._bulk_transport_replace_lock:
            if self._bulk_transport is failed_transport:
                try:
                    replacement = self._bulk_transport_reconnector()
                except Exception:
                    _close_transports_suppressing((failed_transport,))
                    raise
                if replacement is None:
                    _close_transports_suppressing((failed_transport,))
                    raise RuntimeError("bulk transport reconnector returned None")
                old_transport = self._bulk_transport
                self._bulk_transport = replacement
                self._bulk_transport_reconnects += 1
        if old_transport is not None:
            _close_transports_suppressing((old_transport,))
        return True

    def write_frame(self, frame: BulkDataFrame) -> BulkWriteAck | None:
        last_error: BaseException | None = None
        for attempt in range(self._retry_config.write_attempts):
            with self._bulk_transport_replace_lock:
                bulk_transport = self._bulk_transport
            try:
                ack = bulk_transport.write_frame(frame)
                if ack is not None:
                    _require_write_ack_matches_frame(frame, ack)
            except Exception as exc:
                last_error = exc
                if not self._prepare_write_retry(exc, attempt, bulk_transport):
                    raise
                continue
            if attempt > 0:
                self._record_write_retry_success()
            return ack
        if last_error is not None:
            raise last_error
        raise RuntimeError("bulk write retry loop exited without result")

    def _prepare_write_retry(self, exc: BaseException, attempt: int, failed_transport: BulkFrameTransport) -> bool:
        if attempt >= self._retry_config.write_retries:
            return False
        if not _is_retryable_bulk_write_error(exc):
            return False
        if self._bulk_transport_reconnector is None:
            return False
        self._record_write_retry_attempt(exc)
        old_transport: BulkFrameTransport | None = None
        with self._bulk_transport_replace_lock:
            if self._bulk_transport is failed_transport:
                try:
                    replacement = self._bulk_transport_reconnector()
                except Exception:
                    _close_transports_suppressing((failed_transport,))
                    raise
                if replacement is None:
                    _close_transports_suppressing((failed_transport,))
                    raise RuntimeError("bulk transport reconnector returned None")
                old_transport = self._bulk_transport
                self._bulk_transport = replacement
                self._bulk_transport_write_reconnects += 1
        if old_transport is not None:
            _close_transports_suppressing((old_transport,))
        return True

    def close(self) -> None:
        with self._bulk_transport_replace_lock:
            transport = self._bulk_transport
        transport.close()

    def _record_read_path_evidence(
        self,
        *,
        byte_return: bool,
        direct_buffer: bool,
        direct_tensor: bool,
        used_frame_sink: bool,
        frame_count: int,
        byte_count: int,
        bulk_transport: BulkFrameTransport,
        transfer: TransferAcceptedResponse,
    ) -> None:
        observed_cords = _observed_bulk_read_cords(bulk_transport)
        with self._evidence_lock:
            self._read_operation_count += 1
            if byte_return:
                self._byte_return_read_operations += 1
            if direct_buffer:
                self._direct_buffer_read_operations += 1
            if direct_tensor:
                self._direct_tensor_read_operations += 1
            if direct_buffer or direct_tensor:
                self._direct_destination_read_operations += 1
                self._direct_destination_last_observed_cords = observed_cords
                self._direct_destination_max_observed_cords = max(
                    self._direct_destination_max_observed_cords,
                    observed_cords,
                )
            if used_frame_sink:
                self._frame_sink_read_operations += 1
                self._frame_sink_frames += frame_count
                if direct_buffer or direct_tensor:
                    self._direct_destination_frame_sink_read_operations += 1
                    if observed_cords > 1:
                        self._direct_destination_parallel_frame_sink_read_operations += 1
                        self._direct_destination_parallel_frame_sink_frames += frame_count
            else:
                self._fallback_frame_iter_read_operations += 1
                self._fallback_frame_iter_frames += frame_count
            self._payload_bytes_read += byte_count
            self._last_observed_cords = observed_cords
            self._max_observed_cords = max(self._max_observed_cords, observed_cords)
            self._last_read_accepted_initial_cords = transfer.accepted_initial_cords
            self._last_read_accepted_max_cords = transfer.accepted_max_cords

    def _record_direct_destination_read_failure(
        self,
        *,
        direct_buffer: bool,
        direct_tensor: bool,
        zeroed_bytes: int,
    ) -> None:
        with self._evidence_lock:
            self._failed_read_operations += 1
            if direct_buffer:
                self._failed_direct_buffer_read_operations += 1
            if direct_tensor:
                self._failed_direct_tensor_read_operations += 1
            if direct_buffer or direct_tensor:
                self._failed_direct_destination_read_operations += 1
            self._direct_destination_zeroed_bytes_on_failure += zeroed_bytes

    def _record_byte_return_read_failure(self) -> None:
        with self._evidence_lock:
            self._failed_read_operations += 1
            self._failed_byte_return_read_operations += 1

    def _record_read_retry_attempt(self, exc: BaseException) -> None:
        with self._evidence_lock:
            self._read_retry_attempts += 1
            self._last_read_retry_error = f"{type(exc).__name__}: {exc}"

    def _record_read_retry_success(self) -> None:
        with self._evidence_lock:
            self._read_retry_successes += 1
            self._last_read_retry_error = ""

    def _record_write_retry_attempt(self, exc: BaseException) -> None:
        with self._evidence_lock:
            self._write_retry_attempts += 1
            self._last_write_retry_error = f"{type(exc).__name__}: {exc}"

    def _record_write_retry_success(self) -> None:
        with self._evidence_lock:
            self._write_retry_successes += 1
            self._last_write_retry_error = ""

    def _record_write_path_evidence(
        self,
        *,
        direct_tensor: bool,
        directory_output: bool,
        transfer: TransferAcceptedResponse,
        stats: BulkWriteFrameStats,
        byte_count: int,
        entries_committed: int,
        metadata_after_payload_entries: int,
    ) -> None:
        observed_cords = 0 if stats.max_cord_index < 0 else stats.max_cord_index + 1
        with self._evidence_lock:
            self._write_operation_count += 1
            if direct_tensor:
                self._direct_tensor_write_operations += 1
            if directory_output:
                self._directory_output_write_operations += 1
            self._payload_bytes_written += byte_count
            self._write_frame_count += stats.frame_count
            self._write_ack_count += stats.ack_count
            self._write_ack_frame_verified_count += stats.ack_frame_verified_count
            self._write_ack_metadata_verified_count += stats.ack_metadata_verified_count
            self._write_ack_memory_count += stats.ack_memory_count
            self._write_ack_spill_count += stats.ack_spill_count
            self._metadata_after_payload_entries += metadata_after_payload_entries
            self._write_entries_committed += entries_committed
            self._write_transfers_committed += 1
            self._last_write_accepted_initial_cords = transfer.accepted_initial_cords
            self._last_write_accepted_max_cords = transfer.accepted_max_cords
            self._max_observed_write_cords = max(self._max_observed_write_cords, observed_cords)

    def write_from_tensor(self, operation: DirectWriteOperation) -> SendResult:
        session_id = getattr(self._control_client, "session_id", None)
        if session_id is None:
            raise ValueError("authenticated control session id is required for bulk writes")

        source = readable_source_memoryview(operation.source)
        if operation.nbytes > source.nbytes:
            raise ValueError("nbytes exceeds readable source size")

        remote_root, remote_path = _split_direct_write_path(operation.path)
        transfer = self._control_client.bootstrap_write_transfer(
            remote_root,
            payload_format=TransferPayloadFormat.SAFETENSORS,
            initial_cords=self._read_config.initial_cords,
            max_cords=self._read_config.max_cords,
            provisional_container_size_bytes=self._read_config.provisional_container_size_bytes,
            expected_size_limit_bytes=self._read_config.expected_size_limit_bytes,
        )
        if transfer.direction != TransferDirection.WRITE or not transfer.write_payload_admission_granted:
            raise ValueError("server did not admit write payload transfer")

        entry_sequence = 0
        chunk_hashes: list[bytes] = []
        chunk_sizes: list[int] = []
        offset = 0
        chunk_sequence = 0
        in_flight_limit = _accepted_write_frame_window(transfer)
        with _ConcurrentWriteFrameSender(
            self._bulk_transport,
            in_flight_limit,
            write_frame=self.write_frame,
        ) as frame_sender:
            while offset < operation.nbytes:
                chunk_len = min(self._read_config.write_frame_payload_bytes, operation.nbytes - offset)
                payload_view = source[offset:offset + chunk_len]
                chunk_hashes.append(cxnfs_chunk_hash(payload_view))
                chunk_sizes.append(chunk_len)
                frame = BulkDataFrame(
                    transfer.transfer_id,
                    chunk_sequence % transfer.accepted_max_cords,
                    entry_sequence,
                    chunk_sequence,
                    offset,
                    True,
                    payload_view,
                )
                frame_sender.submit(frame)
                offset += chunk_len
                chunk_sequence += 1
        write_stats = frame_sender.stats()

        file_hash = cxnfs_file_hash(chunk_hashes, chunk_sizes)
        entry_accepted = self._control_client.accept_write_entry(
            transfer.transfer_id,
            entry_sequence,
            TransferEntryAction.CREATE_OR_REPLACE,
            remote_path,
            size_hint_bytes=operation.nbytes,
            payload_format=TransferPayloadFormat.SAFETENSORS,
        )
        _require_write_entry_accepted(entry_accepted, transfer.transfer_id, entry_sequence, remote_path)
        metadata_accepted = self._control_client.accept_write_metadata(
            transfer.transfer_id,
            entry_sequence,
            0,
            True,
            operation.nbytes,
            tuple(chunk_hashes),
        )
        _require_write_metadata_accepted(
            metadata_accepted,
            transfer.transfer_id,
            entry_sequence,
            0,
            len(chunk_hashes),
            True,
        )
        entry_committed = self._control_client.commit_write_entry(
            transfer.transfer_id,
            entry_sequence,
            remote_path,
            file_hash,
            operation.nbytes,
        )
        _require_write_entry_committed(entry_committed, transfer.transfer_id, entry_sequence, remote_path, file_hash, operation.nbytes)
        transfer_hash = cxnfs_transfer_hash((file_hash,), (operation.nbytes,))
        transfer_committed = self._control_client.commit_write_transfer(
            transfer.transfer_id,
            remote_root,
            transfer_hash,
            operation.nbytes,
        )
        _require_write_transfer_committed(transfer_committed, transfer.transfer_id, transfer_hash, operation.nbytes)
        self._record_write_path_evidence(
            direct_tensor=True,
            directory_output=False,
            transfer=transfer,
            stats=write_stats,
            byte_count=operation.nbytes,
            entries_committed=1,
            metadata_after_payload_entries=1 if write_stats.frame_count > 0 else 0,
        )
        return SendResult(file_count=1, tensor_file_count=1, bytes_sent=operation.nbytes)


def _accepted_write_frame_window(transfer: TransferAcceptedResponse) -> int:
    return max(1, transfer.accepted_max_cords)


class _ConcurrentWriteFrameSender:
    def __init__(
        self,
        bulk_transport: BulkFrameTransport,
        in_flight_limit: int,
        *,
        write_frame: Callable[[BulkDataFrame], BulkWriteAck | None] | None = None,
    ) -> None:
        if in_flight_limit <= 0:
            raise ValueError("in_flight_limit must be positive")
        self._bulk_transport = bulk_transport
        self._write_frame = write_frame if write_frame is not None else bulk_transport.write_frame
        self._in_flight_limit = in_flight_limit
        self._executor = ThreadPoolExecutor(max_workers=in_flight_limit)
        self._pending_frames: deque[tuple[BulkDataFrame, Future[BulkWriteAck | None]]] = deque()
        self._aborted = False
        self._closed = False
        self._stats = BulkWriteFrameStats()

    def submit(self, frame: BulkDataFrame) -> None:
        if self._aborted:
            raise OSError("bulk write frame sender is closed")
        future = self._executor.submit(_write_frame_and_validate, self._write_frame, frame)
        self._pending_frames.append((frame, future))
        if len(self._pending_frames) >= self._in_flight_limit:
            self._await_one()

    def drain(self) -> None:
        while self._pending_frames:
            self._await_one()

    def abort(self) -> None:
        if self._aborted:
            return
        self._aborted = True
        _cancel_write_frames(self._pending_frames)
        _close_transports_suppressing((self._bulk_transport,))
        self._executor.shutdown(wait=False, cancel_futures=True)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._aborted:
            self._executor.shutdown(wait=True)

    def stats(self) -> BulkWriteFrameStats:
        return self._stats

    def _await_one(self) -> None:
        frame, future = self._pending_frames.popleft()
        try:
            ack = future.result()
        except BaseException:
            self.abort()
            raise
        self._record_result(frame, ack)

    def _record_result(self, frame: BulkDataFrame, ack: BulkWriteAck | None) -> None:
        ack_count = 0 if ack is None else 1
        ack_frame_verified_count = ack_count
        ack_metadata_verified_count = 1 if ack is not None and ack.metadata_verified else 0
        ack_memory_count = 1 if ack is not None and ack.storage == "MEMORY" else 0
        ack_spill_count = 1 if ack is not None and ack.storage == "SPILL" else 0
        self._stats = self._stats.merge(BulkWriteFrameStats(
            frame_count=1,
            payload_bytes=len(frame.payload),
            ack_count=ack_count,
            ack_frame_verified_count=ack_frame_verified_count,
            ack_metadata_verified_count=ack_metadata_verified_count,
            ack_memory_count=ack_memory_count,
            ack_spill_count=ack_spill_count,
            max_cord_index=frame.cord_index,
        ))

    def __enter__(self) -> _ConcurrentWriteFrameSender:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        if exc_type is not None:
            self.abort()
            self.close()
            return None
        try:
            self.drain()
        except BaseException:
            self.abort()
            raise
        finally:
            self.close()
        return None


def _cancel_write_frames(pending_frames: deque[tuple[BulkDataFrame, Future[BulkWriteAck | None]]]) -> None:
    for _, future in pending_frames:
        future.cancel()
    pending_frames.clear()


def _write_frame_and_validate(
    write_frame: Callable[[BulkDataFrame], BulkWriteAck | None],
    frame: BulkDataFrame,
) -> BulkWriteAck | None:
    ack = write_frame(frame)
    if ack is not None:
        _require_write_ack_matches_frame(frame, ack)
    return ack


def _close_transports(transports: Iterable[object]) -> None:
    seen: set[int] = set()
    failure: BaseException | None = None
    for transport in transports:
        identity = id(transport)
        if identity in seen:
            continue
        seen.add(identity)
        close = getattr(transport, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except BaseException as exc:
            if failure is None:
                failure = exc
    if failure is not None:
        raise failure


def _close_transports_suppressing(transports: Iterable[object]) -> None:
    try:
        _close_transports(transports)
    except BaseException:
        pass


def _is_retryable_bulk_read_error(exc: BaseException) -> bool:
    return isinstance(exc, (EOFError, OSError, TimeoutError, ssl.SSLError))


def _is_retryable_bulk_write_error(exc: BaseException) -> bool:
    return isinstance(exc, (EOFError, OSError, TimeoutError, ssl.SSLError))


def _is_retryable_bulk_connect_error(exc: BaseException) -> bool:
    return isinstance(exc, (ConnectionError, EOFError, OSError, TimeoutError, ssl.SSLError))


def encode_bulk_frame(frame: BulkDataFrame) -> bytes:
    writer = _BinaryWriter()
    writer.write(BULK_MAGIC)
    writer.write_int(BULK_VERSION)
    writer.write_uuid(frame.transfer_id)
    writer.write_int(frame.cord_index)
    writer.write_int(frame.entry_sequence)
    writer.write_long(frame.chunk_sequence)
    writer.write_long(frame.entry_offset)
    writer.write_bool(frame.final_frame_for_chunk)
    writer.write_bytes(frame.payload)
    return writer.bytes()


def decode_bulk_frame(frame_bytes: bytes) -> BulkDataFrame:
    reader = _BinaryReader(frame_bytes)
    reader.require(BULK_MAGIC)
    reader.require_int(BULK_VERSION)
    transfer_id = reader.read_uuid()
    cord_index = reader.read_int()
    entry_sequence = reader.read_int()
    chunk_sequence = reader.read_long()
    entry_offset = reader.read_long()
    final_frame = reader.read_bool()
    marker = reader.read_int()
    if marker == -2:  # LZ4 block compression
        uncompressed_len = reader.read_int()
        compressed = reader.read_exact(reader.remaining())
        try:
            import lz4.block
            payload = lz4.block.decompress(compressed, uncompressed_size=uncompressed_len)
        except Exception:
            payload = compressed
    elif marker == -3:  # ZSTD block compression
        uncompressed_len = reader.read_int()
        compressed = reader.read_exact(reader.remaining())
        try:
            import zstandard
            dctx = zstandard.ZstdDecompressor()
            payload = dctx.decompress(compressed, max_output_size=uncompressed_len)
        except Exception:
            payload = compressed
    elif marker >= 0:
        if marker > MAX_PAYLOAD_BYTES:
            raise ValueError(f"bulk data frame payload exceeds maximum size: {marker}")
        payload = reader.read_exact(marker)
    else:
        raise ValueError(f"unsupported bulk frame payload marker: {marker}")
    reader.require_eof()
    return BulkDataFrame(
        transfer_id,
        cord_index,
        entry_sequence,
        chunk_sequence,
        entry_offset,
        final_frame,
        payload,
    )


def encode_bulk_read_request(request: BulkReadRequest) -> bytes:
    writer = _BinaryWriter()
    writer.write(READ_REQUEST_MAGIC)
    writer.write_int(READ_REQUEST_VERSION)
    writer.write_uuid(request.session_id)
    writer.write_uuid(request.transfer_id)
    writer.write_utf8(request.path)
    writer.write_long(request.offset)
    writer.write_long(request.length)
    writer.write_int(request.active_cords)
    return writer.bytes()


def decode_bulk_read_request(request_bytes: bytes) -> BulkReadRequest:
    reader = _BinaryReader(request_bytes)
    reader.require(READ_REQUEST_MAGIC)
    reader.require_int(READ_REQUEST_VERSION)
    request = BulkReadRequest(
        reader.read_uuid(),
        reader.read_uuid(),
        reader.read_utf8(64 * 1024),
        reader.read_long(),
        reader.read_long(),
        reader.read_int(),
    )
    reader.require_eof()
    return request


def encode_bulk_write_ack(ack: BulkWriteAck) -> bytes:
    writer = _BinaryWriter()
    writer.write(WRITE_ACK_MAGIC)
    writer.write_int(WRITE_ACK_VERSION)
    writer.write_uuid(ack.transfer_id)
    writer.write_int(ack.entry_sequence)
    writer.write_long(ack.chunk_sequence)
    writer.write_int(ack.payload_length)
    writer.write_bool(ack.metadata_verified)
    writer.write_utf8(ack.storage)
    return writer.bytes()


def decode_bulk_write_ack(ack_bytes: bytes) -> BulkWriteAck:
    if not ack_bytes or len(ack_bytes) > MAX_ENCODED_BULK_WRITE_ACK_BYTES:
        raise ValueError(f"invalid encoded bulk write ack length: {len(ack_bytes)}")
    reader = _BinaryReader(ack_bytes)
    reader.require(WRITE_ACK_MAGIC)
    reader.require_int(WRITE_ACK_VERSION)
    ack = BulkWriteAck(
        reader.read_uuid(),
        reader.read_int(),
        reader.read_long(),
        reader.read_int(),
        reader.read_bool(),
        reader.read_utf8(16),
    )
    reader.require_eof()
    return ack


def write_encoded_bulk_frame(sock: socket.socket, frame_bytes: bytes) -> None:
    if not frame_bytes or len(frame_bytes) > MAX_ENCODED_BULK_FRAME_BYTES:
        raise ValueError(f"invalid encoded bulk frame length: {len(frame_bytes)}")
    sock.sendall(struct.pack(">i", len(frame_bytes)) + frame_bytes)


def read_encoded_bulk_frame(sock: socket.socket) -> bytes:
    length = struct.unpack(">i", _read_socket_exact(sock, 4))[0]
    if length <= 0 or length > MAX_ENCODED_BULK_FRAME_BYTES:
        raise ValueError(f"invalid encoded bulk frame length: {length}")
    return _read_socket_exact(sock, length)


def write_encoded_bulk_write_ack(sock: socket.socket, ack_bytes: bytes) -> None:
    if not ack_bytes or len(ack_bytes) > MAX_ENCODED_BULK_WRITE_ACK_BYTES:
        raise ValueError(f"invalid encoded bulk write ack length: {len(ack_bytes)}")
    sock.sendall(struct.pack(">i", len(ack_bytes)) + ack_bytes)


def read_encoded_bulk_write_ack(sock: socket.socket) -> bytes:
    length = struct.unpack(">i", _read_socket_exact(sock, 4))[0]
    if length <= 0 or length > MAX_ENCODED_BULK_WRITE_ACK_BYTES:
        raise ValueError(f"invalid encoded bulk write ack length: {length}")
    return _read_socket_exact(sock, length)


def write_encoded_bulk_read_request(sock: socket.socket, request_bytes: bytes) -> None:
    if not request_bytes or len(request_bytes) > MAX_BULK_READ_REQUEST_BYTES:
        raise ValueError(f"invalid encoded bulk read request length: {len(request_bytes)}")
    sock.sendall(struct.pack(">i", len(request_bytes)) + request_bytes)


def read_encoded_bulk_read_request(sock: socket.socket) -> bytes:
    length = struct.unpack(">i", _read_socket_exact(sock, 4))[0]
    if length <= 0 or length > MAX_BULK_READ_REQUEST_BYTES:
        raise ValueError(f"invalid encoded bulk read request length: {length}")
    return _read_socket_exact(sock, length)


def assemble_read_plan_payload(
    plan: ReadPlan,
    frames: list[BulkDataFrame] | tuple[BulkDataFrame, ...],
    *,
    transfer_id: UUID | None = None,
    max_cords: int | None = None,
) -> bytes:
    expected = {chunk.chunk_index: chunk for chunk in plan.chunks}
    by_sequence: dict[int, BulkDataFrame] = {}
    for frame in frames:
        if transfer_id is not None and frame.transfer_id != transfer_id:
            raise ValueError(f"bulk frame belongs to a different transfer: {frame.transfer_id}")
        if max_cords is not None and frame.cord_index >= max_cords:
            raise ValueError(f"bulk frame cord is outside accepted cord range: {frame.cord_index}")
        if frame.chunk_sequence not in expected:
            raise ValueError(f"unexpected bulk frame chunk sequence: {frame.chunk_sequence}")
        if frame.chunk_sequence in by_sequence:
            raise ValueError(f"duplicate bulk frame chunk sequence: {frame.chunk_sequence}")
        by_sequence[frame.chunk_sequence] = frame

    assembled = bytearray()
    for chunk in plan.chunks:
        frame = by_sequence.get(chunk.chunk_index)
        if frame is None:
            raise ValueError(f"missing bulk frame chunk sequence: {chunk.chunk_index}")
        _validate_full_chunk_frame(chunk, frame)
        assembled.extend(frame.payload[chunk.offset_in_chunk:chunk.offset_in_chunk + chunk.bytes_to_read])
    return bytes(assembled)


def cxnfs_chunk_hash(payload: bytes | bytearray | memoryview) -> bytes:
    digest = hashlib.sha256()
    digest.update(b"cxnfs.chunk.v1")
    digest.update(b"\x00")
    digest.update(bytes(payload))
    return digest.digest()


def cxnfs_file_hash(
    chunk_hashes: list[bytes] | tuple[bytes, ...],
    chunk_sizes: list[int] | tuple[int, ...],
) -> bytes:
    if len(chunk_hashes) != len(chunk_sizes):
        raise ValueError("chunk hash and size counts must match")
    digest = hashlib.sha256()
    digest.update(b"cxnfs.file.v1")
    digest.update(b"\x00")
    for chunk_hash, chunk_size in zip(chunk_hashes, chunk_sizes):
        if len(chunk_hash) != HASH_BYTES:
            raise ValueError("chunk hash must be 32 bytes")
        if chunk_size < 0 or chunk_size > 2_147_483_647:
            raise ValueError("chunk size must fit in signed 32-bit integer")
        digest.update(chunk_hash)
        digest.update(struct.pack(">i", chunk_size))
    return digest.digest()


def cxnfs_transfer_hash(
    entry_hashes: list[bytes] | tuple[bytes, ...],
    entry_sizes: list[int] | tuple[int, ...],
) -> bytes:
    if len(entry_hashes) != len(entry_sizes):
        raise ValueError("entry hash and size counts must match")
    digest = hashlib.sha256()
    digest.update(b"cxnfs.transfer.v1")
    digest.update(b"\x00")
    for entry_hash, entry_size in zip(entry_hashes, entry_sizes):
        if len(entry_hash) != HASH_BYTES:
            raise ValueError("entry hash must be 32 bytes")
        if entry_size < 0 or entry_size > 9_223_372_036_854_775_807:
            raise ValueError("entry size must fit in signed 64-bit integer")
        digest.update(entry_hash)
        digest.update(struct.pack(">q", entry_size))
    return digest.digest()


def transfer_root_path(root: Path, remote_root: str | None = None) -> str:
    return _remote_root_for_output(root) if remote_root is None else _remote_root_for_output(Path(remote_root))


def _remote_root_for_output(root: Path) -> str:
    return normalize_namespace_root(root.as_posix(), label="remote_root")


def _join_remote_path(*, transfer_path_root: str, relative_path: str) -> str:
    return join_namespace_path(transfer_path_root, relative_path, label="send file path")


def _split_direct_write_path(path: str) -> tuple[str, str]:
    remote_path = _normalize_remote_file_path(path)
    parts = remote_path.split("/")[1:]
    remote_root = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
    return remote_root, remote_path


def _normalize_remote_file_path(path: str) -> str:
    return normalize_namespace_file_path(path, label="remote file path")


def _entry_from_path(root: Path, relative_path: str, tensor_file: bool):
    from .dataset import SafetensorsFileEntry

    source = root / relative_path
    return SafetensorsFileEntry(relative_path, source.stat().st_size, _sha256_hex(source), tensor_file)


def _sha256_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_full_chunk_frame(chunk: PlannedChunkRead, frame: BulkDataFrame) -> None:
    if not frame.final_frame_for_chunk:
        raise ValueError(f"bulk frame is not final for chunk: {frame.chunk_sequence}")
    if frame.entry_offset != chunk.file_offset:
        raise ValueError(f"bulk frame entry offset does not match read plan: {frame.chunk_sequence}")
    if len(frame.payload) != chunk.chunk_size:
        raise ValueError(f"bulk frame must carry the full touched chunk: {frame.chunk_sequence}")
    actual_hash = cxnfs_chunk_hash(frame.payload)
    if not hmac.compare_digest(actual_hash, chunk.chunk_hash):
        raise ValueError(f"bulk frame chunk hash verification failed: {frame.chunk_sequence}")


def _require_write_ack_matches_frame(frame: BulkDataFrame, ack: BulkWriteAck) -> None:
    if ack.transfer_id != frame.transfer_id:
        raise ValueError("bulk write ack transfer does not match sent frame")
    if ack.entry_sequence != frame.entry_sequence:
        raise ValueError("bulk write ack entry sequence does not match sent frame")
    if ack.chunk_sequence != frame.chunk_sequence:
        raise ValueError("bulk write ack chunk sequence does not match sent frame")
    if ack.payload_length != len(frame.payload):
        raise ValueError("bulk write ack length does not match sent frame")


def _require_read_response_matches_request(path: str, offset: int, length: int, response) -> None:
    response_path = normalize_namespace_path(response.path, label="path")
    requested_path = normalize_namespace_path(path, label="path")
    if response_path != requested_path:
        raise ValueError("control read response path does not match requested read")
    if (
        response.plan.requested_range.offset != offset
        or response.plan.requested_range.length != length
    ):
        raise ValueError("control read plan range does not match requested read")


def _require_write_entry_accepted(response, transfer_id: UUID, entry_sequence: int, path: str) -> None:
    if getattr(response, "transfer_id", None) != transfer_id:
        raise ValueError("write entry acknowledgement transfer does not match request")
    if getattr(response, "entry_sequence", None) != entry_sequence:
        raise ValueError("write entry acknowledgement sequence does not match request")
    if getattr(response, "path", None) != path:
        raise ValueError("write entry acknowledgement path does not match request")


def _require_write_metadata_accepted(
    response,
    transfer_id: UUID,
    entry_sequence: int,
    metadata_sequence: int,
    chunk_hash_count: int,
    final_metadata: bool,
) -> None:
    if getattr(response, "transfer_id", None) != transfer_id:
        raise ValueError("write metadata acknowledgement transfer does not match request")
    if getattr(response, "entry_sequence", None) != entry_sequence:
        raise ValueError("write metadata acknowledgement entry sequence does not match request")
    if getattr(response, "metadata_sequence", None) != metadata_sequence:
        raise ValueError("write metadata acknowledgement sequence does not match request")
    if getattr(response, "chunk_hashes_accepted", None) != chunk_hash_count:
        raise ValueError("write metadata acknowledgement chunk count does not match request")
    if getattr(response, "staged_payloads_verified", None) != chunk_hash_count:
        raise ValueError("write metadata acknowledgement verified count does not match request")
    if getattr(response, "final_metadata", None) != final_metadata:
        raise ValueError("write metadata acknowledgement final flag does not match request")


def _require_write_entry_committed(
    response,
    transfer_id: UUID,
    entry_sequence: int,
    path: str,
    content_hash: bytes,
    bytes_written: int,
) -> None:
    if getattr(response, "transfer_id", None) != transfer_id:
        raise ValueError("write entry commit acknowledgement transfer does not match request")
    if getattr(response, "entry_sequence", None) != entry_sequence:
        raise ValueError("write entry commit acknowledgement sequence does not match request")
    if getattr(response, "path", None) != path:
        raise ValueError("write entry commit acknowledgement path does not match request")
    actual_hash = getattr(response, "content_hash", None)
    if not isinstance(actual_hash, (bytes, bytearray)) or not hmac.compare_digest(bytes(actual_hash), content_hash):
        raise ValueError("write entry commit acknowledgement hash does not match request")
    if getattr(response, "bytes_written", None) != bytes_written:
        raise ValueError("write entry commit acknowledgement byte count does not match request")


def _require_write_transfer_committed(
    response,
    transfer_id: UUID,
    content_hash: bytes,
    bytes_written: int,
) -> None:
    if getattr(response, "transfer_id", None) != transfer_id:
        raise ValueError("write transfer commit acknowledgement transfer does not match request")
    actual_hash = getattr(response, "content_hash", None)
    if not isinstance(actual_hash, (bytes, bytearray)) or not hmac.compare_digest(bytes(actual_hash), content_hash):
        raise ValueError("write transfer commit acknowledgement hash does not match request")
    if getattr(response, "bytes_written", None) != bytes_written:
        raise ValueError("write transfer commit acknowledgement byte count does not match request")


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

    def remaining(self) -> int:
        return len(self._data) - self._offset

    def require_eof(self) -> None:
        if self._offset != len(self._data):
            raise ValueError("trailing bytes after bulk data frame")


def _read_socket_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise EOFError("bulk socket closed while reading frame")
        chunks.extend(chunk)
    return bytes(chunks)
