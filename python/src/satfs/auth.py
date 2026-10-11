from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import getpass
import hashlib
import hmac
import os
from typing import Callable
from uuid import UUID, uuid4


PROOF_ALGORITHM = "PBKDF2WithHmacSHA256"
DEFAULT_ITERATIONS = 65_536
NONCE_BYTES = 32
PROOF_BYTES = 32


class AuthMechanism(str, Enum):
    USERNAME_PASSWORD = "username_password"
    BEARER_TOKEN = "bearer_token"


@dataclass(frozen=True)
class AuthStart:
    mechanism: AuthMechanism
    principal: str

    def __post_init__(self) -> None:
        if self.mechanism is None:
            raise ValueError("mechanism is required")
        if not self.principal:
            raise ValueError("principal is required")


@dataclass(frozen=True)
class AuthChallenge:
    challenge_id: UUID
    mechanism: AuthMechanism
    principal: str
    nonce: bytes
    proof_algorithm: str = PROOF_ALGORITHM
    iterations: int = DEFAULT_ITERATIONS

    def __post_init__(self) -> None:
        if self.challenge_id is None:
            raise ValueError("challenge_id is required")
        if self.mechanism is None:
            raise ValueError("mechanism is required")
        if not self.principal:
            raise ValueError("principal is required")
        if not self.nonce:
            raise ValueError("nonce is required")
        if not self.proof_algorithm:
            raise ValueError("proof_algorithm is required")
        if self.iterations <= 0:
            raise ValueError("iterations must be positive")


@dataclass(frozen=True)
class AuthProof:
    challenge_id: UUID
    principal: str
    proof: bytes

    def __post_init__(self) -> None:
        if self.challenge_id is None:
            raise ValueError("challenge_id is required")
        if not self.principal:
            raise ValueError("principal is required")
        if not self.proof:
            raise ValueError("proof is required")

    def __repr__(self) -> str:
        return (
            f"AuthProof(challenge_id={self.challenge_id!r}, "
            f"principal={self.principal!r}, proof=<hidden:{len(self.proof)} bytes>)"
        )


class ClientCredentials:
    def __init__(self, username: str, password: str | bytes | bytearray | memoryview) -> None:
        if not username:
            raise ValueError("username is required")
        password_bytes = _password_bytes(password)
        if not password_bytes:
            raise ValueError("password is required")
        self.username = username
        self._password = bytearray(password_bytes)
        self._closed = False

    def password_bytes(self) -> bytes:
        if self._closed:
            raise ValueError("credentials are closed")
        return bytes(self._password)

    def close(self) -> None:
        for index in range(len(self._password)):
            self._password[index] = 0
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> ClientCredentials:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"ClientCredentials(username={self.username!r}, password=<hidden>, state={state})"


class ClientAuthenticator:
    def start(self, credentials: ClientCredentials) -> AuthStart:
        return AuthStart(AuthMechanism.USERNAME_PASSWORD, credentials.username)

    def answer(self, challenge: AuthChallenge, credentials: ClientCredentials) -> AuthProof:
        if challenge.mechanism != AuthMechanism.USERNAME_PASSWORD:
            raise ValueError(f"unsupported auth challenge mechanism: {challenge.mechanism}")
        if challenge.proof_algorithm != PROOF_ALGORITHM:
            raise ValueError(f"unsupported auth proof algorithm: {challenge.proof_algorithm}")
        if challenge.principal != credentials.username:
            raise ValueError("auth challenge principal does not match credentials")

        proof = derive_password_proof(
            credentials.password_bytes(),
            challenge.nonce,
            challenge.iterations,
        )
        return AuthProof(challenge.challenge_id, credentials.username, proof)


def new_auth_challenge(principal: str, iterations: int = DEFAULT_ITERATIONS) -> AuthChallenge:
    return AuthChallenge(
        uuid4(),
        AuthMechanism.USERNAME_PASSWORD,
        principal,
        os.urandom(NONCE_BYTES),
        PROOF_ALGORITHM,
        iterations,
    )


def prompt_client_credentials(
    input_fn: Callable[[str], str] | None = None,
    password_fn: Callable[[str], str] | None = None,
) -> ClientCredentials:
    read_username = input if input_fn is None else input_fn
    read_password = getpass.getpass if password_fn is None else password_fn
    username = read_username("CXNFS username: ")
    password = read_password("CXNFS password: ")
    return ClientCredentials(username, password)


def derive_password_proof(
    password: str | bytes | bytearray | memoryview,
    nonce: bytes | bytearray | memoryview,
    iterations: int = DEFAULT_ITERATIONS,
) -> bytes:
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    password_bytes = _password_bytes(password)
    if not password_bytes:
        raise ValueError("password is required")
    nonce_bytes = bytes(nonce)
    if not nonce_bytes:
        raise ValueError("nonce is required")
    return hashlib.pbkdf2_hmac("sha256", password_bytes, nonce_bytes, iterations, PROOF_BYTES)


def verify_password_proof(
    expected_password: str | bytes | bytearray | memoryview,
    nonce: bytes | bytearray | memoryview,
    iterations: int,
    proof: bytes | bytearray | memoryview,
) -> bool:
    expected = derive_password_proof(expected_password, nonce, iterations)
    return hmac.compare_digest(expected, bytes(proof))


def _password_bytes(password: str | bytes | bytearray | memoryview) -> bytes:
    if isinstance(password, str):
        return password.encode("utf-8")
    return bytes(password)
