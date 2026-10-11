import ssl
import unittest
from unittest.mock import patch

import cxnfs.bulk as bulk_module
import cxnfs.control as control_module
from cxnfs import (
    CXNFS_TLS_CIPHER_SUITE,
    configure_client_tls_context,
    create_client_tls_context,
)


class CxnfsTlsTest(unittest.TestCase):
    def test_configures_tls13_single_cipher_policy(self):
        context = FakeTlsContext()

        configured = configure_client_tls_context(context)

        self.assertIs(context, configured)
        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.minimum_version)
        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.maximum_version)
        self.assertEqual([CXNFS_TLS_CIPHER_SUITE], context.cipher_suites)

    def test_creates_default_client_context_with_cafile_and_policy(self):
        context = FakeTlsContext()

        with patch("cxnfs.tls.ssl.create_default_context", return_value=context) as create_default_context:
            created = create_client_tls_context("/trust/server-ca.pem")

        self.assertIs(context, created)
        create_default_context.assert_called_once_with(cafile="/trust/server-ca.pem")
        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.minimum_version)
        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.maximum_version)
        self.assertEqual([CXNFS_TLS_CIPHER_SUITE], context.cipher_suites)

    def test_rejects_runtime_that_cannot_restrict_tls13_cipher_suites(self):
        context = FakeTlsContextWithoutCipherSuites()

        with self.assertRaisesRegex(RuntimeError, "cannot restrict TLS 1.3 cipher suites"):
            configure_client_tls_context(context)

        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.minimum_version)
        self.assertEqual(ssl.TLSVersion.TLSv1_3, context.maximum_version)

    def test_control_socket_default_connect_uses_cxnfs_tls_policy(self):
        raw_socket = object()
        context = FakeTlsContext()

        with patch.object(control_module.socket, "create_connection", return_value=raw_socket) as create_connection:
            with patch.object(control_module, "create_client_tls_context", return_value=context) as tls_policy:
                transport = control_module.SocketControlTransport.connect(
                    "cxnfs.example.internal",
                    4509,
                    timeout=7.0,
                )

        create_connection.assert_called_once_with(("cxnfs.example.internal", 4509), timeout=7.0)
        tls_policy.assert_called_once_with()
        self.assertEqual((raw_socket, "cxnfs.example.internal"), context.wrapped[0])
        self.assertEqual(context.tls_socket, transport._socket)

    def test_bulk_socket_default_connect_uses_cxnfs_tls_policy(self):
        raw_socket = object()
        context = FakeTlsContext()

        with patch.object(bulk_module.socket, "create_connection", return_value=raw_socket) as create_connection:
            with patch.object(bulk_module, "create_client_tls_context", return_value=context) as tls_policy:
                transport = bulk_module.LengthPrefixedBulkFrameTransport.connect(
                    "cxnfs.example.internal",
                    4510,
                    timeout=7.0,
                )

        create_connection.assert_called_once_with(("cxnfs.example.internal", 4510), timeout=7.0)
        tls_policy.assert_called_once_with()
        self.assertEqual((raw_socket, "cxnfs.example.internal"), context.wrapped[0])
        self.assertEqual(context.tls_socket, transport._socket)

    def test_control_socket_closes_raw_socket_when_tls_wrap_fails(self):
        raw_socket = FakeRawSocket()

        with patch.object(control_module.socket, "create_connection", return_value=raw_socket):
            with self.assertRaisesRegex(RuntimeError, "handshake failed"):
                control_module.SocketControlTransport.connect(
                    "cxnfs.example.internal",
                    4509,
                    tls_context=FailingTlsContext(),
                )

        self.assertTrue(raw_socket.closed)

    def test_bulk_socket_closes_raw_socket_when_tls_wrap_fails(self):
        raw_socket = FakeRawSocket()

        with patch.object(bulk_module.socket, "create_connection", return_value=raw_socket):
            with self.assertRaisesRegex(RuntimeError, "handshake failed"):
                bulk_module.LengthPrefixedBulkFrameTransport.connect(
                    "cxnfs.example.internal",
                    4510,
                    tls_context=FailingTlsContext(),
                )

        self.assertTrue(raw_socket.closed)


class FakeTlsContext:
    def __init__(self):
        self.minimum_version = None
        self.maximum_version = None
        self.cipher_suites = []
        self.wrapped = []
        self.tls_socket = object()

    def set_ciphersuites(self, cipher_suites):
        self.cipher_suites.append(cipher_suites)

    def wrap_socket(self, sock, server_hostname=None):
        self.wrapped.append((sock, server_hostname))
        return self.tls_socket


class FakeTlsContextWithoutCipherSuites:
    def __init__(self):
        self.minimum_version = None
        self.maximum_version = None


class FailingTlsContext:
    def wrap_socket(self, sock, server_hostname=None):
        raise RuntimeError("handshake failed")


class FakeRawSocket:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


if __name__ == "__main__":
    unittest.main()
