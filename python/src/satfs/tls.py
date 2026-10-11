from __future__ import annotations

import ssl


CXNFS_TLS_CIPHER_SUITE = "TLS_AES_128_GCM_SHA256"


def create_client_tls_context(cafile: str | None = None) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=cafile)
    return configure_client_tls_context(context)


def configure_client_tls_context(context: ssl.SSLContext) -> ssl.SSLContext:
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    set_ciphersuites = getattr(context, "set_ciphersuites", None)
    if not callable(set_ciphersuites):
        raise RuntimeError(
            "Python SSLContext cannot restrict TLS 1.3 cipher suites; "
            "CXNFS requires TLS_AES_128_GCM_SHA256"
        )
    set_ciphersuites(CXNFS_TLS_CIPHER_SUITE)
    return context
