"""Authentication and credential management package."""

from earthaccess.auth.auth import (
    Auth,
    SessionWithHeaderRedirection,
    _create_earthdata_session,
    netrc_path,
)
from earthaccess.auth.credentials import (
    AuthContext,
    CredentialManager,
    HTTPHeaders,
    S3Credentials,
)
from earthaccess.auth.system import PROD, UAT, System

__all__ = [
    # auth.py
    "Auth",
    "netrc_path",
    "_create_earthdata_session",
    # credentials.py
    "S3Credentials",
    "HTTPHeaders",
    "AuthContext",
    "CredentialManager",
    # system.py
    "PROD",
    "UAT",
    "System",
    # Deprecated - kept for backward compatibility
    "SessionWithHeaderRedirection",
]
