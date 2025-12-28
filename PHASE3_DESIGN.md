# Phase 3: Credential Management and Store Refactoring - Design Document

## Overview

Phase 3 refactors credential handling and store creation using SOLID principles (Single Responsibility, Open/Closed, Liskov Substitution, Interface Segregation, Dependency Inversion).

## Architecture Goals

1. **Type Safety**: Frozen dataclasses for immutable credential objects
2. **Dependency Injection**: Factory pattern for creating filesystems and stores
3. **Testability**: All components mockable via interfaces
4. **Thread Safety**: Support both threaded and distributed execution
5. **Serialization**: Credentials can be pickled/serialized for workers
6. **No Auth Coupling**: Store creation is independent of Auth state

## Component Design

### 1. S3Credentials (store/credentials.py)

**SOLID Principles:**
- Single Responsibility: Encapsulates only S3-specific credential data
- Immutable: Frozen dataclass prevents accidental mutation

```python
@dataclass(frozen=True)
class S3Credentials:
    """Immutable S3 credentials with expiration checking."""

    access_key: str
    secret_key: str
    session_token: Optional[str] = None
    expiration_time: Optional[datetime] = None
    region: str = "us-west-2"

    @classmethod
    def from_auth(cls, auth: Auth) -> "S3Credentials":
        """Extract S3 credentials from Auth object."""

    def is_expired(self) -> bool:
        """Check if credentials have expired."""

    def to_dict(self) -> Dict[str, str]:
        """Convert to dict for s3fs initialization."""
```

**Why Frozen Dataclass:**
- Prevents accidental mutation in concurrent scenarios
- Thread-safe by default
- Can be used as dict keys
- Serializable via pickle

### 2. HTTPHeaders (store/credentials.py)

**Purpose**: Capture HTTP headers/cookies needed for HTTPS fallback when S3 direct access isn't available

```python
@dataclass(frozen=True)
class HTTPHeaders:
    """HTTP headers and cookies for HTTPS fallback access."""

    headers: Dict[str, str]  # Authorization, User-Agent, etc
    cookies: Dict[str, str]  # Session cookies if needed

    @classmethod
    def from_auth(cls, auth: Auth) -> "HTTPHeaders":
        """Extract headers/cookies from authenticated session."""
```

### 3. AuthContext (store/credentials.py)

**SOLID Principles:**
- Single Responsibility: Holds only credential data, not logic
- Dependency Inversion: Depends on credential abstractions, not concrete Auth

```python
@dataclass(frozen=True)
class AuthContext:
    """
    Serializable authentication context for distributed execution.

    Used to reconstruct credentials in worker processes without
    re-authenticating. Captures all necessary credential information.
    """

    s3_credentials: Optional[S3Credentials] = None
    http_headers: Optional[HTTPHeaders] = None
    urs_token: Optional[str] = None
    provider_credentials: Dict[str, Dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_auth(cls, auth: Auth) -> "AuthContext":
        """Extract context from authenticated Auth object."""

    def to_auth(self, system: System = PROD) -> Auth:
        """Reconstruct Auth object from context (for workers)."""

    def is_valid(self) -> bool:
        """Check if all credentials are non-expired."""
```

### 4. FileSystemFactory (store/filesystems.py)

**SOLID Principles:**
- Open/Closed: Open for extension (new filesystem types), closed for modification
- Dependency Inversion: Depends on factory interface, not concrete implementations

```python
class FileSystemFactory(ABC):
    """
    Abstract factory for creating configured filesystem instances.

    Enables dependency injection for testability and supports
    multiple backend configurations (S3, HTTPS, etc).
    """

    @abstractmethod
    def create_s3_fs(self, credentials: S3Credentials) -> s3fs.S3FileSystem:
        """Create S3 filesystem with given credentials."""

    @abstractmethod
    def create_https_fs(self, headers: HTTPHeaders) -> AbstractFileSystem:
        """Create HTTPS filesystem with headers/cookies."""

    @abstractmethod
    def create_default_fs(self) -> AbstractFileSystem:
        """Create default local filesystem."""


class DefaultFileSystemFactory(FileSystemFactory):
    """Standard implementation using fsspec."""

    def create_s3_fs(self, credentials: S3Credentials) -> s3fs.S3FileSystem:
        """Create S3 filesystem with standard s3fs configuration."""
        return s3fs.S3FileSystem(
            key=credentials.access_key,
            secret=credentials.secret_key,
            token=credentials.session_token,
            region_name=credentials.region,
            anon=False,
        )

    def create_https_fs(self, headers: HTTPHeaders) -> AbstractFileSystem:
        """Create HTTPS filesystem with HTTP headers."""
        return fsspec.filesystem('https', headers=headers.headers)
```

### 5. CredentialManager (store/credentials.py)

**SOLID Principles:**
- Single Responsibility: Only manages credential caching and retrieval
- Dependency Inversion: Accepts credentials objects, doesn't create them

```python
class CredentialManager:
    """
    Cache and retrieve credentials by provider.

    Centralizes credential management for multiple data providers,
    enabling efficient credential reuse and replacement.
    """

    def __init__(self):
        self._credentials: Dict[str, Dict[str, str]] = {}
        self._s3_credentials: Optional[S3Credentials] = None
        self._lock = threading.RLock()

    def store_s3_credentials(self, creds: S3Credentials) -> None:
        """Store S3 credentials thread-safely."""

    def get_s3_credentials(self) -> Optional[S3Credentials]:
        """Retrieve S3 credentials (or None if not set)."""

    def store_provider_credentials(self, provider: str, creds: Dict[str, str]) -> None:
        """Store credentials for specific provider (e.g., PODAAC, NSIDC)."""

    def get_provider_credentials(self, provider: str) -> Optional[Dict[str, str]]:
        """Retrieve credentials for specific provider."""
```

### 6. Store Refactoring with Dependency Injection

**Current Issue**: Store creates filesystems internally, tightly coupled to s3fs

**Refactored Design**:

```python
class Store:
    """Data store with dependency-injected filesystem factory."""

    def __init__(
        self,
        auth: Auth,
        fs_factory: Optional[FileSystemFactory] = None,
        cred_manager: Optional[CredentialManager] = None,
    ):
        self.auth = auth
        self.fs_factory = fs_factory or DefaultFileSystemFactory()
        self.cred_manager = cred_manager or CredentialManager()

    def _get_s3_fs(self) -> s3fs.S3FileSystem:
        """Get or create S3 filesystem using factory."""
        creds = self.cred_manager.get_s3_credentials()
        if not creds:
            creds = S3Credentials.from_auth(self.auth)
            self.cred_manager.store_s3_credentials(creds)
        return self.fs_factory.create_s3_fs(creds)
```

**Benefits**:
- Can swap implementations for testing (MockFileSystemFactory)
- Credentials managed centrally
- Auth is optional (credentials provided directly)
- Testable without real AWS calls

### 7. WorkerContext and StreamingIterator

**Purpose**: Support parallel execution by serializing credentials to workers

```python
@dataclass
class WorkerContext:
    """Context for worker process/thread execution."""

    auth_context: AuthContext
    granules: List[DataGranule]
    operation: str  # 'download', 'open', etc

    def get_auth(self) -> Auth:
        """Reconstruct Auth for worker."""
        return self.auth_context.to_auth()


class StreamingIterator:
    """
    Iterator for granule operations in parallel execution.

    Handles credential setup and cleanup in worker contexts.
    """

    def __init__(
        self,
        granules: Iterable[DataGranule],
        operation: Callable,
        auth: Optional[Auth] = None,
    ):
        self.granules = granules
        self.operation = operation
        self.auth_context = AuthContext.from_auth(auth) if auth else None

    def __iter__(self):
        for granule in self.granules:
            auth = self.auth_context.to_auth() if self.auth_context else None
            yield self.operation(granule, auth=auth)
```

## Testing Strategy (TDD)

### Test Organization

```
tests/unit/
  test_credentials.py              # S3Credentials, HTTPHeaders, AuthContext
  test_store_credentials.py        # CredentialManager, Store integration
  test_store_filesystems.py        # FileSystemFactory implementations
  test_streaming.py                # WorkerContext, StreamingIterator
```

### Testing Principles

1. **Isolation**: Each class tested independently with mocks
2. **SOLID Compliance**: Test interfaces, not implementations
3. **Thread Safety**: Concurrent access tests for CredentialManager
4. **Serialization**: Pickle round-trips for distributed execution
5. **Expiration**: Credentials with future/past expiration times

### Example Test Structure

```python
# test_credentials.py

class TestS3Credentials:
    """Test S3Credentials frozen dataclass."""

    def test_creation(self):
        """S3Credentials can be created with required fields."""

    def test_frozen(self):
        """S3Credentials is immutable."""

    def test_expiration_check(self):
        """is_expired() correctly identifies expired credentials."""

    def test_from_auth(self):
        """S3Credentials.from_auth() extracts credentials from Auth."""

    def test_serialization(self):
        """S3Credentials can be pickled for distributed execution."""

    def test_to_dict(self):
        """to_dict() produces valid s3fs kwargs."""


class TestAuthContext:
    """Test AuthContext serialization and reconstruction."""

    def test_from_auth_extracts_all_credentials(self):
        """AuthContext.from_auth() captures all necessary data."""

    def test_roundtrip_serialization(self):
        """AuthContext can be pickled and unpickled."""

    def test_to_auth_reconstructs_working_auth(self):
        """Reconstructed Auth objects are functional."""

    def test_is_valid_checks_expiration(self):
        """is_valid() returns False for expired credentials."""
```

## SOLID Principles Applied

| Principle | Application |
|-----------|-------------|
| **S**ingle Responsibility | S3Credentials: only S3 data; CredentialManager: only caching; FileSystemFactory: only filesystem creation |
| **O**pen/Closed | FileSystemFactory is open for extending with new implementations, closed for modification |
| **L**iskov Substitution | All FileSystemFactory implementations are interchangeable |
| **I**nterface Segregation | Small focused interfaces (S3Credentials, AuthContext) rather than large credential objects |
| **D**ependency Inversion | Store depends on FileSystemFactory interface, not concrete s3fs |

## Migration Path

1. **Phase 3a**: Add new credential classes alongside existing code (backward compatible)
2. **Phase 3b**: Refactor Store to use FileSystemFactory
3. **Phase 3c**: Deprecate old credential patterns (with warnings)
4. **Phase 4+**: Remove old patterns in major version

## Key Files to Create

- `earthaccess/store/credentials.py` (~450 lines)
  - S3Credentials, HTTPHeaders, AuthContext, CredentialManager

- `earthaccess/store/filesystems.py` (~200 lines)
  - FileSystemFactory, DefaultFileSystemFactory

- `earthaccess/streaming.py` (~150 lines)
  - WorkerContext, StreamingIterator

- `tests/unit/test_credentials.py` (~485 lines)
  - Comprehensive credential tests

- `tests/unit/test_store_credentials.py` (~351 lines)
  - Store integration tests

- `tests/unit/test_streaming.py` (~400 lines)
  - Parallel execution context tests

## Risk Mitigation

1. **Backward Compatibility**: Old Auth-based patterns still work
2. **Gradual Migration**: Existing code can use old API during transition
3. **Feature Parity**: New system supports all existing credential types
4. **Testing**: 100% test coverage for new code
5. **Documentation**: Migration guide for existing code

---

**Total Implementation Effort**: 2-3 weeks
**Test Coverage Target**: 95%+
**SOLID Compliance**: Full adherence to all 5 principles
