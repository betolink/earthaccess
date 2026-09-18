"""Earthdata Environments/Systems module."""

import os
from dataclasses import dataclass
from typing import NewType, Optional

from cmr import CMR_OPS, CMR_UAT

CMRBaseURL = NewType("CMRBaseURL", str)
EDLHostname = NewType("EDLHostname", str)
StatusURL = NewType("StatusURL", str)
StatusApiURL = NewType("StatusApiURL", str)

PROXY_ENV_VAR = "EARTHDATA_PROXY_URL"

_proxy: Optional[str] = None


def set_proxy(url: Optional[str]) -> None:
    """Route Earthdata HTTP requests through a proxy.

    This is useful in environments where NASA's Earthdata services cannot be
    reached directly, e.g. because of CORS restrictions in JupyterLite/Pyodide.

    The proxy is expected to accept a request whose URL encodes the original
    target URL as a path, e.g. ``<proxy>/https://cmr.earthdata.nasa.gov/search``,
    forward it, and return the response with permissive CORS headers.

    Pass `None` to clear an explicitly set proxy. When no proxy has been set
    explicitly, the ``EARTHDATA_PROXY_URL`` environment variable is used, if set.

    Parameters:
        url: The base URL of the proxy, or `None` to clear it.

    Examples:
        ```python
        import earthaccess

        earthaccess.set_proxy("https://earthdata-proxy.example.workers.dev")
        ```
    """
    global _proxy
    _proxy = _normalize_proxy(url)


def get_proxy() -> Optional[str]:
    """Return the currently configured proxy URL, if any.

    An explicitly set proxy (via :func:`set_proxy`) takes precedence over the
    ``EARTHDATA_PROXY_URL`` environment variable.

    Returns:
        The normalized proxy URL, or `None` if no proxy is configured.
    """
    return _proxy or _normalize_proxy(os.environ.get(PROXY_ENV_VAR))


def _normalize_proxy(url: Optional[str]) -> Optional[str]:
    """Normalize a proxy URL so it can be prepended to an absolute URL."""
    if not url:
        return None
    return url if url.endswith("/") else f"{url}/"


def route(url: str) -> str:
    """Prefix a URL with the configured proxy, if any.

    Parameters:
        url: A fully-qualified URL.

    Returns:
        The proxied URL if a proxy is configured, otherwise `url` unchanged.
    """
    proxy = get_proxy()
    return f"{proxy}{url}" if proxy else url


@dataclass(frozen=True)
class System:
    """Host URL options, for different Earthdata domains."""

    cmr_base_url: CMRBaseURL
    status_url: StatusURL
    status_api_url: StatusApiURL
    edl_hostname: EDLHostname

    def route(self, url: str) -> str:
        """Prefix a URL with the configured proxy, if any.

        Parameters:
            url: A fully-qualified URL.

        Returns:
            The proxied URL if a proxy is configured, otherwise `url` unchanged.
        """
        return route(url)


PROD = System(
    CMRBaseURL(CMR_OPS),
    StatusURL("https://status.earthdata.nasa.gov/"),
    StatusApiURL("https://status.earthdata.nasa.gov/api/v1/statuses"),
    EDLHostname("urs.earthdata.nasa.gov"),
)
UAT = System(
    CMRBaseURL(CMR_UAT),
    StatusURL("https://status.uat.earthdata.nasa.gov/"),
    StatusApiURL("https://status.uat.earthdata.nasa.gov/api/v1/statuses"),
    EDLHostname("uat.urs.earthdata.nasa.gov"),
)
