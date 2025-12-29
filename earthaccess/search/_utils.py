"""Internal utilities for search module.

This module contains helper functions used by the search classes.
It's separate to avoid circular import issues.
"""

from typing import Any, List, Union

import requests

from cmr import CollectionQuery, GranuleQuery, ServiceQuery


def get_results(
    session: requests.Session,
    query: Union[CollectionQuery, GranuleQuery, ServiceQuery],
    limit: int = 2000,
) -> List[Any]:
    """Fetch paginated results from CMR using search-after headers.

    Parameters:
        session: HTTP session for making requests
        query: CMR query object (CollectionQuery, GranuleQuery, or ServiceQuery)
        limit: Maximum number of results to return (default: 2000)

    Returns:
        List of result items from CMR

    Raises:
        RuntimeError: If the CMR query fails
    """
    page_size = min(limit, 2000)
    url = query._build_url()
    results: List[Any] = []
    headers = dict(query.headers or {})

    while True:
        response = session.get(url, headers=headers, params={"page_size": page_size})

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            raise RuntimeError(ex.response.text) from ex

        items = response.json()["items"]
        results.extend(items)

        # Stop if we've reached the limit or there are no more results
        if len(items) < page_size or len(results) >= limit:
            break

        # Use search-after for next page
        if search_after := response.headers.get("cmr-search-after"):
            headers["cmr-search-after"] = search_after
        else:
            break

    return results
