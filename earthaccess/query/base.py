"""Base query class for earthaccess.

This module provides the abstract base class for all query types.
Query classes support both method chaining and named parameter construction.
"""

from abc import ABC, abstractmethod
from copy import deepcopy
from inspect import getmembers, ismethod
from typing import Any, Dict, List, Optional

from typing_extensions import Self

from .types import DateRange, SpatialType
from .validation import ValidationResult


class QueryBase(ABC):
    """Abstract base class for query objects.

    Query objects encapsulate search parameters and can be converted to either
    CMR or STAC format. They support method chaining and named parameters.

    All query methods return `self` to allow method chaining:
        query = GranuleQuery().short_name("ATL03").temporal("2020-01", "2020-02")

    Queries can also be constructed with named parameters:
        query = GranuleQuery(short_name="ATL03", temporal=("2020-01", "2020-02"))
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a query with optional named parameters.

        Args:
            **kwargs: Named parameters matching method names. Values can be
                single values or tuples for methods with multiple parameters.

        Raises:
            ValueError: If a parameter name doesn't match a method.
            TypeError: If a parameter value doesn't match the method signature.
        """
        self._params: Dict[str, Any] = {}
        self._temporal_ranges: List[DateRange] = []
        self._spatial: Optional[SpatialType] = None

        # Apply any named parameters
        if kwargs:
            self.parameters(**kwargs)

    def parameters(self, **kwargs: Any) -> Self:
        """Apply query parameters as keyword arguments.

        The keyword needs to match the name of a method, and the value should
        either be a single value or a tuple of values matching the method signature.

        Example:
            >>> query = GranuleQuery().parameters(
            ...     short_name="ATL03",
            ...     temporal=("2020-01", "2020-02"),
            ...     bounding_box=(-180, -90, 180, 90)
            ... )

        Args:
            **kwargs: Named parameters matching method names.

        Returns:
            self for method chaining

        Raises:
            ValueError: If a parameter name doesn't match a method.
            TypeError: If a parameter value doesn't match the method signature.
        """
        methods = dict(getmembers(self, predicate=ismethod))

        for key, val in kwargs.items():
            if key not in methods:
                raise ValueError(f"Unknown parameter: {key}")

            if isinstance(val, tuple):
                methods[key](*val)
            else:
                methods[key](val)

        return self

    def copy(self) -> Self:
        """Create a deep copy of this query.

        Returns:
            A new query instance with the same parameters.
        """
        return deepcopy(self)

    def validate(self) -> ValidationResult:
        """Validate the current query parameters.

        Returns:
            A ValidationResult indicating whether the query is valid.
        """
        result = ValidationResult()
        self._validate(result)
        return result

    def _validate(self, result: ValidationResult) -> None:
        """Internal validation method to be overridden by subclasses.

        Args:
            result: The ValidationResult to add errors to.
        """
        pass

    @abstractmethod
    def to_cmr(self) -> Dict[str, Any]:
        """Convert the query to CMR API format.

        Returns:
            A dictionary of CMR query parameters.
        """
        pass

    @abstractmethod
    def to_stac(self) -> Dict[str, Any]:
        """Convert the query to STAC API format.

        Returns:
            A dictionary of STAC API query parameters.
        """
        pass

    def _set_param(self, key: str, value: Any) -> Self:
        """Set a query parameter.

        Args:
            key: The parameter name
            value: The parameter value

        Returns:
            self for method chaining
        """
        self._params[key] = value
        return self

    def _get_param(self, key: str, default: Any = None) -> Any:
        """Get a query parameter.

        Args:
            key: The parameter name
            default: Default value if not set

        Returns:
            The parameter value or default
        """
        return self._params.get(key, default)

    def _has_param(self, key: str) -> bool:
        """Check if a parameter is set.

        Args:
            key: The parameter name

        Returns:
            True if the parameter is set
        """
        return key in self._params

    def __repr__(self) -> str:
        """Return a string representation of the query."""
        class_name = self.__class__.__name__
        params_str = ", ".join(f"{k}={v!r}" for k, v in self._params.items())
        return f"{class_name}({params_str})"

    def __eq__(self, other: object) -> bool:
        """Check if two queries are equal."""
        if not isinstance(other, QueryBase):
            return NotImplemented
        return (
            type(self) is type(other)
            and self._params == other._params
            and self._temporal_ranges == other._temporal_ranges
            and self._spatial == other._spatial
        )
