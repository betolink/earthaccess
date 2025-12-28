"""Validation utilities for the query module.

This module provides validation helpers and error types for validating query parameters.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class ValidationError:
    """A validation error for a query parameter.

    Attributes:
        field: The name of the field that failed validation
        message: A description of the validation error
        value: The invalid value (optional)
    """

    field: str
    message: str
    value: Optional[Any] = None

    def __str__(self) -> str:
        """Return a human-readable representation of the error."""
        if self.value is not None:
            return f"{self.field}: {self.message} (got: {self.value!r})"
        return f"{self.field}: {self.message}"


@dataclass
class ValidationResult:
    """The result of validating a query.

    Attributes:
        is_valid: Whether the query is valid
        errors: List of validation errors (empty if valid)
    """

    is_valid: bool = True
    errors: List[ValidationError] = field(default_factory=list)

    def add_error(
        self, field: str, message: str, value: Optional[Any] = None
    ) -> "ValidationResult":
        """Add a validation error.

        Args:
            field: The name of the field that failed validation
            message: A description of the validation error
            value: The invalid value (optional)

        Returns:
            self for method chaining
        """
        self.errors.append(ValidationError(field=field, message=message, value=value))
        self.is_valid = False
        return self

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one.

        Args:
            other: Another ValidationResult to merge

        Returns:
            self for method chaining
        """
        self.errors.extend(other.errors)
        if not other.is_valid:
            self.is_valid = False
        return self

    def __str__(self) -> str:
        """Return a human-readable representation of the validation result."""
        if self.is_valid:
            return "Validation passed"
        error_strs = [str(e) for e in self.errors]
        return "Validation failed:\n  " + "\n  ".join(error_strs)

    def raise_if_invalid(self) -> None:
        """Raise a ValueError if validation failed.

        Raises:
            ValueError: If there are validation errors
        """
        if not self.is_valid:
            raise ValueError(str(self))


def validate_type(
    value: Any,
    expected_type: type,
    field_name: str,
    result: Optional[ValidationResult] = None,
) -> ValidationResult:
    """Validate that a value is of the expected type.

    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: The name of the field being validated
        result: An existing ValidationResult to add to (creates new if None)

    Returns:
        The ValidationResult (existing or new)
    """
    if result is None:
        result = ValidationResult()

    if not isinstance(value, expected_type):
        result.add_error(
            field=field_name,
            message=f"must be of type {expected_type.__name__}",
            value=value,
        )
    return result


def validate_not_empty(
    value: Any,
    field_name: str,
    result: Optional[ValidationResult] = None,
) -> ValidationResult:
    """Validate that a value is not empty.

    Args:
        value: The value to validate
        field_name: The name of the field being validated
        result: An existing ValidationResult to add to (creates new if None)

    Returns:
        The ValidationResult (existing or new)
    """
    if result is None:
        result = ValidationResult()

    if not value:
        result.add_error(
            field=field_name,
            message="must not be empty",
            value=value,
        )
    return result


def validate_range(
    value: float,
    min_val: float,
    max_val: float,
    field_name: str,
    result: Optional[ValidationResult] = None,
) -> ValidationResult:
    """Validate that a value is within a range.

    Args:
        value: The value to validate
        min_val: The minimum allowed value (inclusive)
        max_val: The maximum allowed value (inclusive)
        field_name: The name of the field being validated
        result: An existing ValidationResult to add to (creates new if None)

    Returns:
        The ValidationResult (existing or new)
    """
    if result is None:
        result = ValidationResult()

    if not min_val <= value <= max_val:
        result.add_error(
            field=field_name,
            message=f"must be between {min_val} and {max_val}",
            value=value,
        )
    return result
