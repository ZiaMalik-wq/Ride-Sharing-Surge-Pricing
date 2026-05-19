"""Custom exception types for the Surge Pricing API.

These are raised by the service layer and translated into HTTP responses
by the exception handlers registered in ``main.py``.
"""


class ZoneNotFoundError(Exception):
    """Raised when a requested zone does not exist in the live data."""


class ServiceUnavailableError(Exception):
    """Raised when a backing store (Redis / Cassandra) is unreachable."""
