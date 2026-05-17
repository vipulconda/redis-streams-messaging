class StreamError(Exception):
    """Base error for stream operations."""


class MaxRetriesExceeded(StreamError):
    """Raised when a message exhausts retry attempts."""
