"""
Custom exceptions.
"""


class CustomError(Exception):
    """Base databases error."""

    pass


class DatabaseConnectionError(CustomError):
    """Database connection error."""

    pass


class DuplicateUserError(CustomError):
    """User with this name already exists (uniqueness violated)."""

    pass


class DatabaseQueryError(CustomError):
    """Query execution error (non-unique, invalid, syntax, etc.)."""

    pass


class PoolConnectionError(CustomError):
    """Connection pool error."""

    pass


class RedisConnectionError(CustomError):
    """Redis connection error."""

    pass

class RedisError(CustomError):
    """Unexpected Redis error."""

    pass


class OperatorError(CustomError):
    """Error from some Operator worker."""

    pass

class SchedulerError(CustomError):
    """Error during AsyncIOScheduler operation."""

    pass

class BrainError(CustomError):
    """Error during brain_service operation."""

    pass

class StreamError(CustomError):
    """Ошибка при работе с стримом."""
    pass
