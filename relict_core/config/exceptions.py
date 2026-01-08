"""
Custom exceptions.
"""


class CustomError(Exception):
    """Base databases error."""

    pass


class DuplicateUserError(CustomError):
    """User with this name already exists (uniqueness violated)."""

    pass
