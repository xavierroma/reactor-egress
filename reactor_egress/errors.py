"""Public exception types for reactor_egress."""


class EgressError(Exception):
    """Base exception for all reactor-egress failures."""


class ConfigError(EgressError):
    """Raised when user-provided options are invalid."""


class SourceError(EgressError):
    """Raised when reading from the source fails."""


class SinkError(EgressError):
    """Raised when writing to the sink fails."""
