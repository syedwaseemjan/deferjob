class DeferjobError(Exception):
    """Base error for the library."""


class NotConfigured(DeferjobError):
    """Defer was constructed without a connection string or connect=."""


class JobExists(DeferjobError):
    """A pending or running job already uses this key."""


class JobNotFound(DeferjobError):
    """No matching job for the given id or key."""


class JobNotPending(DeferjobError):
    """The job exists but is not pending, so it cannot be changed."""


class UnknownJob(DeferjobError):
    """The worker claimed a job name with no registered handler."""
