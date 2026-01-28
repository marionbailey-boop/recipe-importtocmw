class AppError(Exception):
    """
    Base application error.
    All custom exceptions should inherit from this.
    """
    def __init__(self, message: str, *, code: str | None = None, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __str__(self) -> str:
        return self.message


class ValidationError(AppError):
    """
    Raised when incoming JSON is structurally invalid
    or violates pre-import rules.
    """
    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message, code="VALIDATION_ERROR", details=details)


class MappingError(AppError):
    """
    Raised when mapping from Nooko schema to CMC schema fails.
    (missing required fields, incompatible values, etc.)
    """
    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message, code="MAPPING_ERROR", details=details)


class DownstreamError(AppError):
    """
    Raised when the CMC import API or downstream system fails.
    """
    def __init__(self, message: str, *, status_code: int | None = None, details: dict | None = None):
        super().__init__(message, code="DOWNSTREAM_ERROR", details=details)
        self.status_code = status_code
