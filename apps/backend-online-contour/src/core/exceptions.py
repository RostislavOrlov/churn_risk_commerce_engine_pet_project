from fastapi import status


class RealTimeCommerceException(Exception):
    def __init__(
        self,
        message: str,
        code: str = "INTERNAL_SERVER_ERROR",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code


class SecurityException(RealTimeCommerceException):
    def __init__(self, message: str, code: str = "AUTH_VALIDATION_FAILED"):
        super().__init__(message, code, status.HTTP_401_UNAUTHORIZED)


class RBACException(RealTimeCommerceException):
    def __init__(self, message: str, code: str = "ACCESS_FORBIDDEN"):
        super().__init__(message, code, status.HTTP_403_FORBIDDEN)


class DataQualityViolationException(RealTimeCommerceException):
    def __init__(self, message: str, code: str = "DATA_QUALITY_GATE_VIOLATION"):
        super().__init__(message, code, status.HTTP_422_UNPROCESSABLE_ENTITY)


class InfrastructureTimeoutException(RealTimeCommerceException):
    def __init__(self, message: str, code: str = "DATABASE_TIMEOUT"):
        super().__init__(message, code, status.HTTP_504_GATEWAY_TIMEOUT)


class EntityNotFoundException(RealTimeCommerceException):
    def __init__(self, message: str, code: str = "ENTITY_NOT_FOUND"):
        super().__init__(message, code, status.HTTP_404_NOT_FOUND)
