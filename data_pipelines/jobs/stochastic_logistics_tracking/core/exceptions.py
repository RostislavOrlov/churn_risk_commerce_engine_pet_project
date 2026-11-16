class RealTimeCommerceException(Exception):
    def __init__(self, message: str, context: dict = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or {}


class DataQualityViolationException(RealTimeCommerceException):
    pass
