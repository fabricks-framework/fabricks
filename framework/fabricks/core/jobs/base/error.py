class CheckFailedException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class CheckWarningException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class InvokerFailedException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
