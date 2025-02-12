class PreRunCheckFailedException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PostRunCheckFailedException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PreRunCheckWarningException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PostRunCheckWarningException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class PreRunInvokerFailedException(Exception):
    pass


class PostRunInvokerFailedException(Exception):
    pass
