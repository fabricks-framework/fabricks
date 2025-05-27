from pyspark.sql import DataFrame


class CustomException(Exception):
    pass


class CheckException(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class CheckWarning(CheckException):
    pass


class PreRunCheckException(CheckException):
    pass


class PostRunCheckException(CheckException):
    pass


class PreRunCheckWarning(CheckWarning):
    pass


class PostRunCheckWarning(CheckWarning):
    pass


class PreRunInvokeException(CustomException):
    pass


class PostRunInvokeException(CustomException):
    pass


class SkipRunCheckWarning(CheckWarning):
    pass
