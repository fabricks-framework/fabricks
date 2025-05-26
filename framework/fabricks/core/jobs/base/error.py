from pyspark.sql import DataFrame


class CustomException(Exception):
    def __init__(self, fail: bool, *args, **kwargs):
        self.fail = fail
        super().__init__(*args, **kwargs)


class PreRunCheckException(CustomException):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(fail=True, message=self.message)


class PostRunCheckException(CustomException):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(fail=True, message=self.message)


class PreRunCheckWarning(CustomException):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(fail=False, message=self.message)


class PostRunCheckWarning(CustomException):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(fail=False, message=self.message)


class PreRunInvokeException(CustomException):
    def __init__(self, *args):
        super().__init__(fail=True, *args)


class PostRunInvokeException(CustomException):
    def __init__(self, *args):
        super().__init__(fail=True, *args)


class RunSkipWarning(CustomException):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(fail=False, message=self.message)
