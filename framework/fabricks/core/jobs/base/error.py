from pyspark.sql import DataFrame


class PreRunCheckException(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class PostRunCheckException(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class PreRunCheckWarning(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class PostRunCheckWarning(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)


class PreRunInvokeException(Exception):
    pass


class PostRunInvokeException(Exception):
    pass


class RunSkipWarning(Exception):
    def __init__(self, message: str, dataframe: DataFrame):
        self.message = message
        self.dataframe = dataframe

        super().__init__(self.message)
