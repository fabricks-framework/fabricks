from typing import Any, List, Optional


class FDict:
    def __init__(self, options: Any):
        self.options = options

    def get(self, key: str) -> Optional[Any]:
        return self.options.get(key)

    def get_list(self, key: str) -> List[Any]:
        values = self.options.get(key, [])
        if values is not None:
            if not isinstance(values, List):
                values = [values]
        return values

    def get_boolean(self, key: str, if_none: Optional[bool] = None) -> Optional[bool]:
        o = self.options.get(key)
        if isinstance(o, bool):
            return o
        elif o is not None:
            return o.lower() == "true"
        else:
            return if_none

    def get_dict(self, key) -> dict:
        return self.options.get(key, {})
