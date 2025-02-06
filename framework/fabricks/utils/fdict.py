from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, overload

T = TypeVar("T")


class FDict:
    """
    A flexible dictionary wrapper that provides type-safe access to nested data structures
    with convenient conversion methods.
    """

    def __init__(self, options: Union[Dict[str, Any], Any, None] = None):
        """
        Initialize FDict with a dictionary of options.

        Args:
            options: Input dictionary. If None, creates an empty dictionary.
        """
        self.options = options if options is not None else {}

    def __getitem__(self, key: str) -> Any:
        """Enable dictionary-like access with [] operator."""
        return self.options[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Enable dictionary-like value setting with [] operator."""
        self.options[key] = value

    def __contains__(self, key: str) -> bool:
        """Enable 'in' operator for membership testing."""
        return key in self.options

    def __repr__(self) -> str:
        """Return string representation of the FDict."""
        return f"FDict({self.options})"

    def to_dict(self) -> Dict[str, Any]:
        """Convert FDict to a regular dictionary."""
        return self.options

    @overload
    def get(self, key: str) -> Optional[Any]: ...

    @overload
    def get(self, key: str, default: T) -> Union[Any, T]: ...

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the dictionary with an optional default.

        Args:
            key: The key to look up
            default: Value to return if key is not found

        Returns:
            The value associated with the key or the default value
        """
        return self.options.get(key, default)

    def get_list(self, key: str, default: Optional[List[Any]] = None) -> List[Any]:
        """
        Get a value as a list, converting single items to a single-item list.

        Args:
            key: The key to look up
            default: Default value if key is not found

        Returns:
            A list containing the value(s)
        """
        values = self.options.get(key, default if default is not None else [])
        if values is None:
            return []

        return [values] if not isinstance(values, list) else values

    def get_boolean(self, key: str, default: Optional[bool] = None) -> Optional[bool]:
        """
        Get a value as a boolean, with string conversion support.

        Args:
            key: The key to look up
            default: Default value if key is not found

        Returns:
            Boolean value of the key, or default if key not found
        """
        value = self.options.get(key)

        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")

        return bool(value)

    def get_dict(self, key: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a nested dictionary, with a default empty dict if not found.

        Args:
            key: The key to look up
            default: Default value if key is not found

        Returns:
            Dictionary value of the key, or default if key not found
        """
        return self.options.get(key, default if default is not None else {})

    def get_nested(self, *keys: str, default: Any = None) -> Any:
        """
        Access nested dictionary values using a sequence of keys.

        Args:
            *keys: Sequence of keys to traverse
            default: Default value if path not found

        Returns:
            Value at the nested path, or default if path not found
        """
        current = self.options
        for key in keys:
            if not isinstance(current, dict):
                return default
            if key not in current:
                return default
            current = current[key]

        return current

    def set_nested(self, *keys: str, value: Any) -> None:
        """
        Set a value in a nested dictionary path, creating intermediate dictionaries as needed.

        Args:
            *keys: Sequence of keys defining the path
            value: Value to set at the path
        """
        current = self.options
        for key in keys[:-1]:
            current = current.setdefault(key, {})

        current[keys[-1]] = value

    def filter(self, predicate: Callable[[str, Any], bool]) -> "FDict":
        """
        Create a new FDict with key-value pairs that satisfy the predicate function.

        Args:
            predicate: Lambda function that takes key and value as arguments and returns bool

        Returns:
            New FDict containing only the filtered key-value pairs

        Example:
            # Get all items with numeric values greater than 10
            filtered = fdict.filter(lambda k, v: isinstance(v, (int, float)) and v > 10)
        """
        filtered_dict = {k: v for k, v in self.options.items() if predicate(k, v)}
        return FDict(filtered_dict)

    def filter_keys(self, predicate: Callable[[str], bool]) -> "FDict":
        """
        Create a new FDict with keys that satisfy the predicate function.

        Args:
            predicate: Lambda function that takes key as argument and returns bool

        Returns:
            New FDict containing only the filtered keys

        Example:
            # Get all items with keys starting with 'user_'
            filtered = fdict.filter_keys(lambda k: k.startswith('user_'))
        """
        return self.filter(lambda k, _: predicate(k))

    def filter_values(self, predicate: Callable[[Any], bool]) -> "FDict":
        """
        Create a new FDict with values that satisfy the predicate function.

        Args:
            predicate: Lambda function that takes value as argument and returns bool

        Returns:
            New FDict containing only the filtered values

        Example:
            # Get all items with string values
            filtered = fdict.filter_values(lambda v: isinstance(v, str))
        """
        return self.filter(lambda _, v: predicate(v))

    def map_values(self, transform: Callable[[Any], Any]) -> "FDict":
        """
        Create a new FDict with transformed values using the provided function.

        Args:
            transform: Lambda function that takes a value and returns transformed value

        Returns:
            New FDict containing transformed values

        Example:
            # Convert all string values to uppercase
            transformed = fdict.map_values(lambda v: v.upper() if isinstance(v, str) else v)
        """
        transformed_dict = {k: transform(v) for k, v in self.options.items()}
        return FDict(transformed_dict)

    def deep_filter(self, predicate: Callable[[str, Any], bool]) -> "FDict":
        """
        Recursively filter nested dictionaries using the predicate function.

        Args:
            predicate: Lambda function that takes key and value as arguments and returns bool

        Returns:
            New FDict with filtered nested structure

        Example:
            # Filter all nested numeric values greater than 10
            filtered = fdict.deep_filter(lambda k, v:
                not isinstance(v, dict) and isinstance(v, (int, float)) and v > 10)
        """

        def filter_recursive(d: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    filtered_nested = filter_recursive(v)
                    if filtered_nested:  # Only include non-empty nested dicts
                        result[k] = filtered_nested
                elif predicate(k, v):
                    result[k] = v
            return result

        return FDict(filter_recursive(self.options))
