"""Redis value wrapper for typed values."""

from typing import Any


class RedisValue:
    """Wrapper for typed Redis values."""

    def __init__(self, dtype: str, value: Any):
        self.type = dtype
        self.value = value
        
    def is_type(self, type: str) -> bool:
        return self.type == type

    def is_string(self) -> bool:
        return self.type == "string"

    def is_list(self) -> bool:
        return self.type == "list"

    def is_stream(self) -> bool:
        return self.type == "stream"
    
    def is_zset(self) -> bool:
        return self.type == "zset"
