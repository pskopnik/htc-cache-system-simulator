from typing import Any
from .exceptions import ValidationError

def validate(instance: Any, schema: Any, cls: Any=...) -> None:
	...

__all__ = [
	'ValidationError',
]
