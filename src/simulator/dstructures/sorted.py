from sortedcontainers import SortedDict
from typing import Any, Callable, Generic, Optional, TypeVar, TYPE_CHECKING

_KT = TypeVar('_KT')
_VT = TypeVar('_VT')

if TYPE_CHECKING:
	class _BaseSortedDict(Generic[_KT, _VT], SortedDict[_KT, _VT]):
		pass
else:
	class _BaseSortedDict(Generic[_KT, _VT], SortedDict):
		pass


class SortedDefaultDict(_BaseSortedDict[_KT, _VT]):
	"""DefaultDict-style dict sub-class where iteration respects key order.
	"""
	def __init__(
		self,
		default_factory: Optional[Callable[[], _VT]] = None,
		*args: Any,
		**kwargs: Any,
	) -> None:
		super(SortedDefaultDict, self).__init__(*args, **kwargs)
		self.default_factory: Optional[Callable[[], _VT]] = default_factory

	def __missing__(self, key: _KT) -> _VT:
		if self.default_factory is not None:
			val = self.default_factory()
			self[key] = val
			return val
		raise KeyError(key)
