from sortedcontainers import SortedDict
from typing import Any, Callable, Generic, Optional, TypeVar, TYPE_CHECKING

K = TypeVar('K')
V = TypeVar('V')

if TYPE_CHECKING:
	class _BaseSortedDict(Generic[K, V], SortedDict[K, V]):
		pass
else:
	class _BaseSortedDict(Generic[K, V], SortedDict):
		pass


class SortedDefaultDict(_BaseSortedDict[K, V]):
	"""DefaultDict sub-class where iteration respects key order.
	"""
	def __init__(
		self,
		default_factory: Optional[Callable[[], V]] = None,
		*args: Any,
		**kwargs: Any,
	) -> None:
		super(SortedDefaultDict, self).__init__(*args, **kwargs)
		self.default_factory: Optional[Callable[[], V]] = default_factory

	def __missing__(self, key: K) -> V:
		if self.default_factory is not None:
			return self.default_factory()
		raise KeyError(key)
