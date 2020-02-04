import bisect
from typing import (
	Any,
	Callable,
	cast,
	DefaultDict,
	Dict,
	Generic,
	ItemsView,
	Iterable,
	Iterator,
	KeysView,
	List,
	Mapping,
	overload,
	Tuple,
	TypeVar,
	Union,
	ValuesView,
)

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

# KeysView, ValuesView and ItemsView provide fully implemented views onto a
# mapping using only the mapping's public interface. However, the constructors
# are not documented (API stability?) and thus fail mypy. These functions
# provide a workaround.

def _init_keys_view(mapping: Mapping[K, V]) -> KeysView[K]:
	return cast('Callable[[Mapping[K, V]], KeysView[K]]', KeysView)(mapping)

def _init_values_view(mapping: Mapping[K, V]) -> ValuesView[V]:
	return cast('Callable[[Mapping[K, V]], ValuesView[V]]', ValuesView)(mapping)

def _init_items_view(mapping: Mapping[K, V]) -> ItemsView[K, V]:
	return cast('Callable[[Mapping[K, V]], ItemsView[K, V]]', ItemsView)(mapping)


class KeyOrderedDict(Generic[K, V], Dict[K, V]):
	"""dict sub-class where iteration respects key order.
	"""

	def __init__(self, *args: Any, **kwargs: Any) -> None:
		super(KeyOrderedDict, self).__init__(*args, **kwargs)
		self._key_list: List[K] = []

	def __iter__(self) -> Iterator[K]:
		return iter(self._key_list)

	def __setitem__(self, key: K, val: V) -> None:
		if not super(KeyOrderedDict, self).__contains__(key):
			bisect.insort(self._key_list, key)
		super(KeyOrderedDict, self).__setitem__(key, val)

	def __delitem__(self, key: K) -> None:
		super(KeyOrderedDict, self).__delitem__(key)
		del self._key_list[self._key_index(key)]

	def clear(self) -> None:
		super(KeyOrderedDict, self).clear()
		self._key_list.clear()

	@overload
	def pop(self, key: K) -> V:
		...
	@overload
	def pop(self, key: K, default: Union[V, T]=...) -> Union[V, T]:
		...

	# def pop(self, key: K, default: Union[V, T, None]=None) -> Union[V, T]:
	# Overloaded function implementation does not accept all possible arguments of signature 2
	# Overloaded function implementation cannot produce return type of signature 2

	def pop(self, key: K, default: Any=None) -> Any:
		if super(KeyOrderedDict, self).__contains__(key):
			del self._key_list[self._key_index(key)]
		return super(KeyOrderedDict, self).pop(key, default=default)

	def popitem(self) -> Tuple[K, V]:
		# TODO: respect key order instead of dict (insertion) LIFO order
		popped = super(KeyOrderedDict, self).popitem()
		del self._key_list[self._key_index(popped[0])]
		return popped

	def setdefault(self, key: K, default: V=cast('V', None)) -> V:
		if not super(KeyOrderedDict, self).__contains__(key):
			bisect.insort(self._key_list, key)
		return super(KeyOrderedDict, self).setdefault(key, default=default)

	@overload
	def update(self, __m: Mapping[K, V], **kwargs: V) -> None:
		...
	@overload
	def update(self, __m: Iterable[Tuple[K, V]], **kwargs: V) -> None:
		...
	@overload
	def update(self, **kwargs: V) -> None:
		...
	def update(self, *args: Any, **kwargs: Any) -> None:
		super(KeyOrderedDict, self).update(*args, **kwargs)
		self._key_list = sorted(super(KeyOrderedDict, self).keys())

	def keys(self) -> KeysView[K]:
		return _init_keys_view(self)

	def values(self) -> ValuesView[V]:
		return _init_values_view(self)

	def items(self) -> ItemsView[K, V]:
		return _init_items_view(self)

	def _key_index(self, key: K) -> int:
	    """Locate the leftmost value exactly equal to key.

	    Method copied from the Python documentation:

	    https://docs.python.org/3.7/library/bisect.html
	    """
	    i = bisect.bisect_left(self._key_list, key)
	    if i != len(self._key_list) and self._key_list[i] == key:
	        return i
	    raise ValueError


class KeyOrderedDefaultDict(Generic[K, V], KeyOrderedDict[K, V], DefaultDict[K, V]):
	"""DefaultDict sub-class where iteration respects key order.
	"""
