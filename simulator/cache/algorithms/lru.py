from collections import OrderedDict
from typing import Generic, Iterable, Iterator, Optional, Tuple, TypeVar

from ..processor import AccessInfo, Access
from ..state import FileID, StateDrivenProcessor, StateDrivenOnlineProcessor

KeyType = TypeVar('KeyType')
ElementType = TypeVar('ElementType')

# Would be great to inherit from OrderedDict for implementation but not
# inheriting the interface (python/typing#241).
# Unfortunately it is not possible to re-assign the special (dunder) methods
# in __init__.
# Both would speed up implementation.
# https://github.com/python/typing/issues/241

class LRUStructure(Generic[KeyType, ElementType]):
	def __init__(self) -> None:
		self._odict: OrderedDict[KeyType, ElementType] = OrderedDict()

	def __len__(self) -> int:
		return len(self._odict)

	def __contains__(self, key: KeyType) -> bool:
		return key in self._odict

	def __getitem__(self, key: KeyType) -> ElementType:
		return self._odict[key]

	def __setitem__(self, key: KeyType, el: ElementType) -> None:
		self._odict[key] = el

	def __delitem__(self, key: KeyType) -> None:
		del self._odict[key]

	def __iter__(self) -> Iterator[KeyType]:
		"""Returns an iterator yielding the most recently accessed elements
		first.
		"""
		return iter(self._odict.keys())

	def access(self, key: KeyType) -> None:
		"""Replicates an element access by moving the element to the front of
		the LRU structure.

		If ensure is False and the element is not in the structure, a KeyError
		will be raised.
		"""
		self._odict.move_to_end(key, last=False)

	def pop(self) -> Tuple[KeyType, ElementType]:
		"""Removes the least recently accessed element from the LRU structure.

		Raises a KeyError if there are no elements.
		"""
		return self._odict.popitem()


class LRU(StateDrivenOnlineProcessor):
	"""Processor evicting the least recently accessed file from the cache.
	"""

	class State(StateDrivenProcessor.State):
		class Item(StateDrivenProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self) -> None:
			self._lru: LRUStructure[FileID, None] = LRUStructure()

		def pop_eviction_candidates(
			self,
			file: FileID = '',
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
		) -> Iterable[FileID]:
			file, _ = self._lru.pop() # Raises KeyError if empty
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._lru:
				return LRU.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, LRU.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._lru[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			if ensure:
				self._lru[file] = None
			self._lru.access(file)

	def _init_state(self) -> 'LRU.State':
		return LRU.State()
