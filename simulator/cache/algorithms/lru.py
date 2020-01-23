from ..processor import AccessInfo, Access
from ..state import FileID, StateDrivenProcessor, StateDrivenOnlineProcessor
from typing import Generic, Iterable, Iterator, Optional, TypeVar
# from typing import OrderedDict # TODO This is not properly recognised by mypy
from collections import OrderedDict


ElementType = TypeVar('ElementType')

class LRUStructure(Generic[ElementType]):
	def __init__(self) -> None:
		self._odict: OrderedDict[ElementType, None] = OrderedDict()

	def __len__(self) -> int:
		return len(self._odict)

	def __contains__(self, el: ElementType) -> bool:
		return el in self._odict

	def __delitem__(self, el: ElementType) -> None:
		del self._odict[el]

	def __iter__(self) -> Iterator[ElementType]:
		"""Returns an iterator yielding the most recently accessed elements
		first.
		"""
		return iter(self._odict.keys())

	def access(self, el: ElementType, ensure: bool=True) -> None:
		"""Reproduces an element access by moving the element to the front of
		the LRU structure.

		If ensure is False and the element is not in the structure, a KeyError
		will be raised.
		"""
		if ensure:
			self._odict[el] = None

		self._odict.move_to_end(el, last=False)

	def pop(self) -> ElementType:
		"""Removes the least recently accessed element from the LRU structure.

		Raises a KeyError if there are no elements.
		"""
		key, _ = self._odict.popitem()
		return key


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
			self._lru: LRUStructure[FileID] = LRUStructure()

		def pop_eviction_candidates(
			self,
			file: FileID = "",
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
		) -> Iterable[FileID]:
			return (self._lru.pop(),)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._lru:
				return LRU.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, LRU.State.Item):
				raise TypeError("unsupported item type passed")

			del self._lru[item._file]

		def remove_file(self, file: FileID) -> None:
			del self._lru[file]

		def process_access(
			self,
			file: FileID,
			ind: int = 0,
			ensure: bool = True,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> None:
			self._lru.access(file, ensure=ensure)

	def _init_state(self) -> 'LRU.State':
		return LRU.State()
