from collections import OrderedDict
from typing import Iterable, Optional

from ...dstructures.lru import LRUDict
from ..state import Access, AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor


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
			self._lru: LRUDict[FileID, None] = LRUDict()

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
