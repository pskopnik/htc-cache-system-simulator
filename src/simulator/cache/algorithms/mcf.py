from apq import KeyedPQ
from typing import Iterable, Optional

from ..state import AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor, Storage


class MCF(StateDrivenOnlineProcessor):
	"""Processor evicting the file with the lowest fetch cost, i.e. file size.
	"""

	class State(StateDrivenOnlineProcessor.State):
		class Item(StateDrivenOnlineProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self) -> None:
			self._pq: KeyedPQ[None] = KeyedPQ()

		def pop_eviction_candidates(
			self,
			file: FileID = '',
			ts: int = 0,
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
		) -> Iterable[FileID]:
			file, _, _ = self._pq.pop() # Raises IndexError if empty
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return MCF.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, MCF.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			self._pq.add_or_change_value(file, info.total_bytes, None)

	def _init_state(self) -> 'MCF.State':
		return MCF.State()
