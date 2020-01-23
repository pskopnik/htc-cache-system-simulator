from apq import KeyedPQ, Item
from dataclasses import dataclass, field
from ..state import FileID, StateDrivenProcessor, StateDrivenOnlineProcessor
from typing import Iterable, Optional


class GreedyDual(StateDrivenOnlineProcessor):
	"""Processor evicting the least recently accessed file from the cache.
	"""

	class State(StateDrivenProcessor.State):
		@dataclass
		class _FileInfo(object):
			access_threshold: float = field(init=True)

		class Item(StateDrivenProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self) -> None:
			self._pq: KeyedPQ[GreedyDual.State._FileInfo] = KeyedPQ()
			self._threshold: float = 0.0

		def pop_eviction_candidates(
			self,
			file: FileID = "",
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			free_bytes: int = 0,
			required_bytes: int = 0,
		) -> Iterable[FileID]:
			file, running_credit, _ = self._pq.pop()
			self._threshold = running_credit
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return GreedyDual.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, GreedyDual.State.Item):
				raise TypeError("unsupported item type passed")

			del self._pq[item._file]

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(
			self,
			file: FileID,
			ind: int = 0,
			ensure: bool = True,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> None:
			it: Optional[Item[GreedyDual.State._FileInfo]]
			try:
				it = self._pq[file]
			except KeyError:
				it = None

			credit = total_bytes + self._threshold
			if it is None:
				it = self._pq.add(file, credit, GreedyDual.State._FileInfo(0.0))
			else:
				self._pq.change_value(it, credit)

			it.data.access_threshold = self._threshold

	def _init_state(self) -> 'GreedyDual.State':
		return GreedyDual.State()
