from apq import KeyedPQ, Item
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Iterable, Optional

from ..state import AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor, Storage


class Mode(Enum):
	TOTAL_SIZE = auto()
	ACCESS_SIZE = auto()


class GreedyDual(StateDrivenOnlineProcessor):
	"""Processor evicting the file with the lowest "credit" from the cache.

	Credit is set to the fetch cost when the file is placed into the cache and
	re-set whenever the file is re-accessed. The file with the lowest credit
	is evicted and the value of its credit is deducted from the credit of all
	files remaining in the cache.

	That means GreedyDual is a cost-aware generalisation of LRU.

	The cost of the file is determined by the mode argument. It can either be
	the total size of the size in the cache (TOTAL_SIZE) or the size of the
	accessed fraction of the file (ACCESS_SIZE). When running in ACCESS_SIZE
	mode the credit is never reduced, i.e. the credit remains unchanged when
	greater than the accessed fraction.
	"""

	class State(StateDrivenOnlineProcessor.State):
		@dataclass
		class _FileInfo(object):
			access_threshold: float = field(init=False)

		class Item(StateDrivenOnlineProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, mode: Mode) -> None:
			self._mode: Mode = mode
			self._pq: KeyedPQ[GreedyDual.State._FileInfo] = KeyedPQ()
			self._threshold: float = 0.0

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
			file, running_credit, _ = self._pq.pop() # Raises IndexError if empty
			self._threshold = running_credit
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return GreedyDual.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, GreedyDual.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			if self._mode == Mode.TOTAL_SIZE:
				credit = float(info.total_bytes + self._threshold)
				it = self._pq.add_or_change_value(
					file,
					self._threshold + credit,
					GreedyDual.State._FileInfo(),
				)
				it.data.access_threshold = self._threshold
			elif self._mode == Mode.ACCESS_SIZE:
				self._process_access_access_size(file, info.bytes_requested)
			else:
				raise NotImplementedError

		def _process_access_access_size(self, file: FileID, requested_bytes: int) -> None:
			it: Optional[Item[GreedyDual.State._FileInfo]]
			current_credit: float
			try:
				it = self._pq[file]
				current_credit = it.value - self._threshold
			except KeyError:
				it = None
				current_credit = 0.0

			credit = max(current_credit, float(requested_bytes))
			running_credit = self._threshold + credit

			if it is not None:
				self._pq.change_value(it, running_credit)
			else:
				it = self._pq.add(file, running_credit, GreedyDual.State._FileInfo())
			it.data.access_threshold = self._threshold

	def _init_state(self) -> 'GreedyDual.State':
		return GreedyDual.State(self._mode)

	def __init__(self, storage: Storage, mode: Mode=Mode.TOTAL_SIZE, state: Optional[State]=None):
		self._mode: Mode = mode
		super(GreedyDual, self).__init__(storage, state=state)
