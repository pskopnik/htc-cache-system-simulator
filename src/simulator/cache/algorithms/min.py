from apq import KeyedPQ
from typing import Iterable, Optional

from ...dstructures.accessseq import ReuseTimer
from ..state import AccessInfo, FileID, SimpleAccessReader, StateDrivenProcessor, StateDrivenOfflineProcessor


class MIN(StateDrivenOfflineProcessor):
	"""Processor performing Belady's MIN algorithm offline.

	The algorithm yields the best possible hit rate (uniform cost, uniform
	size caching problem). It does so by first computing the reuse index for
	each access, i.e. the first index in the accesses sequence when the file
	is accessed again. This is computed using ReuseTimer which iterates the
	accesses sequence backwards. The processor then evicts the file from the
	cache with the future-most reuse index. The next reuse index for each file
	in the cache is kept in a PQ and updated on access by querying the
	ReuseTimer.
	"""
	class State(StateDrivenOfflineProcessor.State):
		class Item(StateDrivenOfflineProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, reuse_timer: ReuseTimer) -> None:
			self._reuse_timer: ReuseTimer = reuse_timer
			self._pq: KeyedPQ[None] = KeyedPQ(max_heap=True)

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
				return MIN.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, MIN.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			val = self._reuse_timer.reuse_ind_inf(ind)
			self._pq.add_or_change_value(file, val, None)

	def _init_full_state(self, accesses: SimpleAccessReader) -> 'MIN.State':
		return MIN.State(ReuseTimer(accesses))
