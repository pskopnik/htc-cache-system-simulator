from apq import KeyedPQ
from array import array
import itertools
import math
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Union

from ..accesses import SimpleAccessReader
from ..processor import AccessInfo, Access
from ..state import FileID, StateDrivenProcessor, StateDrivenOfflineProcessor
from ..storage import Storage

class ReuseTimer(object):
	def __init__(self, accesses: SimpleAccessReader) -> None:
		self._reuse_ind: array[int] = self._build_reuse_ind(accesses)

	def reuse_time(self, ind: int) -> Optional[int]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return None

		return self._reuse_ind[ind] - ind

	def reuse_time_inf(self, ind: int) -> Union[int, float]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return math.inf

		return self._reuse_ind[ind] - ind

	def reuse_ind(self, ind: int) -> Optional[int]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return None

		return self._reuse_ind[ind]

	def reuse_ind_inf(self, ind: int) -> Union[int, float]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return math.inf

		return self._reuse_ind[ind]

	def reuse_ind_len(self, ind: int) -> int:
		return self._reuse_ind[ind]

	def verify(self, accesses: Sequence[Access]) -> None:
		for ind, reuse_ind in enumerate(self._reuse_ind):
			file = accesses[ind].file
			for i in range(ind + 1, reuse_ind):
				if accesses[i].file == file:
					raise Exception('Found earlier reuse ind', ind, i, reuse_ind, file)
			if reuse_ind != len(self._reuse_ind):
				if accesses[reuse_ind].file != file:
					raise Exception('Invalid reuse ind', reuse_ind, file)

	@staticmethod
	def _build_reuse_ind(accesses: SimpleAccessReader) -> 'array[int]':
		next_access: Dict[FileID, int] = {}
		accesses_length = len(accesses)
		reuse_time: array[int] = array('Q', itertools.repeat(0, accesses_length))

		for rev_ind, access in enumerate(reversed(accesses)):
			ind = accesses_length - rev_ind - 1
			reuse_time[ind] = next_access.get(access.file, accesses_length)
			next_access[access.file] = ind

		return reuse_time


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
			self._pq: KeyedPQ[None] = KeyedPQ(max_heap=True)
			self._reuse_timer: ReuseTimer = reuse_timer

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

		def process_access(
			self,
			file: FileID,
			ind: int = 0,
			ensure: bool = True,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> None:
			val = self._reuse_timer.reuse_ind_inf(ind)
			self._pq.add_or_change_value(file, val, None)

	def _init_full_state(self, accesses: SimpleAccessReader) -> 'MIN.State':
		return MIN.State(ReuseTimer(accesses))
