from apq import KeyedPQ
from dataclasses import dataclass, field
import itertools
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ...dstructures.binning import BinnedMapping, LogBinner
from ...dstructures.accessseq import ReuseTimer
from ..state import Access, AccessInfo, FileID, SimpleAccessReader, StateDrivenProcessor, StateDrivenOfflineProcessor, Storage


class OBMA(StateDrivenOfflineProcessor):
	"""Offline Bit Model Algorithm: MIN variant evicting across file sizes.

	OBMA assigns files to classes according to their file size. The class `j`
	contains all files with size greater equal `2^j` and less than `2^j(+1)`.
	The first class also contains all files of smaller size and the last class
	contains all files of greater size.

	The algorithm is configured by passing an instance of the
	OBMA.Configuration class to the constructor. The first_class and
	last_class parameters describe the `j` values of the first and last class.
	class_width is allows to coarsen the classes. A class_width of `k` leads
	to classes `j, ..., j + k - 1` being merged.

	OBMA evicts across all classes. If `b` bytes are required to be freed,
	OBMA marks `b` bytes from each class to be evicted. An eviction counter is
	maintained for each class. `b` is added to the eviction counter when bytes
	needs to be evicted. Files are only evicted from classes with files
	greater than `b` if the eviction counter reaches the size of the smallest
	file in the class. At least `b` bytes are always evicted from the
	smallest-size class containing files.
	"""

	@dataclass
	class Configuration(object):
		first_class: int = field(init=True, default=10)
		last_class: int = field(init=True, default=40)
		class_width: int = field(init=True, default=2)

	class State(StateDrivenOfflineProcessor.State):
		@dataclass
		class _FileInfo(object):
			size: int = field(init=True)

		class _Class(object):
			__slots__ = ['pq', 'total_size', 'eviction_counter']

			def __init__(self) -> None:
				# PQ of cached files in this class
				self.pq: KeyedPQ[OBMA.State._FileInfo] = KeyedPQ(max_heap=True)
				# Total size of cached files in this class
				self.total_size: int = 0
				# Counts bytes to be evicted (eviction is delayed until this
				# counter surpasses the eviction candidate's size)
				self.eviction_counter: int = 0

			def pop_file(self) -> Tuple[FileID, 'OBMA.State._FileInfo']:
				file, _, info = self.pq.pop()
				self.total_size -= info.size
				return file, info

			def remove_file(self, file: FileID) -> None:
				it = self.pq[file]
				self.total_size -= it.data.size
				del self.pq[it]

			def add_file(self, file: FileID, size: int, reuse_ind: Union[int, float]) -> None:
				info = OBMA.State._FileInfo(size)
				self.pq.add(file, reuse_ind, info)
				self.total_size += size

			def update_file(self, file: FileID, size: int, reuse_ind: Union[int, float]) -> None:
				info = self.pq[file].data
				self.pq.change_value(file, reuse_ind)
				self.total_size += - info.size + size
				info.size = size

		class Item(StateDrivenOfflineProcessor.State.Item):
			def __init__(self, file: FileID, size: int) -> None:
				self._file: FileID = file
				self._size: int = size

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, reuse_timer: ReuseTimer, configuration: 'OBMA.Configuration') -> None:
			self._reuse_timer: ReuseTimer = reuse_timer

			self._classes: BinnedMapping[OBMA.State._Class] = BinnedMapping(
				LogBinner(
					first = configuration.first_class,
					last = configuration.last_class,
					step = configuration.class_width,
				),
				OBMA.State._Class,
			)

		def _round_up_to_evict(self, required_free_bytes: int) -> int:
			classes_before = self._classes.values_until(required_free_bytes, half_open=False)
			if sum(clas.total_size for clas in classes_before) < required_free_bytes:
				for clas in self._classes.values_from(required_free_bytes, half_open=True):
					if len(clas.pq) > 0:
						return clas.pq.peek().data.size
				raise Exception('Not enough space in cache to fit file')
			else:
				return required_free_bytes

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
			candidates: List[FileID] = []

			to_evict_bytes = self._round_up_to_evict(required_free_bytes)

			for clas in self._classes.values_until(required_free_bytes, half_open=False):
				# clas contains files smaller equal to required_free_bytes.
				# Multiple files have to be evicted from clas. Leave
				# eviction_counter in tact as that is the most sensible
				# operation.
				evicted_bytes = 0
				while len(clas.pq) > 0 and evicted_bytes < to_evict_bytes:
					candidate, info = clas.pop_file()
					evicted_bytes += info.size
					candidates.append(candidate)

			for clas in self._classes.values_from(required_free_bytes, half_open=True):
				# clas contains files greater than required_free_bytes.
				# Update the eviction_counter and evict the candidate file
				# (greatest reuse distance) if the eviction_counter exceeds its
				# size.
				clas.eviction_counter += to_evict_bytes
				while len(clas.pq) > 0 and clas.eviction_counter > clas.pq.peek().data.size:
					candidate, info = clas.pop_file()
					clas.eviction_counter -= info.size
					candidates.append(candidate)

			return candidates

		def find(self, file: FileID) -> Optional[Item]:
			for cost, clas in self._classes.items():
				if file in clas.pq:
					return OBMA.State.Item(file, cost)

			return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, OBMA.State.Item):
				raise TypeError('unsupported item type passed')

			del self._classes[item._size].pq[item._file]

		def remove_file(self, file: FileID) -> None:
			item = self.find(file)
			if item is None:
				raise KeyError(f'{file!r} not in OBMA.State')

			self.remove(item)

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			old_size = info.total_bytes - info.bytes_added
			new_size = info.total_bytes
			moved = self._classes.binner(old_size) != self._classes.binner(new_size)

			reuse_ind = self._reuse_timer.reuse_ind_inf(ind)

			if old_size == 0:
				self._classes[new_size].add_file(file, info.total_bytes, reuse_ind)
			elif moved:
				self._classes[old_size].remove_file(file)
				self._classes[new_size].add_file(file, info.total_bytes, reuse_ind)
			else:
				self._classes[new_size].update_file(file, info.total_bytes, reuse_ind)

	def __init__(
		self,
		configuration: 'OBMA.Configuration',
		storage: Storage,
		state: Optional[State] = None,
	) -> None:
		self._configuration: OBMA.Configuration = configuration
		super(OBMA, self).__init__(storage, state=state)

	def _init_full_state(self, accesses: SimpleAccessReader) -> 'OBMA.State':
		return OBMA.State(ReuseTimer(accesses), self._configuration)
