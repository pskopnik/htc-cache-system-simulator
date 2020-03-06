from apq import KeyedItem, KeyedPQ
from dataclasses import dataclass, field
import functools
import itertools
import math
from typing import Callable, cast, Iterable, Optional, Union

from ...dstructures.accessseq import ReuseTimer
from ...dstructures.binning import BinnedMapping, LogBinner
from ...dstructures.sorted import SortedDefaultDict
from ..state import AccessInfo, FileID, SimpleAccessReader, StateDrivenProcessor, StateDrivenOfflineProcessor, Storage


class MIND(StateDrivenOfflineProcessor):
	"""MIN-d is a cost-aware modification of the offline MIN algorithm.

	MIN-d evicts the file with the lowest cost (i.e. size) among the `d` files
	with the longest reuse distance. `d` is configurable by passing an
	instance of the MIND.Configuration class to the constructor. d_factor
	describes the fraction of files in the cache which are considered:
	`d = d_factor * cached_files`. Additionally, min_d and max_d may
	optionally be set to ensure that `d` is respects the minimum and / or
	maximum.
	"""
	@dataclass
	class Configuration(object):
		d_factor: float = field(init=True)
		min_d: Optional[int] = field(init=True, default=None)
		max_d: Optional[int] = field(init=True, default=None)

	class State(StateDrivenOfflineProcessor.State):
		@dataclass
		class _FileInfo(object):
			size: int = field(init=True)

		class Item(StateDrivenOfflineProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, reuse_timer: ReuseTimer, configuration: 'MIND.Configuration') -> None:
			self._reuse_timer: ReuseTimer = reuse_timer
			self._d_factor: float = configuration.d_factor
			self._min_d: Optional[int] = configuration.min_d
			self._max_d: Optional[int] = configuration.max_d
			self._pq: KeyedPQ[MIND.State._FileInfo] = KeyedPQ(max_heap=True)

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
			min_item = self._pq.peek() # Raises IndexError if empty
			min_cost = min_item.data.size

			d = len(self._pq)
			if self._min_d is not None:
				d = max(self._min_d, d)
			if self._max_d is not None:
				d = min(self._max_d, d)

			for item in itertools.islice(self._pq.ordered_iter(), d):
				if item.data.size < min_cost:
					min_item = item
					min_cost = item.data.size

			file = min_item.key
			del self._pq[min_item]
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return MIND.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, MIND.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			val = self._reuse_timer.reuse_ind_inf(ind)

			try:
				item = self._pq[file]
				item.data.size = info.total_bytes
				self._pq.change_value(item, val)
			except KeyError:
				self._pq.add(file, val, MIND.State._FileInfo(info.total_bytes))

	def __init__(
		self,
		configuration: 'MIND.Configuration',
		storage: Storage,
		state: Optional[State] = None,
	) -> None:
		self._configuration: MIND.Configuration = configuration
		super(MIND, self).__init__(storage, state=state)

	def _init_full_state(self, accesses: SimpleAccessReader) -> 'MIND.State':
		return MIND.State(ReuseTimer(accesses), self._configuration)


class MINCod(StateDrivenOfflineProcessor):
	"""MIN-cod is a cost-aware modification of the offline MIN algorithm.

	MIN-cod evicts the file with the smallest cost over distance (cod) value.
	That is `cost / reuse_distance`. Cost is the fetch-cost, i.e. the size of
	the file.

	MIN-cod collects files of similar cost in the same PQ. This behaviour can
	be configured by passing a MINCod.Configuration instance to the
	constructor. If classes is False (the default), each cost value results in
	a separate PQ. If there are too many unique cost values, classes has to be
	enabled. An exponential binning is used when classes is enabled. Each file
	is assigned to class `j` where `2^j <= file_size < 2^(j+1)`. The first
	class also contains all files of smaller size and the last class contains
	all files of greater size. The first_class and last_class parameters
	describe the `j` values of the first and last class. class_width is allows
	to coarsen the classes. A class_width of `k` leads to classes
	`j, ..., j + k - 1` being merged.
	"""
	@dataclass
	class Configuration(object):
		classes: bool = field(init=True, default=False)
		first_class: int = field(init=True, default=10)
		last_class: int = field(init=True, default=40)
		class_width: int = field(init=True, default=2)

	class State(StateDrivenOfflineProcessor.State):
		@dataclass
		class _FileInfo(object):
			size: int = field(init=True)

		class Item(StateDrivenOfflineProcessor.State.Item):
			def __init__(self, file: FileID, size: int) -> None:
				self._file: FileID = file
				self._size: int = size

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, reuse_timer: ReuseTimer, configuration: 'MINCod.Configuration') -> None:
			self._reuse_timer: ReuseTimer = reuse_timer

			self._pop_eviction_candidate: Callable[[], FileID]
			self._process_access: Callable[[FileID, int, AccessInfo], None]
			self._find: Callable[[FileID], Optional[MINCod.State.Item]]
			self._remove: Callable[[MINCod.State.Item], None]

			binner = LogBinner()

			if configuration.classes:
				binner = LogBinner(
					first = configuration.first_class,
					last = configuration.last_class,
					step = configuration.class_width,
				)
				self._pop_eviction_candidate = self._pop_eviction_candidate_using_classes
				self._process_access = self._process_access_using_classes
				self._find = self._find_using_classes
				self._remove = self._remove_using_classes
			else:
				self._pop_eviction_candidate = self._pop_eviction_candidate_using_sorted_dict
				self._process_access = self._process_access_using_sorted_dict
				self._find = self._find_using_sorted_dict
				self._remove = self._remove_using_sorted_dict

			self._sorted_dict: SortedDefaultDict[int, KeyedPQ[MINCod.State._FileInfo]] = SortedDefaultDict(
				cast(
					'Callable[[], KeyedPQ[MINCod.State._FileInfo]]',
					functools.partial(KeyedPQ, max_heap=True),
				),
			)
			self._classes: BinnedMapping[KeyedPQ[MINCod.State._FileInfo]] = BinnedMapping(
				binner,
				cast(
					'Callable[[], KeyedPQ[MINCod.State._FileInfo]]',
					functools.partial(KeyedPQ, max_heap=True),
				),
			)

		def _pop_eviction_candidate_using_classes(self) -> FileID:
			min_item: Optional[KeyedItem[MINCod.State._FileInfo]] = None
			min_pq: Optional[KeyedPQ[MINCod.State._FileInfo]] = None
			min_cod: Union[int, float] = math.inf

			for pq_min_cost, pq in self._classes.items():
				if len(pq) == 0:
					continue

				# Alternatively, to avoid the overhead of ordered iteration,
				# the PQs could be iterated using the values_until
				# operation. Before starting to iterate over a PQ, the maximum
				# reuse index is calculated which could lead to a new minimum
				# cod. That is pq_min_cost / min_cod. Derived from the
				# condition "stop searching PQ as soon as
				# pq_min_cost / pq_max_future_reuse_ind >= min_cod".
				#
				# An evaluation of this loop has shown that the cyclic access
				# pattern leads to bins with a low spread of cost and runs of
				# consecutive reuse indices. In such cases the bin has to be
				# iterated completely, as the item.value hardly decreases.
				#
				# The first PQ would be iterated completely without order, as
				# min_cod is unset before starting iteration. Even if
				# peeking a first item and setting min_cod, the pq_min_cost
				# would still be 0 and thus not lead to a sensible lower
				# bound for the maximum reuse index.

				for item in pq.ordered_iter():
					# item.value is the maximum reuse index of all future
					# items about to be visited by the ordered iterator.

					cod = item.data.size / item.value
					if cod < min_cod:
						min_item = item
						min_pq = pq
						min_cod = cod

					if pq_min_cost / item.value >= min_cod:
						break

			if min_item is None or min_pq is None:
				raise Exception('Not enough space in cache to fit file')

			file = min_item.key
			del min_pq[min_item]

			return file

		def _pop_eviction_candidate_using_sorted_dict(self) -> FileID:
			min_item: Optional[KeyedItem[MINCod.State._FileInfo]] = None
			min_pq_cost: Optional[int] = None
			min_pq: Optional[KeyedPQ[MINCod.State._FileInfo]] = None
			min_cod: Union[int, float] = math.inf

			for pq_cost, pq in self._sorted_dict.items():
				item = pq.peek() # there should be no empty PQs!
				cod = item.data.size / item.value
				if cod < min_cod:
					min_item = item
					min_pq_cost = pq_cost
					min_pq = pq
					min_cod = cod

			if min_item is None or min_pq_cost is None or min_pq is None:
				raise Exception('Not enough space in cache to fit file')

			file = min_item.key
			del min_pq[min_item]
			if len(min_pq) == 0:
				del self._sorted_dict[min_pq_cost]

			return file

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
			return (self._pop_eviction_candidate(),)

		def _find_using_classes(self, file: FileID) -> Optional[Item]:
			for pq in self._classes.values():
				if file in pq:
					return MINCod.State.Item(file, pq[file].data.size)

			return None

		def _find_using_sorted_dict(self, file: FileID) -> Optional[Item]:
			for pq in self._sorted_dict.values():
				if file in pq:
					return MINCod.State.Item(file, pq[file].data.size)

			return None

		def find(self, file: FileID) -> Optional[Item]:
			return self._find(file)

		def _remove_using_classes(self, item: 'MINCod.State.Item') -> None:
			del self._classes[item._size][item._file]

		def _remove_using_sorted_dict(self, item: 'MINCod.State.Item') -> None:
			del self._sorted_dict[item._size][item._file]

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, MINCod.State.Item):
				raise TypeError('unsupported item type passed')

			self._remove(item)

		def remove_file(self, file: FileID) -> None:
			item = self._find(file)
			if item is None:
				raise KeyError(f'{file!r} not in MINCod.State')

			self._remove(item)

		def _process_access_using_classes(self, file: FileID, ind: int, info: AccessInfo) -> None:
			old_size = info.total_bytes - info.bytes_added
			new_size = info.total_bytes
			moved = self._classes.binner(old_size) != self._classes.binner(new_size)

			reuse_ind = self._reuse_timer.reuse_ind_inf(ind)

			if old_size == 0:
				self._classes[new_size].add(file, reuse_ind, MINCod.State._FileInfo(info.total_bytes))
			elif moved:
				old_pq = self._classes[old_size]
				item = old_pq[file]
				file_info = item.data
				del old_pq[item]
				file_info.size = info.total_bytes
				self._classes[new_size].add(file, reuse_ind, file_info)
			else:
				pq = self._classes[new_size]
				item = pq[file]
				item.data.size = info.total_bytes
				pq.change_value(item, reuse_ind)

		def _process_access_using_sorted_dict(self, file: FileID, ind: int, info: AccessInfo) -> None:
			old_size = info.total_bytes - info.bytes_added
			new_size = info.total_bytes
			moved = old_size != new_size

			reuse_ind = self._reuse_timer.reuse_ind_inf(ind)

			if old_size == 0:
				self._sorted_dict[new_size].add(file, reuse_ind, MINCod.State._FileInfo(info.total_bytes))
			elif moved:
				old_pq = self._sorted_dict[old_size]
				item = old_pq[file]
				file_info = item.data
				del old_pq[item]
				if len(old_pq) == 0:
					del self._sorted_dict[old_size]
				file_info.size = info.total_bytes
				self._sorted_dict[new_size].add(file, reuse_ind, file_info)
			else:
				pq = self._sorted_dict[new_size]
				item = pq[file]
				item.data.size = info.total_bytes
				pq.change_value(item, reuse_ind)

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			self._process_access(file, ind, info)

	def __init__(
		self,
		configuration: 'MINCod.Configuration',
		storage: Storage,
		state: Optional[State] = None,
	) -> None:
		self._configuration: MINCod.Configuration = configuration
		super(MINCod, self).__init__(storage, state=state)

	def _init_full_state(self, accesses: SimpleAccessReader) -> 'MINCod.State':
		return MINCod.State(ReuseTimer(accesses), self._configuration)
