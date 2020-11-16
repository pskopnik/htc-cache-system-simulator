from apq import KeyedItem, KeyedPQ
from array import array
from dataclasses import dataclass, field
from enum import auto, Enum
import itertools
from typing import (
	Callable,
	cast,
	DefaultDict,
	Dict,
	Hashable,
	Iterable,
	Optional,
	Reversible,
	Tuple,
	TypeVar,
)

from ..classification import Classifier, Combine, Constant, DirectoryName
from ..state import AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor, Storage
from ...dstructures.binning import Binner, LinearBinner, LogBinner
from ...dstructures.histogram import BinnedCounters, BinnedFloats
from ...params import parse_user_args, SimpleField
from ...workload import Access


_T_num = TypeVar('_T_num', int, float)

_classifier_dict: Dict[str, Callable[[], Classifier]] = {
	'constant': lambda: Constant(''),
	'dataset': lambda: DirectoryName(from_root=0),
	'dirname': lambda: DirectoryName(from_file=0),
}

def classifier_from_user_arg(user_arg: str) -> Classifier:
	if '&' in user_arg:
		return Combine(_classifier_dict[name]() for name in user_arg.split('&'))
	else:
		return _classifier_dict[user_arg]()


class _ReusedClassifier(object):
	class _Reused(Enum):
		NONREUSED = auto()
		REUSED = auto()

	def __init__(self, state: 'EVA.State') -> None:
		self._state = state

	def __call__(self, access: Access) -> Hashable:
		try:
			return self._from_file_info(self._state._pq[access.file].data)
		except KeyError:
			return _ReusedClassifier._Reused.NONREUSED

	def _from_file_info(self, info: 'EVA.State._FileInfo') -> Hashable:
		if info.reused:
			return _ReusedClassifier._Reused.REUSED
		else:
			return _ReusedClassifier._Reused.NONREUSED

	@staticmethod
	def is_reused(clas: Hashable) -> bool:
		if isinstance(clas, tuple) and len(clas) == 2:
			typed_clas = cast('Tuple[_ReusedClassifier._Reused, Hashable]', clas)
			return typed_clas[0] is _ReusedClassifier._Reused.REUSED

		return False

	@staticmethod
	def is_non_reused(clas: Hashable) -> bool:
		if isinstance(clas, tuple) and len(clas) == 2:
			typed_clas = cast('Tuple[_ReusedClassifier._Reused, Hashable]', clas)
			return typed_clas[0] is _ReusedClassifier._Reused.NONREUSED

		return False

	@staticmethod
	def to_reused(clas: Hashable) -> Hashable:
		if isinstance(clas, tuple) and len(clas) == 2:
			typed_clas = cast('Tuple[_ReusedClassifier._Reused, Hashable]', clas)
			return _ReusedClassifier._Reused.REUSED, typed_clas[1]

		raise ValueError('unexpected value passed in')

	@staticmethod
	def to_non_reused(clas: Hashable) -> Hashable:
		if isinstance(clas, tuple) and len(clas) == 2:
			typed_clas = cast('Tuple[_ReusedClassifier._Reused, Hashable]', clas)
			return _ReusedClassifier._Reused.NONREUSED, typed_clas[1]

		raise ValueError('unexpected value passed in')


class EVA(StateDrivenOnlineProcessor):
	"""Processor evicting the file with the lowest estimated economic value.

	"""

	@dataclass
	class Configuration(object):
		classifier: Classifier = field(init=True, default_factory=lambda: Constant(""))
		age_bin_width: int = field(init=True, default=3*24*60*60)
		ewma_factor: float = field(init=True, default=0.0088)
		eva_computation_interval: int = field(init=True, default=10000)

		@classmethod
		def from_user_args(cls, user_args: str) -> 'EVA.Configuration':
			inst = cls()
			parse_user_args(user_args, inst, [
				SimpleField('classifier', classifier_from_user_arg),
				SimpleField('age_bin_width', int),
				SimpleField('ewma_factor', float),
				SimpleField('eva_computation_interval', int),
			])
			return inst

	class State(StateDrivenProcessor.State):
		class _FileInfo(object):
			__slots__ = [
				'size',
				'first_access_ts',
				'last_access_ts',
				'file_class',
				'reused',
			]

			def __init__(self, size: int, access_ts: int, file_class: Hashable) -> None:
				self.size: int = size
				self.first_access_ts: int = access_ts
				self.last_access_ts: int = access_ts
				self.file_class = file_class
				self.reused = False

		class _ClassInfo(object):
			__slots__ = [
				'binner',
				'hit_counters',
				'eviction_counters',
				'durable_hit_counters',
				'durable_eviction_counters',
				'evas',
			]

			def __init__(self, binner: Binner) -> None:
				self.binner: Binner = binner
				self.hit_counters: BinnedCounters = BinnedCounters(binner)
				self.eviction_counters: BinnedCounters = BinnedCounters(binner)
				self.durable_hit_counters: BinnedCounters = BinnedCounters(binner)
				self.durable_eviction_counters: BinnedCounters = BinnedCounters(binner)
				self.evas: BinnedFloats = BinnedFloats(binner)

			def record_hit(self, age: int) -> None:
				self.hit_counters.increment(age)

			def record_eviction(self, age: int) -> None:
				self.eviction_counters.increment(age)

			def get_eva(self, age: int) -> float:
				return self.evas[age]

		class Item(StateDrivenProcessor.State.Item):
			def __init__(self, file: FileID) -> None:
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, configuration: 'EVA.Configuration') -> None:
			self._classifier: Classifier = Combine((
				_ReusedClassifier(self), configuration.classifier,
			))
			self._age_bin_width: int = configuration.age_bin_width
			self._ewma_factor: float = configuration.ewma_factor
			self._eva_computation_interval: int = configuration.eva_computation_interval

			self._pq: KeyedPQ[EVA.State._FileInfo] = KeyedPQ()

			self._age_binner: Binner = LinearBinner(width=self._age_bin_width)
			self._class_infos: DefaultDict[Hashable, EVA.State._ClassInfo] = DefaultDict(
				lambda: EVA.State._ClassInfo(self._age_binner),
			)
			self._accesses_since_eva_computation: int = 0
			self._last_eva_computation_ts: int = 0
			self._last_age_bin: int = 0

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
			file, _, info = self._pq.pop() # Raises IndexError if empty
			age = ts - info.last_access_ts
			self._class_infos[info.file_class].record_eviction(age)
			# self._accesses_since_eva_computation += 1 # TODO: only accesses or all events? paper uses accesses
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return EVA.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, EVA.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			# The file is not recorded in any counters, as if it never entered the cache
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			size = info.total_bytes
			ts = info.access.access_ts

			file_class = self._classifier(info.access)

			it: Optional[KeyedItem[EVA.State._FileInfo]]
			file_info: EVA.State._FileInfo
			try:
				it = self._pq[file]
				file_info = it.data

				self._class_infos[file_info.file_class].record_hit(ts - file_info.last_access_ts)

				file_info.size = size
				file_info.last_access_ts = ts
				file_info.file_class = file_class
				file_info.reused = True
			except KeyError:
				it = None
				file_info = EVA.State._FileInfo(size, ts, file_class)

			eva = self._class_infos[file_info.file_class].evas[ts - file_info.last_access_ts]

			if it is None:
				it = self._pq.add(file, eva, file_info)
			else:
				self._pq.change_value(it, eva)

			self._accesses_since_eva_computation += 1

			if self._accesses_since_eva_computation >= self._eva_computation_interval:
				self._compute_evas(ts)
				self._set_priorities(ts)
			elif self._age_binner(ts) != self._last_age_bin:
				self._set_priorities(ts)

		def _set_priorities(self, ts: int) -> None:
			old_pq = self._pq
			class_infos = self._class_infos
			self._pq = KeyedPQ(
				(
					it.key,
					class_infos[it.data.file_class].evas[ts - it.data.last_access_ts],
					it.data,
				)
				for it in old_pq
			)
			self._last_age_bin = self._age_binner(ts)

		def _compute_evas(self, ts: int) -> None:
			# TODO: de-value very old files?
			# TODO: scouts? I.e. a small fraction of elements which survive
			# or a long time to explore whether they are reaccessed in the far
			# future. Or ghosts? I.e. similar to scouts, but only meta-data is
			# stored and the data is evicted regularly.

			# TODO: choose _eva_computation_interval dynamically? Similar to age
			# granularity? Should depend on the depth of the age histogram.

			# Update durable counters and compute per-class and total hit rates

			total_hits: int = 0
			total_accesses: int = 0

			class_hit_rates: Dict[Hashable, array[float]] = {}

			for clas, info in self._class_infos.items():
				info.durable_hit_counters.update(info.hit_counters, self._ewma_factor)
				info.durable_eviction_counters.update(info.eviction_counters, self._ewma_factor)
				# print(clas, {
				# 	"info.hit_counters.bin_data": info.hit_counters.bin_data,
				# 	"info.eviction_counters.bin_data": info.eviction_counters.bin_data,
				# 	"info.durable_hit_counters.bin_data": info.durable_hit_counters.bin_data,
				# 	"info.durable_eviction_counters.bin_data": info.durable_eviction_counters.bin_data,
				# })
				info.hit_counters.reset()
				info.eviction_counters.reset()

				class_hit_rates[clas] = reversed_array('d', map(
					lambda x: lenient_div(x[0], (x[0] + x[1])),
					itertools.accumulate(
						zip_longest_reversed_arrays(
							info.durable_hit_counters.bin_data,
							info.durable_eviction_counters.bin_data,
						),
						lambda a, b: (a[0] + b[0], a[1] + b[1]), # functools.partial(map, operator.plus)
					),
				))

				total_hits += info.durable_hit_counters.total
				total_accesses += info.durable_hit_counters.total + info.durable_eviction_counters.total

			total_hit_rate: float = lenient_div(total_hits, total_accesses) # TODO: might be zero because EWMA leads to events disappearing

			avg_cache_size: int = len(self._pq) # TODO: this is a very rough estimate

			per_access_cost = total_hit_rate / avg_cache_size

			# Calculate per-class EVAs

			time_interval = ts - self._last_eva_computation_ts # TODO: might be zero
			# TODO: is this correct or is it the avg accesses during a age_bin_width of time?
			per_bin_avg_accesses = self._age_bin_width * total_accesses / time_interval
			# average benefit of a bin, this is incorporated into the cost for the EVA of a specific class
			per_bin_cost = per_access_cost * per_bin_avg_accesses
			print({
				"time_interval": time_interval,
				"ts": ts,
				"self._last_eva_computation_ts": self._last_eva_computation_ts,
				"self._age_bin_width": self._age_bin_width,
				"total_accesses": total_accesses,
			})

			for clas, info in self._class_infos.items():
				l = max(
					len(info.durable_hit_counters.bin_data),
					len(info.durable_eviction_counters.bin_data),
				)
				last_cumulative_lifetime = (
						array_get(info.durable_hit_counters.bin_data, l - 1)
					+
						array_get(info.durable_eviction_counters.bin_data, l - 1)
				)

				# TODO: interpolation within the bin?
				# TODO: OR use center of the bin for calculations.

				info.evas.set_bin_data(reversed_array('d', map(
					# (hits - per_bin_cost * cumulative_lifetimes) / (hits + evictions)
					lambda x: lenient_div((x[1] - per_bin_cost * x[0]), (x[1] + x[2])),
					# Accumulates: (cumulative_lifetimes, cumulative_hits, cumulative_evictions)
					# in reverse bin order.
					# cumulative_lifetimes is the sum of all future lifetimes. Divide this by the
					# number of future events to get the expected lifetime.
					# cumulative_hits is the number of all future hits.
					# cumulative_evictions is the number of all future evictions.
					itertools.accumulate(
						zip_longest_reversed_arrays(
							array('Q', [0] * (l - 1) + [last_cumulative_lifetime]),
							info.durable_hit_counters.bin_data,
							info.durable_eviction_counters.bin_data,
						),
						lambda a, b: (a[0] + a[1] + a[2] + b[1] + b[2], a[1] + b[1], a[2] + b[2]),
					),
				)))

				print({
					'clas': clas,
					'per_bin_cost': per_bin_cost,
					'hits': info.durable_hit_counters.bin_data,
					'evictions': info.durable_eviction_counters.bin_data,
					'l': l,
					'last_cumulative_lifetime': last_cumulative_lifetime,
					'accumulation': list(itertools.accumulate(
						zip_longest_reversed_arrays(
							array('Q', [0] * (l - 1) + [last_cumulative_lifetime]),
							info.durable_hit_counters.bin_data,
							info.durable_eviction_counters.bin_data,
						),
						lambda a, b: (a[0] + a[1] + a[2] + b[1] + b[2], a[1] + b[1], a[2] + b[2]),
					)),
					'simple_accumulation': list(itertools.accumulate(
						zip_longest_reversed_arrays(
							info.durable_hit_counters.bin_data,
							info.durable_eviction_counters.bin_data,
						),
						lambda a, b: (a[0] + b[0], a[1] + b[1]),
					)),
					'evas': info.evas.bin_data,
				})

			# Apply 'reused' bias

			for clas, info in self._class_infos.items():
				reused_class = _ReusedClassifier.to_reused(clas)
				bias: float
				if (
					reused_class in self._class_infos and
					len(class_hit_rates[reused_class]) > 0 and
					class_hit_rates[reused_class][0] != 1.0
				):
					bias = (
						self._class_infos[reused_class].evas[0] /
						(1.0 - class_hit_rates[reused_class][0])
					)
				else:
					continue

				for bin_edge, class_hit_rate in zip(info.evas, class_hit_rates[clas]):
					info.evas[bin_edge] += (class_hit_rate - total_hit_rate) * bias

			# Reset counters

			self._accesses_since_eva_computation = 0
			self._last_eva_computation_ts = ts

	def _init_state(self) -> 'EVA.State':
		return EVA.State(self._configuration)

	def __init__(
		self,
		configuration: 'EVA.Configuration',
		storage: Storage,
		state: Optional[State] = None,
	):
		self._configuration: EVA.Configuration = configuration
		super(EVA, self).__init__(storage, state=state)


def array_get(a: 'array[int]', ind: int, default: int=0) -> int:
	try:
		return a[ind]
	except IndexError:
		return default

def zip_longest_reversed_arrays(
	*arrays: 'array[int]',
	fillvalue: int = 0,
) -> Iterable[Tuple[int, ...]]:
	l = max(map(len, arrays))
	return zip(
		*(itertools.chain(itertools.repeat(fillvalue, l - len(a)), reversed(a)) for a in arrays)
	)

def reversed_array(code: str, it: Iterable[_T_num]) -> 'array[_T_num]':
	out: 'array[_T_num]'

	if isinstance(it, array) and code == it.typecode:
		out = array(code, it)
		out.reverse()
		return out

	elif isinstance(it, Reversible):
		return array(code, reversed(it))

	else:
		out = array(code, it)
		out.reverse()
		return out

def lenient_div(dividend: float, divisor : float) -> float:
	try:
		return dividend / divisor
	except ZeroDivisionError as e:
		if dividend == 0.0:
			return 0.0
		else:
			raise e
