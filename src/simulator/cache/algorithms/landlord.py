from apq import KeyedItem, KeyedPQ
from dataclasses import dataclass, field
from enum import auto, Enum
from typing import Iterable, Optional

from ..state import AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor, Storage


class Mode(Enum):
	TOTAL_SIZE = auto()
	ACCESS_SIZE = auto()
	FETCH_SIZE = auto()
	ADD_FETCH_SIZE = auto()
	NO_COST = auto()


class Landlord(StateDrivenOnlineProcessor):
	"""Processor evicting the file with the lowest "credit" per volume.

	Volume refers to the space of the cache's storage medium taken up, i.e.
	the size of the cached fraction of the file (referred to as the "total
	cached size"). The credit per volume is considered for eviction decisions.

	Landlord evicts the file with the lowest credit per volume. This value is
	deducted from the credit per volume of all files remaining in the cache.

	The Landlord processor can run in several modes. The mode determines how a
	file's credit is updated on re-access. Initially the credit is set to the
	cost of fetching the file, i.e. the total cached size of the file. Note
	this means the initial credit per volume is always 1. During its lifetime
	in the cache, the credit per volume decreases when other files are evicted
	(see above). When a file in the cache is accessed again its credit is
	increased up to its total cached size. A file's credit is never reduced on
	re-access. If a mode would reduced a file's credit, the credis is left
	unchanged instead.

	TOTAL_SIZE - The credit is set to the total cached size of the file. This
	emulates LRU.

	ACCESS_SIZE - The credit is set to the size of the accessed fraction of
	the file.

	FETCH_SIZE - The credit is set to the size of the newly fetched fraction
	of the file caused by the access.

	ADD_FETCH_SIZE - The fetched size is added onto the current credit.

	NO_COST - A file's credit is never increased on re-access. This almost
	emulates FIFO, but not quite, as the volume credit decreases whenever an
	additional fraction of the file is fetched.

	Landlord is a generalisation of many strategies, including FIFO, LRU,
	GreedyDual and GreedyDual-Size.
	"""

	class State(StateDrivenProcessor.State):
		@dataclass
		class _FileInfo(object):
			size: int = field(init=True)
			access_rent_threshold: float = field(init=True)

		class Item(StateDrivenProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, mode: Mode) -> None:
			self._mode: Mode = mode
			self._pq: KeyedPQ[Landlord.State._FileInfo] = KeyedPQ()
			self._rent_threshold: float = 0.0

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
			file, running_volume_credit, _ = self._pq.pop() # Raises IndexError if empty
			self._rent_threshold = running_volume_credit
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return Landlord.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, Landlord.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			del self._pq[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			it: Optional[KeyedItem[Landlord.State._FileInfo]]
			current_credit: float
			try:
				it = self._pq[file]

				current_credit = (it.value - self._rent_threshold) * it.data.size
			except KeyError:
				it = None

				current_credit = 0.0

			total_bytes = info.total_bytes

			credit = self._credit(
				requested_bytes = info.bytes_requested,
				placed_bytes = info.bytes_added,
				total_bytes = total_bytes,
				current_credit = current_credit,
			)
			running_volume_credit = credit / total_bytes + self._rent_threshold

			if it is None:
				it = self._pq.add(file, running_volume_credit, Landlord.State._FileInfo(0, 0.0))
			else:
				self._pq.change_value(it, running_volume_credit)

			it.data.size = total_bytes
			it.data.access_rent_threshold = self._rent_threshold

		def _credit(
			self,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
			current_credit: float = 0.0,
		) -> float:
			mode = self._mode
			if mode is Mode.TOTAL_SIZE:
				return float(total_bytes)
			elif mode is Mode.ACCESS_SIZE:
				return max(current_credit, float(requested_bytes))
			elif mode is Mode.FETCH_SIZE:
				return max(current_credit, float(placed_bytes))
			elif mode is Mode.ADD_FETCH_SIZE:
				return current_credit + float(placed_bytes)
			elif mode is Mode.NO_COST:
				if current_credit == 0.0:
					return float(total_bytes)
				else:
					return current_credit

			raise NotImplementedError

	def _init_state(self) -> 'Landlord.State':
		return Landlord.State(self._mode)

	def __init__(self, storage: Storage, mode: Mode=Mode.TOTAL_SIZE, state: Optional[State]=None):
		self._mode: Mode = mode
		super(Landlord, self).__init__(storage, state=state)
