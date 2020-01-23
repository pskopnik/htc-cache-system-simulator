from apq import KeyedPQ, Item
from dataclasses import dataclass, field
from ..state import FileID, StateDrivenProcessor, StateDrivenOnlineProcessor
from ..storage import Storage
from typing import Iterable, Optional
from enum import auto, Enum


class Mode(Enum):
	TOTAL_COST = auto()
	ACCESS_COST = auto()
	FETCH_COST = auto()
	ADD_FETCH_COST = auto()
	NO_COST = auto()


class Landlord(StateDrivenOnlineProcessor):
	"""Processor evicting the least recently accessed file from the cache.
	"""

	class State(StateDrivenProcessor.State):
		@dataclass
		class _FileInfo(object):
			size: float = field(init=True)
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
			file: FileID = "",
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			free_bytes: int = 0,
			required_bytes: int = 0,
		) -> Iterable[FileID]:
			file, running_volume_credit, _ = self._pq.pop()
			self._rent_threshold = running_volume_credit
			return (file,)

		def find(self, file: FileID) -> Optional[Item]:
			if file in self._pq:
				return Landlord.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, Landlord.State.Item):
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
			it: Optional[Item[Landlord.State._FileInfo]]
			try:
				it = self._pq[file]
			except KeyError:
				it = None

			credit_base = self._credit_base(
				requested_bytes = requested_bytes,
				placed_bytes = placed_bytes,
				total_bytes = total_bytes,
			)
			credit = credit_base / total_bytes + self._rent_threshold

			if it is None:
				it = self._pq.add(file, credit, Landlord.State._FileInfo(0, 0.0))
			else:
				self._pq.change_value(it, credit)

			it.data.access_rent_threshold = self._rent_threshold
			it.data.size = total_bytes

		def _credit_base(
			self,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> float:
			mode = self._mode
			if mode is Mode.TOTAL_COST:
				return float(total_bytes)
			elif mode is Mode.ACCESS_COST:
				return float(requested_bytes)
			elif mode is Mode.FETCH_COST:
				return float(placed_bytes)
			elif mode is Mode.ADD_FETCH_COST:
				# return min(current_credit + placed_bytes, initial_credit)
				pass
			elif mode is Mode.NO_COST:
				# return float(current_credit)
				pass

			raise NotImplementedError

	def _init_state(self) -> 'Landlord.State':
		return Landlord.State(self._mode)

	def __init__(self, storage: Storage, mode: Mode=Mode.TOTAL_COST, state: Optional[State]=None):
		self._mode: Mode = mode
		super(Landlord, self).__init__(storage, state=state)
