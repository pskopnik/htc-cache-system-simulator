import abc
from typing import Iterable, Iterator, Optional
from ..workload import Access, FileID
from .accesses import SimpleAccessReader
from .processor import AccessInfo, OnlineProcessor, OfflineProcessor
from .storage import Storage

__all__ = ['FileID', 'StateDrivenProcessor', 'StateDrivenOnlineProcessor', 'StateDrivenOfflineProcessor']


class StateDrivenProcessor(object):
	class State(abc.ABC):
		class Item(abc.ABC):
			@property
			@abc.abstractmethod
			def file(self) -> FileID:
				raise NotImplementedError

		@abc.abstractmethod
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
			raise NotImplementedError

		@abc.abstractmethod
		def find(self, file: FileID) -> Optional[Item]:
			raise NotImplementedError

		@abc.abstractmethod
		def remove(self, item: Item) -> None:
			raise NotImplementedError

		@abc.abstractmethod
		def remove_file(self, file: FileID) -> None:
			raise NotImplementedError

		@abc.abstractmethod
		def process_access(
			self,
			file: FileID,
			ind: int = 0,
			ensure: bool = True,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> None:
			raise NotImplementedError

	def __init__(self, storage: Storage, state: Optional[State]=None):
		super(StateDrivenProcessor, self).__init__()
		self._storage: Storage = storage
		self._state: StateDrivenProcessor.State = state or self._init_state()
		self._ind: int = 0

	@abc.abstractmethod
	def _init_state(self) -> 'StateDrivenProcessor.State':
		raise NotImplementedError

	def _process_access(self, access: Access) -> AccessInfo:
		ind = self._ind
		self._ind += 1

		requested_bytes = sum(part_bytes for part_ind, part_bytes in access.parts)
		contained_bytes = self._storage.contained_bytes(access.file, access.parts)
		missing_bytes = requested_bytes - contained_bytes

		if missing_bytes == 0:
			total_bytes = sum(part_bytes for _, part_bytes in self._storage.parts(access.file))
			return AccessInfo(
				access,
				contained_bytes,
				missing_bytes,
				0,
				0,
				total_bytes,
			)

		free_bytes = self._storage.free_bytes
		evicted_bytes = 0

		while free_bytes < missing_bytes:
			for eviction_candidate in self._state.pop_eviction_candidates(
				file = access.file,
				ind = ind,
				requested_bytes = requested_bytes,
				contained_bytes = contained_bytes,
				missing_bytes = missing_bytes,
				free_bytes = free_bytes,
				required_bytes = missing_bytes - free_bytes,
			):
				evicted_file_bytes = self._storage.evict(eviction_candidate)

				evicted_bytes += evicted_file_bytes
				free_bytes += evicted_file_bytes

		placed_bytes = self._storage.place(access.file, access.parts)
		total_bytes = sum(part_bytes for _, part_bytes in self._storage.parts(access.file))
		self._state.process_access(
			access.file,
			ind = ind,
			ensure = True,
			requested_bytes = requested_bytes,
			placed_bytes = placed_bytes,
			total_bytes = total_bytes,
		)

		return AccessInfo(
			access,
			contained_bytes,
			missing_bytes,
			placed_bytes,
			evicted_bytes,
			total_bytes,
		)


class StateDrivenOnlineProcessor(StateDrivenProcessor, OnlineProcessor):
	pass


class StateDrivenOfflineProcessor(StateDrivenProcessor, OfflineProcessor):
	class _DummyState(StateDrivenProcessor.State):
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
			raise NotImplementedError

		def find(self, file: FileID) -> Optional[StateDrivenProcessor.State.Item]:
			raise NotImplementedError

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			raise NotImplementedError

		def remove_file(self, file: FileID) -> None:
			raise NotImplementedError

		def process_access(
			self,
			file: FileID,
			ind: int = 0,
			ensure: bool = True,
			requested_bytes: int = 0,
			placed_bytes: int = 0,
			total_bytes: int = 0,
		) -> None:
			raise NotImplementedError

	class State(StateDrivenProcessor.State):
		pass

	def __init__(self, storage: Storage, state: Optional[State]=None):
		super(StateDrivenOfflineProcessor, self).__init__(
			storage,
			state = state or StateDrivenOfflineProcessor._DummyState(),
		)

	def _init_state(self) -> 'StateDrivenProcessor.State':
		raise NotImplementedError

	def _process_accesses(self, accesses: SimpleAccessReader) -> Iterator[AccessInfo]:
		self._state = self._init_full_state(accesses)

		for access in accesses:
			yield self._process_access(access)

	@abc.abstractmethod
	def _init_full_state(self, accesses: SimpleAccessReader) -> 'StateDrivenOfflineProcessor.State':
		raise NotImplementedError
