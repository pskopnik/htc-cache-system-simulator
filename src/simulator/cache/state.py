import abc
from typing import Iterable, Iterator, List, Optional

from .accesses import SimpleAccessReader
from .processor import AccessInfo, OfflineProcessor, OnlineProcessor
from .storage import Storage
from ..workload import Access, FileID

__all__ = [
	'Access',
	'AccessInfo',
	'FileID',
	'SimpleAccessReader',
	'StateDrivenProcessor',
	'StateDrivenOnlineProcessor',
	'StateDrivenOfflineProcessor',
	'Storage',
]

# TODO: Move Item type of StateDrivenProcessor.remove() here

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
			ts: int = 0,
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
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
		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
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

		file_hit = self._storage.contains_file(access.file)
		requested_bytes = sum(part_bytes for part_ind, part_bytes in access.parts)
		contained_bytes = self._storage.contained_bytes(access.file, access.parts)
		missing_bytes = requested_bytes - contained_bytes
		in_cache_bytes = sum(part_bytes for _, part_bytes in self._storage.parts(access.file))

		if missing_bytes == 0:
			info = AccessInfo(
				access,
				True,
				contained_bytes,
				missing_bytes,
				0,
				0,
				in_cache_bytes,
				[],
			)
			self._state.process_access(access.file, ind, False, info)
			return info

		free_bytes = self._storage.free_bytes
		evicted_files: List[FileID] = []
		evicted_bytes = 0

		while free_bytes < missing_bytes:
			for eviction_candidate in self._state.pop_eviction_candidates(
				file = access.file,
				ts = access.access_ts,
				ind = ind,
				requested_bytes = requested_bytes,
				contained_bytes = contained_bytes,
				missing_bytes = missing_bytes,
				in_cache_bytes = in_cache_bytes,
				free_bytes = free_bytes,
				required_free_bytes = missing_bytes - free_bytes,
			):
				evicted_file_bytes = self._storage.evict(eviction_candidate)

				evicted_files.append(eviction_candidate)
				evicted_bytes += evicted_file_bytes
				free_bytes += evicted_file_bytes

				if eviction_candidate == access.file:
					# TODO: just evicted the file about to be accessed...
					# Should a warning be emitted?

					# This also affects statistics, conceptually the eviction
					# takes place before re-placement. I.e. the access is a
					# a complete miss.

					contained_bytes = 0
					missing_bytes = requested_bytes
					in_cache_bytes = 0

		placed_bytes = self._storage.place(access.file, access.parts) # should equal missing_bytes
		total_bytes = in_cache_bytes + placed_bytes

		info = AccessInfo(
			access,
			file_hit,
			contained_bytes,
			missing_bytes,
			placed_bytes,
			evicted_bytes,
			total_bytes,
			evicted_files,
		)
		# If any byte is in cache, the state tracks the file
		ensure = in_cache_bytes == 0
		self._state.process_access(access.file, ind, ensure, info)
		return info


class StateDrivenOnlineProcessor(StateDrivenProcessor, OnlineProcessor):
	pass


class StateDrivenOfflineProcessor(StateDrivenProcessor, OfflineProcessor):
	class _DummyState(StateDrivenProcessor.State):
		def pop_eviction_candidates(
			self,
			file: FileID = "",
			ts: int = 0,
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
		) -> Iterable[FileID]:
			raise NotImplementedError

		def find(self, file: FileID) -> Optional[StateDrivenProcessor.State.Item]:
			raise NotImplementedError

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			raise NotImplementedError

		def remove_file(self, file: FileID) -> None:
			raise NotImplementedError

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			raise NotImplementedError

	class State(StateDrivenProcessor.State):
		pass

	def __init__(self, storage: Storage, state: Optional[State]=None) -> None:
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
