import abc
from collections import deque
from typing import Any, cast, Deque, Generator, Iterable, Iterator, List, Optional, Sequence

from .accesses import SimpleAccessReader
from ..workload import Access, FileID, TimeStamp


class AccessInfo(object):
	__slots__ = [
		'access',
		'file_hit',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		'bytes_removed',
		'total_bytes',
		'evicted_files',
	]

	def __init__(
		self,
		access: Access,
		file_hit: bool,
		bytes_hit: int,
		bytes_missed: int,
		bytes_added: int,
		bytes_removed: int,
		total_bytes: int,
		evicted_files: Sequence[FileID],
	):
		self.access: Access = access
		self.file_hit: bool = file_hit
		self.bytes_hit: int = bytes_hit
		self.bytes_missed: int = bytes_missed
		self.bytes_added: int = bytes_added
		self.bytes_removed: int = bytes_removed
		self.total_bytes: int = total_bytes
		self.evicted_files: Sequence[FileID] = evicted_files

	@property
	def bytes_requested(self) -> int:
		return self.bytes_hit + self.bytes_missed

	@staticmethod
	def key(access_info: 'AccessInfo') -> TimeStamp:
		return access_info.access.access_ts


ProcessorGenerator = Generator[Optional[AccessInfo], Optional[Access], None]

class Processor(abc.ABC):
	def __iter__(self) -> ProcessorGenerator:
		return self.generator()

	@abc.abstractmethod
	def generator(self) -> ProcessorGenerator:
		raise NotImplementedError


class OnlineProcessor(Processor):
	def generator(self) -> ProcessorGenerator:
		access_info: Optional[AccessInfo] = None
		while True:

			# Needs to yield in order to receive next access
			#
			# Could cache and expose access_info differently (buffer build-up
			# here)
			#
			# Advantage: Caller can interact with generator interactively
			# (send to specific generator) (→ push interface), generator can
			# be passed between callers (unnecessary)
			#
			# Could yield None until ready to yield AccessInfo stats. Cache
			# system aggregates this (time merge) into a single iterable until
			# all underlying streams stop.

			access = yield access_info
			# access: Optional[Access] = yield access_info
			if access is None:
				return
			access_info = self._process_access(access)

	@abc.abstractmethod
	def _process_access(self, access: Access) -> AccessInfo:
		raise NotImplementedError


class OfflineProcessor(Processor):
	def __init__(self) -> None:
		self._accesses: List[Access] = []

	def generator(self) -> ProcessorGenerator:
		while True:
			# access: Access = yield None
			access = yield None
			if access is None:
				# start processing
				break
			self._accesses.append(access)

		it = self._process_accesses(self._accesses)
		for access_info in it:
			yield access_info

	@abc.abstractmethod
	def _process_accesses(self, accesses: SimpleAccessReader) -> Iterator[AccessInfo]:
		raise NotImplementedError
