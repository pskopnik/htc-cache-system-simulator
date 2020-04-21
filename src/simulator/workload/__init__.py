import abc
from typing import Any, Iterator, Iterable, List, Sequence, Tuple, Optional

BytesRate = int
BytesSize = int
TimeStamp = int

FileID = str
PartInd = int

PartSpec = Tuple[PartInd, BytesSize] # (part_ind, bytes_read)


class AccessRequest(object):
	__slots__ = ['file', 'parts']

	def __init__(self, file: FileID, parts: Sequence[PartSpec]) -> None:
		self.file: FileID = file
		self.parts: Sequence[PartSpec] = parts


class Access(object):
	__slots__ = ['access_ts', 'file', 'parts']

	def __init__(self, access_ts: TimeStamp, file: FileID, parts: Sequence[PartSpec]) -> None:
		self.access_ts: TimeStamp = access_ts
		self.file: FileID = file
		self.parts: Sequence[PartSpec] = parts


class Job(object):
	__slots__ = ['submit_ts', 'access_requests']

	def __init__(self, submit_ts: Optional[TimeStamp], access_requests: Sequence[AccessRequest]) -> None:
		self.submit_ts: Optional[TimeStamp] = submit_ts # TODO: define semantics with regards to optionality
		self.access_requests: Sequence[AccessRequest] = access_requests


class Submitter(abc.ABC):
	def __init__(self, start_ts: TimeStamp, origin: Optional[Any]=None) -> None:
		self.start_ts: TimeStamp = start_ts
		self._origin: Optional[Any] = origin

	@property
	def origin(self) -> Optional[Any]:
		return self._origin

	@abc.abstractmethod
	def __iter__(self) -> Iterator[Job]:
		raise NotImplementedError


Task = Iterable[Submitter]


class PartsGenerator(abc.ABC):
	@abc.abstractmethod
	def parts(self, total_bytes: int) -> List[PartSpec]:
		raise NotImplementedError
