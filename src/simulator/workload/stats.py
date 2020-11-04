from typing import Dict, Iterable, Iterator, List, Union, ValuesView

from . import Access, AccessRequest, BytesSize, FileID, PartInd


class PartStats(object):
	__slots__ = ['ind', 'accesses', 'total_bytes_accessed', 'unique_bytes_accessed']

	def __init__(self, ind: PartInd) -> None:
		self.ind: PartInd = ind
		self.accesses: int = 0
		self.total_bytes_accessed: BytesSize = 0
		self.unique_bytes_accessed: BytesSize = 0

	def reset(self) -> None:
		self.accesses = 0
		self.total_bytes_accessed = 0
		self.unique_bytes_accessed = 0


class FileStats(object):
	__slots__ = ['id', 'accesses', 'total_bytes_accessed', 'unique_bytes_accessed', 'parts']

	def __init__(self, id: FileID) -> None:
		self.id: FileID = id
		self.accesses: int = 0
		self.total_bytes_accessed: BytesSize = 0
		self.unique_bytes_accessed: BytesSize = 0
		self.parts: List[PartStats] = []

	def reset(self) -> None:
		self.accesses = 0
		self.total_bytes_accessed = 0
		self.unique_bytes_accessed = 0
		self.parts = []


class TotalStats(object):
	__slots__ = ['accesses', 'total_bytes_accessed', 'unique_bytes_accessed']

	def __init__(self) -> None:
		self.accesses: int = 0
		self.total_bytes_accessed: BytesSize = 0
		self.unique_bytes_accessed: BytesSize = 0

	def reset(self) -> None:
		self.accesses = 0
		self.total_bytes_accessed = 0
		self.unique_bytes_accessed = 0


class StatsCounters(object):
	def __init__(self) -> None:
		self._files_stats: Dict[FileID, FileStats] = {}
		self._total_stats: TotalStats = TotalStats()

	@property
	def total_stats(self) -> TotalStats:
		return self._total_stats

	@property
	def files_stats(self) -> ValuesView[FileStats]:
		return self._files_stats.values()

	def file_stats(self, file: FileID) -> FileStats:
		return self._files_stats[file]

	def reset(self) -> None:
		self._files_stats.clear()
		self._total_stats.reset()

	def _new_file_stats(self, id: FileID) -> FileStats:
		return FileStats(id)

	def _new_part_stats(self, ind: PartInd) -> PartStats:
		return PartStats(ind)

	def process_access(self, access: Union[Access, AccessRequest]) -> None:
		try:
			file_stats = self._files_stats[access.file]
		except KeyError:
			file_stats = self._new_file_stats(access.file)
			self._files_stats[access.file] = file_stats

		file_stats.accesses += 1
		self._total_stats.accesses += 1

		for ind, bytes_read in access.parts:
			try:
				part_stats = file_stats.parts[ind]
			except IndexError:
				l = len(file_stats.parts)
				file_stats.parts.extend(
					self._new_part_stats(l + i) for i in range(ind + 1 - l)
				)
				part_stats = file_stats.parts[ind]

			part_stats.accesses += 1

			if bytes_read > part_stats.unique_bytes_accessed:
				diff = bytes_read - part_stats.unique_bytes_accessed
				part_stats.unique_bytes_accessed += diff
				file_stats.unique_bytes_accessed += diff
				self._total_stats.unique_bytes_accessed += diff

			part_stats.total_bytes_accessed += bytes_read
			file_stats.total_bytes_accessed += bytes_read
			self._total_stats.total_bytes_accessed += bytes_read


class StatsCollector(object):
	def __init__(self, accesses_it: Iterable[Access]) -> None:
		self._accesses_it: Iterator[Access] = iter(accesses_it)
		self._counters: StatsCounters = StatsCounters()

	@property
	def stats(self) -> StatsCounters:
		return self._counters

	def __iter__(self) -> Iterator[Access]:
		for access in self._accesses_it:
			self._counters.process_access(access)
			yield access
