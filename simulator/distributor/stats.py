from typing import Dict, Iterable, Iterator, List, Union
from ..workload import Access, AccessScheme, FileID, Job, Submitter, Task


class PartStats(object):
	__slots__ = ['ind', 'accesses', 'total_bytes_accessed', 'unique_bytes_accessed']

	def __init__(self, ind: int):
		self.ind: int = ind
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0


class FileStats(object):
	__slots__ = ['id', 'accesses', 'total_bytes_accessed', 'unique_bytes_accessed', 'parts']

	def __init__(self, id: FileID):
		self.id: FileID = id
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0
		self.parts: List[PartStats] = []


class TotalStats(object):
	__slots__ = ['accesses', 'total_bytes_accessed', 'unique_bytes_accessed']

	def __init__(self) -> None:
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0


class StatsCollector(object):
	def __init__(self, jobs_it: Iterable[Job]):
		self._jobs_it: Iterator[Job] = iter(jobs_it)
		self._files_stats: Dict[FileID, FileStats] = {}
		self._total_stats: TotalStats = TotalStats()

	def __iter__(self) -> Iterator[Job]:
		for job in self._jobs_it:
			for scheme in job.access_schemes:
				self._process_access(scheme)
			yield job

	def _process_access(self, access: Union[Access, AccessScheme]) -> None:
		try:
			file_stats = self._files_stats[access.file]
		except KeyError:
			file_stats = FileStats(access.file)
			self._files_stats[access.file] = file_stats

		file_stats.accesses += 1
		self._total_stats.accesses += 1

		for ind, bytes_read in access.parts:
			try:
				part_stats = file_stats.parts[ind]
			except IndexError:
				l = len(file_stats.parts)
				file_stats.parts.extend(
					PartStats(l + i) for i in range(ind + 1 - l)
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
