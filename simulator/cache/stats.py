from typing import Dict, Iterable, Iterator, List, Union
from ..workload import Access, AccessScheme, FileID, Job, Submitter, Task
# from ..distributor import stats
from .processor import AccessInfo


# class FileStats(stats.FileStats):
# 	__slots__ = ['bytes_hit', 'bytes_missed', 'bytes_added', 'bytes_removed']

# 	def __init__(self, id: FileID):
# 		super(FileStats, self).__init__(id)
# 		self.bytes_hit: int = 0
# 		self.bytes_missed: int = 0
# 		self.bytes_added: int = 0
# 		self.bytes_removed: int = 0


# class TotalStats(stats.TotalStats):
# 	__slots__ = ['bytes_hit', 'bytes_missed', 'bytes_added', 'bytes_removed']

# 	def __init__(self):
# 		super(TotalStats, self).__init__()
# 		self.bytes_hit: int = 0
# 		self.bytes_missed: int = 0
# 		self.bytes_added: int = 0
# 		self.bytes_removed: int = 0


# class StatsCollector(stats.StatsCollector):
# 	def __init__(self, access_info_it: Iterable[AccessInfo]):
# 		self._access_info_it: Iterator[AccessInfo] = iter(access_info_it)
# 		self._files_stats: Dict[FileID, FileStats] = {}
# 		self._total_stats: TotalStats = TotalStats()

# 	def __iter__(self) -> Iterator[AccessInfo]:
# 		for access_info in self._access_info_it:
# 			self._process_access(access_info.access)
# 			yield access_info


class PartStats(object):
	__slots__ = ['ind', 'accesses', 'total_bytes_accessed', 'unique_bytes_accessed']

	def __init__(self, ind: int):
		self.ind: int = ind
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0


class FileStats(object):
	__slots__ = [
		'id', # inferrable from Access object
		'accesses', # inferrable from Access object
		'total_bytes_accessed', # inferrable from Access object
		'unique_bytes_accessed', # inferrable from Access object
		'parts', # inferrable from Access object
		'hits',
		'misses',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		# 'bytes_removed', # not known

	]

	def __init__(self, id: FileID):
		self.id: FileID = id
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0
		self.parts: List[PartStats] = []
		self.hits: int = 0
		self.misses: int = 0
		self.bytes_hit: int = 0
		self.bytes_missed: int = 0
		self.bytes_added: int = 0
		# self.bytes_removed: int = 0 # not known


class TotalStats(object):
	__slots__ = [
		'accesses', # inferrable from Access object
		'total_bytes_accessed', # inferrable from Access object
		'unique_bytes_accessed', # inferrable from Access object
		'files_hit',
		'files_missed',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		'bytes_removed',
	]

	def __init__(self) -> None:
		self.accesses: int = 0
		self.total_bytes_accessed: int = 0
		self.unique_bytes_accessed: int = 0
		self.files_hit: int = 0
		self.files_missed: int = 0
		self.bytes_hit: int = 0
		self.bytes_missed: int = 0
		self.bytes_added: int = 0
		self.bytes_removed: int = 0


class StatsCollector(object):
	def __init__(self, access_info_it: Iterable[AccessInfo]):
		self._access_info_it: Iterator[AccessInfo] = iter(access_info_it)
		self._files_stats: Dict[FileID, FileStats] = {}
		self._total_stats: TotalStats = TotalStats()

	def reset(self) -> None:
		self._files_stats = {}
		self._total_stats = TotalStats()

	def __iter__(self) -> Iterator[AccessInfo]:
		for access_info in self._access_info_it:
			self._process_access(access_info.access)
			self._process_access_info(access_info)
			yield access_info

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

	def _process_access_info(self, access_info: AccessInfo) -> None:
		try:
			file_stats = self._files_stats[access_info.access.file]
		except KeyError:
			file_stats = FileStats(access_info.access.file)
			self._files_stats[access_info.access.file] = file_stats

		file_stats.bytes_hit += access_info.bytes_hit
		file_stats.bytes_missed += access_info.bytes_missed
		file_stats.bytes_added += access_info.bytes_added

		if access_info.total_bytes - access_info.bytes_added > 0:
			file_stats.hits += 1
			self._total_stats.files_hit += 1
		else:
			file_stats.misses += 1
			self._total_stats.files_missed += 1

		self._total_stats.bytes_hit += access_info.bytes_hit
		self._total_stats.bytes_missed += access_info.bytes_missed
		self._total_stats.bytes_added += access_info.bytes_added
		self._total_stats.bytes_removed += access_info.bytes_removed
