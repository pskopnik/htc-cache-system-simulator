from typing import cast, Dict, Iterable, Iterator, ValuesView

from .processor import AccessInfo
from ..workload import BytesSize, FileID, stats, TimeStamp


class FileStats(stats.FileStats):
	__slots__ = [
		'hits',
		'misses',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		# 'bytes_removed', # not precisely known
		'bytes_removed_due',
		'last_residence_begin',
		'last_residence_end',
	]

	def __init__(self, id: FileID) -> None:
		super(FileStats, self).__init__(id)
		self.hits: int = 0
		self.misses: int = 0
		self.bytes_hit: BytesSize = 0
		self.bytes_missed: BytesSize = 0
		self.bytes_added: BytesSize = 0
		# self.bytes_removed: int = 0 # not precisely known
		self.bytes_removed_due: BytesSize = 0
		self.last_residence_begin: TimeStamp = 0
		self.last_residence_end: TimeStamp = 0

	def reset(self) -> None:
		super(FileStats, self).reset()
		self.hits = 0
		self.misses = 0
		self.bytes_hit = 0
		self.bytes_missed = 0
		self.bytes_added = 0
		# self.bytes_removed = 0 # not precisely known
		self.bytes_removed_due = 0
		self.last_residence_begin = 0
		self.last_residence_end = 0


class TotalStats(stats.TotalStats):
	__slots__ = [
		'files_hit',
		'files_missed',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		'bytes_removed',
	]

	def __init__(self) -> None:
		super(TotalStats, self).__init__()
		self.files_hit: int = 0
		self.files_missed: int = 0
		self.bytes_hit: BytesSize = 0
		self.bytes_missed: BytesSize = 0
		self.bytes_added: BytesSize = 0
		self.bytes_removed: BytesSize = 0

	def reset(self) -> None:
		super(TotalStats, self).reset()
		self.files_hit = 0
		self.files_missed = 0
		self.bytes_hit = 0
		self.bytes_missed = 0
		self.bytes_added = 0
		self.bytes_removed = 0


class StatsCounters(stats.StatsCounters):
	def __init__(self) -> None:
		super(StatsCounters, self).__init__()
		self._total_stats: TotalStats = TotalStats()

		# This is a workaround to have a reference to self._files_stats with the
		# adapted types expected in this class.
		self._cache_files_stats: Dict[FileID, FileStats] = cast(
			Dict[FileID, FileStats],
			self._files_stats,
		)

	@property
	def total_stats(self) -> TotalStats:
		return self._total_stats

	@property
	def files_stats(self) -> ValuesView[FileStats]:
		return self._cache_files_stats.values()

	def file_stats(self, file: FileID) -> FileStats:
		return self._cache_files_stats[file]

	def _new_file_stats(self, id: FileID) -> FileStats:
		return FileStats(id)

	def process_access_info(self, access_info: AccessInfo) -> None:
		try:
			file_stats = self._cache_files_stats[access_info.access.file]
		except KeyError:
			file_stats = self._new_file_stats(access_info.access.file)
			self._cache_files_stats[access_info.access.file] = file_stats

		file_stats.bytes_hit += access_info.bytes_hit
		file_stats.bytes_missed += access_info.bytes_missed
		file_stats.bytes_added += access_info.bytes_added
		file_stats.bytes_removed_due += access_info.bytes_removed

		if access_info.file_hit:
			file_stats.hits += 1
			self._total_stats.files_hit += 1
		else:
			file_stats.misses += 1
			self._total_stats.files_missed += 1
			file_stats.last_residence_begin = access_info.access.access_ts

		self._total_stats.bytes_hit += access_info.bytes_hit
		self._total_stats.bytes_missed += access_info.bytes_missed
		self._total_stats.bytes_added += access_info.bytes_added
		self._total_stats.bytes_removed += access_info.bytes_removed

		for file in access_info.evicted_files:
			self._cache_files_stats[file].last_residence_end = access_info.access.access_ts


class StatsCollector(object):
	def __init__(self, access_info_it: Iterable[AccessInfo]) -> None:
		self._access_info_it: Iterator[AccessInfo] = iter(access_info_it)
		self._counters: StatsCounters = StatsCounters()

	@property
	def stats(self) -> StatsCounters:
		return self._counters

	def __iter__(self) -> Iterator[AccessInfo]:
		for access_info in self._access_info_it:
			self._counters.process_access(access_info.access)
			self._counters.process_access_info(access_info)
			yield access_info
