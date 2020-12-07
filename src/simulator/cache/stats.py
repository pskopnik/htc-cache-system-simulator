from typing import cast, Dict, Iterable, Iterator, List, Optional, Tuple, ValuesView

from .processor import AccessInfo
from ..workload import BytesSize, FileID, PartInd, PartSpec, stats, TimeStamp


class FileStats(stats.FileStats):
	__slots__ = [
		'hits',
		'misses',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		# 'bytes_removed', # not precisely known
		'bytes_removed_due',
		'last_residency_begin',
		'last_residency_end',
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
		# last_residency_begin < last_residency_end means the file is not in the cache atm.
		# last_residency_begin > last_residency_end means the file is in the cache atm.
		# last_residency_begin == last_residency_end is unclear.
		self.last_residency_begin: TimeStamp = 0
		# last_residency_end == 0 may either mean "unset" or an residency end at time 0
		self.last_residency_end: TimeStamp = 0

	def reset(self) -> None:
		super(FileStats, self).reset()
		self.hits = 0
		self.misses = 0
		self.bytes_hit = 0
		self.bytes_missed = 0
		self.bytes_added = 0
		# self.bytes_removed = 0 # not precisely known
		self.bytes_removed_due = 0
		self.last_residency_begin = 0
		self.last_residency_end = 0


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
			file_stats.last_residency_begin = access_info.access.access_ts

		self._total_stats.bytes_hit += access_info.bytes_hit
		self._total_stats.bytes_missed += access_info.bytes_missed
		self._total_stats.bytes_added += access_info.bytes_added
		self._total_stats.bytes_removed += access_info.bytes_removed

		for file in access_info.evicted_files:
			try:
				self._cache_files_stats[file].last_residency_end = access_info.access.access_ts
			except KeyError:
				pass


class StatsCollector(object):
	def __init__(self, access_info_it: Iterable[AccessInfo]) -> None:
		self._access_info_it: Iterator[AccessInfo] = iter(access_info_it)
		self._counters: StatsCounters = StatsCounters()
		self._filter: Optional[MissOnFirstReaccessFilter] = None

	@property
	def stats(self) -> StatsCounters:
		return self._counters

	def reset(self) -> None:
		if self._filter is not None:
			raise Exception('There is already a filter in place, cannot reset again')

		self._filter = MissOnFirstReaccessFilter(self._counters)
		self._counters.reset()

	def __iter__(self) -> Iterator[AccessInfo]:
		for access_info in self._access_info_it:
			self._counters.process_access(access_info.access)

			access_info_to_be_processed = access_info
			if self._filter is not None:
				access_info_to_be_processed = self._filter(access_info)

				if len(self._filter) == 0:
					self._filter = None

			self._counters.process_access_info(access_info_to_be_processed)

			yield access_info


class MissOnFirstReaccessFilter(object):
	def __init__(self, counters: StatsCounters) -> None:
		self._marked_files: Dict[FileID, Dict[PartInd, Tuple[BytesSize, BytesSize]]] = self._build_marked_files(counters)

	def __len__(self) -> int:
		return len(self._marked_files)

	def __call__(self, access_info: AccessInfo) -> AccessInfo:
		try:
			file_info = self._marked_files[access_info.access.file]
		except KeyError:
			return access_info

		hit_parts: List[PartSpec] = []

		for part_spec in access_info.hit_parts:
			part_ind, hit_size = part_spec
			if part_ind in file_info:
				marked_missing, max_size_seen = file_info[part_ind]
				part_bytes_hit = hit_size - min(hit_size, marked_missing) + min(hit_size, max_size_seen)
				# This does not fit the PartSpec model: There, the `size` value always describes
				# the `size` "first" bytes of the part, but here part_bytes_hit may represent
				# bytes which have a "gap":
				# hit_size:       |------------|
				# marked missing: |---------|
				# max_size_seen:  |------|
				# still missing:          |-|
				# part_bytes_hit: |------|   |-|
				# However, the most common calculations still work.
				hit_parts.append((part_ind, part_bytes_hit))
			else:
				hit_parts.append(part_spec)

		if access_info.file_hit:
			for part_ind, requested_size in access_info.access.parts:
				if part_ind in file_info:
					marked_missing, max_size_seen = file_info[part_ind]
					if requested_size >= marked_missing:
						del file_info[part_ind]
					elif requested_size > max_size_seen:
						file_info[part_ind] = (marked_missing, requested_size)

			if len(file_info) == 0:
				del self._marked_files[access_info.access.file]
		else:
			del self._marked_files[access_info.access.file]

		for file in access_info.evicted_files:
			if file in self._marked_files:
				del self._marked_files[file]

		requested_bytes = access_info.bytes_hit + access_info.bytes_missed
		bytes_hit = sum(size for _, size in hit_parts)
		bytes_missed = requested_bytes - bytes_hit
		bytes_added = access_info.bytes_added + access_info.bytes_hit - bytes_hit
		# There are scenarios where this calculation of total_bytes is incorrect:
		#  * partial evictions have removed some data from storage.
		#    The total_bytes calculated would be too small.
		#  * ?
		total_bytes = max(requested_bytes, access_info.total_bytes - sum(
			marked_missing - max_size_seen for marked_missing, max_size_seen in file_info.values()
		))
		# As it is based on total_bytes, previous_total_bytes may be incorrect!
		previous_total_bytes = total_bytes - bytes_added

		return AccessInfo(
			access_info.access,
			hit_parts,
			# Although this should not occur, ensure that an incorrect calculation of
			# previous_total_bytes does not switch file_hit == False to file_hit == True
			access_info.file_hit and previous_total_bytes > 0,
			bytes_hit,
			bytes_missed,
			bytes_added,
			access_info.bytes_removed,
			total_bytes,
			access_info.evicted_files,
		)

	@staticmethod
	def _build_marked_files(counters: StatsCounters) -> Dict[FileID, Dict[PartInd, Tuple[BytesSize, BytesSize]]]:
		marked_files: Dict[FileID, Dict[PartInd, Tuple[BytesSize, BytesSize]]] = {}

		for file_stats in counters.files_stats:
			# If a file is accessed and evicted in the same second, the condition is true but the
			# file is not in the cache!
			# Such cases are identified and removed by checking access_info.file_hit on the next
			# access to the file
			if file_stats.last_residency_end <= file_stats.last_residency_begin:
				marked_files[file_stats.id] = {
					part_stats.ind: (part_stats.unique_bytes_accessed, 0)
					for part_stats in file_stats.parts
				}

		return marked_files
