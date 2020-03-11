from dataclasses import dataclass, field
from typing import cast, Iterable, List, Optional, Tuple

from ..state import Access, AccessInfo, FileID, StateDrivenProcessor, StateDrivenOnlineProcessor, Storage
from ...dstructures.lru import FileInfo, FileLRUDict
from ...params import parse_user_args, SimpleField


class ARCBit(StateDrivenOnlineProcessor):
	"""Processor following an adaptive policy based on two LRU structures.
	"""

	@dataclass
	class Configuration(object):
		ghosts_factor: float = field(init=True, default=1.0)

		@classmethod
		def from_user_args(cls, user_args: str) -> 'ARCBit.Configuration':
			inst = cls()
			parse_user_args(user_args, inst, [
				SimpleField('ghosts_factor', float),
			])
			return inst

	class State(StateDrivenProcessor.State):
		class Item(StateDrivenProcessor.State.Item):
			def __init__(self, file: FileID):
				self._file: FileID = file

			@property
			def file(self) -> FileID:
				return self._file

		def __init__(self, total_size: int, configuration: 'ARCBit.Configuration') -> None:
			self._total_size: int = total_size
			self._ghosts_factor: float = configuration.ghosts_factor

			# Targeted _top_once.total_size
			self._top_once_target_size: int = 0
			self._ghosts_total_size: int = int(total_size * self._ghosts_factor)
			self._once_total_size: int = int((1 + self._ghosts_factor) * total_size / 2)

			# _top_once contains the top of the LRU list of files seen only
			# once. _Referred to as T_1 in the paper.
			self._top_once: FileLRUDict = FileLRUDict()
			# _bottom_once contains the bottom of the LRU list of files seen
			# only once. _Referred to as B_1 in the paper.
			self._bottom_once: FileLRUDict = FileLRUDict()
			# _top_once contains the top of the LRU list of files seen
			# multiple times. _Referred to as T_2 in the paper.
			self._top_multiple: FileLRUDict = FileLRUDict()
			# _bottom_once contains the bottom of the LRU list of files seen
			# multiple times. _Referred to as B_2 in the paper.
			self._bottom_multiple: FileLRUDict = FileLRUDict()

		def pop_eviction_candidates(
			self,
			file: FileID = '',
			ts: int = 0,
			ind: int = 0,
			requested_bytes: int = 0,
			contained_bytes: int = 0,
			missing_bytes: int = 0,
			in_cache_bytes: int = 0,
			free_bytes: int = 0,
			required_free_bytes: int = 0,
		) -> Iterable[FileID]:
			candidates: List[FileID]
			in_top_once = file in self._top_once
			if file in self._top_multiple or in_top_once:
				# Case 1: the file is already in the cache

				if in_top_once:
					self._move_file(file, self._top_once, self._top_multiple)
				else:
					self._top_multiple.access(file)

				candidates = self._evict(missing_bytes, in_top_once)

				if file in candidates:
					candidates.extend(self._evict(requested_bytes, in_top_once))
					self._top_multiple[file] = FileInfo(requested_bytes)
				else:
					self._top_multiple.add_bytes_to_file(file, missing_bytes)

				return candidates

			elif file in self._bottom_once:
				# Case 2: the file is not in the cache, but still tracked
				# because it was accessed once (since its previous eviction)

				# Increase target size of _top_once
				self._top_once_target_size = min(
					self._total_size,
					self._top_once_target_size + round(
						max(
							self._bottom_multiple.total_size / self._bottom_once.total_size, 1,
						) * self._average_file_size(),
					),
				)

				candidates = self._evict(missing_bytes, True)

				self._move_file_from_bottom(
					file,
					self._bottom_once,
					self._top_multiple,
					missing_bytes,
				)

				return candidates

			elif file in self._bottom_multiple:
				# Case 3: the file is not in the cache, but still tracked
				# because it was accessed multiple times (since its previous
				# eviction)

				# Decrease target size of _top_once
				self._top_once_target_size = max(
					0,
					self._total_size - round(
						max(
							self._bottom_once.total_size / self._bottom_multiple.total_size, 1,
						) * self._average_file_size(),
					),
				)

				candidates = self._evict(missing_bytes, False)

				self._move_file_from_bottom(
					file,
					self._bottom_multiple,
					self._top_multiple,
					missing_bytes,
				)

				return candidates

			else:
				# Case 4: The file is neither in the cache nor the cache
				# directory (_bottom_once or _bottom_multiple).

				candidates = []

				while (
					self._top_once.total_size + self._bottom_once.total_size + missing_bytes
						> self._once_total_size
				):
					if len(self._bottom_once) > 0:
						self._bottom_once.pop()
					else:
						candidate, _ = self._top_once.pop()
						candidates.append(candidate)

				candidates.extend(self._evict(missing_bytes, False))

				self._top_once[file] = FileInfo(missing_bytes)

				return candidates

		def _evict(self, required_bytes: int, in_once: bool) -> List[FileID]:
			candidates: List[FileID] = []
			candidate_size: int

			# For quick access (shorter to write, faster to execute)
			top_once = self._top_once
			top_multiple = self._top_multiple
			t_o_target = self._top_once_target_size

			while top_once.total_size + top_multiple.total_size + required_bytes > self._total_size:
				candidate: FileID

				if (
					len(top_once) > 1
					and (
						(
							top_once.total_size > t_o_target
							or (not in_once and top_once.total_size + required_bytes > t_o_target)
						)
						or len(top_multiple) == 0
					)
				):
					candidate, info = self._move_lru(top_once, self._bottom_once)
					candidate_size = info.size
				else:
					candidate, info = self._move_lru(top_multiple, self._bottom_multiple)
					candidate_size = info.size

				# Invariant 3) may be violated until this while loop
				# completes.

				while (
					self._bottom_once.total_size + self._bottom_multiple.total_size + candidate_size
						> self._ghosts_total_size
				):
					self._bottom_multiple.pop()

				candidates.append(candidate)

			return candidates

		def _average_file_size(self) -> int:
			return round(
				(
					self._top_once.total_size + self._bottom_once.total_size +
					self._top_multiple.total_size + self._bottom_multiple.total_size
				) / (
					len(self._top_once) + len(self._bottom_once) +
					len(self._top_multiple) + len(self._bottom_multiple)
				)
			)

		def _move_file(self, file: FileID, origin: FileLRUDict, dest: FileLRUDict) -> None:
			info = origin[file]
			del origin[file]
			dest[file] = info

		def _move_file_from_bottom(
			self,
			file: FileID,
			origin: FileLRUDict,
			dest: FileLRUDict,
			requested_bytes: int,
		) -> None:
			try:
				info = origin[file]
				del origin[file]
				info.size = requested_bytes
			except KeyError:
				info = FileInfo(requested_bytes)
			dest[file] = info

		def _move_lru(self, origin: FileLRUDict, dest: FileLRUDict) -> Tuple[FileID, FileInfo]:
			popped_tuple = origin.pop()
			dest[popped_tuple[0]] = popped_tuple[1]
			return popped_tuple

		def _verify(self) -> None:
			if self._top_once.total_size + self._top_multiple.total_size > self._total_size:
				raise Exception('Invariant 1) violated: total cache size exceeded')

			if self._top_once.total_size + self._bottom_once.total_size > self._once_total_size:
				raise Exception('Invariant 2) violated: once list size exceeded')

			if (
				self._bottom_once.total_size + self._bottom_multiple.total_size
					> self._ghosts_total_size
			):
				raise Exception('Invariant 3) violated: too many ghost entries')

		def find(self, file: FileID) -> Optional[Item]:
			if (
				file in self._top_once or
				file in self._top_multiple or
				file in self._bottom_once or
				file in self._bottom_multiple
			):
				return ARCBit.State.Item(file)
			else:
				return None

		def remove(self, item: StateDrivenProcessor.State.Item) -> None:
			if not isinstance(item, ARCBit.State.Item):
				raise TypeError('unsupported item type passed')

			self.remove_file(item._file)

		def remove_file(self, file: FileID) -> None:
			try:
				del self._top_once[file]
				return
			except KeyError:
				pass
			try:
				del self._bottom_once[file]
				return
			except KeyError:
				pass
			try:
				del self._top_multiple[file]
				return
			except KeyError:
				pass

			del self._bottom_multiple[file]

		def process_access(self, file: FileID, ind: int, ensure: bool, info: AccessInfo) -> None:
			if info.bytes_added == 0:
				# pop_eviction_candidates was not called because the access
				# was a full hit. That means the file is in _top_multiple or
				# _top_once.

				if file in self._top_multiple:
					# Case 1: the file is already in the cache
					# Case 1.1: the file is already in _top_multiple
					self._top_multiple.access(file)
				elif file in self._top_once:
					# Case 1: the file is already in the cache
					# Case 1.1: the file is in _top_once
					self._move_file(file, self._top_once, self._top_multiple)
				else:
					raise Exception(
						'File from access not adding bytes to cache is not in the cache directory',
					)

			elif info.bytes_removed == 0:
				# pop_eviction_candidates was not called because there was
				# sufficient space in the cache.

				candidates = self.pop_eviction_candidates(
					file = file,
					ind = ind,
					requested_bytes = info.bytes_requested,
					contained_bytes = info.bytes_hit,
					missing_bytes = info.bytes_missed,
					in_cache_bytes = info.bytes_hit, # incorrect, but doesn't matter
					free_bytes = info.bytes_missed, # incorrect, but doesn't matter
					required_free_bytes = 0,
				)

				if len(cast('List[FileID]', candidates)) > 0:
					raise Exception(
						'Processing of access with sufficient spare cache capacity yields ' +
						'eviction candidates',
					)

	def _init_state(self) -> 'ARCBit.State':
		return ARCBit.State(self._storage.total_bytes, self._configuration)

	def __init__(
		self,
		configuration: 'ARCBit.Configuration',
		storage: Storage,
		state: Optional[State] = None,
	) -> None:
		self._configuration = configuration
		super(ARCBit, self).__init__(storage, state=state)
