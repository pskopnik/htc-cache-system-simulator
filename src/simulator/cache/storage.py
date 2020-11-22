from typing import Dict, List, Sequence

from ..workload import BytesSize, FileID, PartInd, PartSpec


class InsufficientFreeSpace(Exception):
	pass


class Storage(object):
	def __init__(self, total_bytes: BytesSize):
		self._total_bytes: BytesSize = total_bytes
		self._used_bytes: BytesSize = 0
		self._files: Dict[FileID, Dict[PartInd, BytesSize]] = {}

	@property
	def total_bytes(self) -> BytesSize:
		return self._total_bytes

	@property
	def used_bytes(self) -> BytesSize:
		return self._used_bytes

	@property
	def free_bytes(self) -> BytesSize:
		return self._total_bytes - self._used_bytes

	def parts(self, file: FileID) -> List[PartSpec]:
		"""Returns all parts of file which are contained in the storage.
		"""
		try:
			file_parts = self._files[file]
		except KeyError:
			return []

		return sorted((part_ind, part_bytes) for part_ind, part_bytes in file_parts.items())

	def contains_file(self, file: FileID) -> bool:
		return file in self._files

	def contains(self, file: FileID, parts: Sequence[PartSpec]) -> bool:
		try:
			file_parts = self._files[file]
		except KeyError:
			return False

		for part_ind, part_bytes in parts:
			if file_parts.get(part_ind, 0) < part_bytes:
				return False

		return True

	def contained_bytes(self, file: FileID, parts: Sequence[PartSpec]) -> BytesSize:
		try:
			file_parts = self._files[file]
		except KeyError:
			return 0

		return sum(
			min(file_parts.get(part_ind, 0), part_bytes) for part_ind, part_bytes in parts
		)

	def missing_bytes(self, file: FileID, parts: Sequence[PartSpec]) -> BytesSize:
		requested_bytes = sum(part_bytes for part_ind, part_bytes in parts)

		return requested_bytes - self.contained_bytes(file, parts)

	def evict(self, file: FileID) -> BytesSize:
		"""Evicts all parts of file from the storage.

		Returns:
			Number of bytes evicted from the storage.
		"""
		try:
			file_parts = self._files[file]
		except KeyError:
			return 0

		del self._files[file]

		evicted_bytes = sum(file_parts.values())
		self._used_bytes -= evicted_bytes

		return evicted_bytes

	def evict_partially(
		self,
		file: FileID,
		fraction: float = 1.0,
		parts: Optional[Iterable[PartInd]] = None,
	) -> BytesSize:
		"""Evicts fraction bytes of parts of file from the storage.

		This method may break aspects of the model!
		For fractional eviction of parts, it assumes that accesses always read the full part, i.e. specify the full size as the part size in the scheme.

		No action is performed when the file is not in cache.
		The parts parameter filters the list of parts in the cache in the sense of ``parts_in_cache intersection parts``.
		If there are no remaining parts, that is fine.
		If some evictions are expected

		flooring bytes to be evicted

		Args:
			file: File key
			parts: Parts . If parts is ``None``, all parts in cache are considered.

		Returns:
			Number of bytes evicted from the storage.
		"""
		if fraction < 0.0 or fraction > 1.0:
			raise ValueError(f'Argument fraction must be in [0.0, 1.0], is {fraction!r}')

		try:
			file_parts = self._files[file]
		except KeyError:
			return 0

		candidate_parts: Iterable[PartInd]
		if parts is None:
			candidate_parts = list(file_parts.keys())
		else:
			candidate_parts = (part_ind for part_ind in parts if part_ind in file_parts)

		evicted_bytes = 0

		if fraction == 1.0:
			for part_ind in candidate_parts:
				evicted_bytes += file_parts.pop(part_ind)
		else:
			for part_ind in candidate_parts:
				part_size = file_parts[part_ind]
				part_evicted_bytes = round(fraction * part_size)
				part_remaining_size = part_size - part_evicted_bytes
				file_parts[part_ind] = part_remaining_size

				if part_remaining_size == 0:
					del file_parts[part_ind]

				evicted_bytes += part_evicted_bytes

		if len(file_parts) == 0:
			del self._files[file]

		self._used_bytes -= evicted_bytes

		return evicted_bytes

	def place(self, file: FileID, parts: Sequence[PartSpec]) -> BytesSize:
		"""Places the passed parts of file in the storage.

		Returns:
			Number of bytes added to the storage.
		"""
		missing_bytes = self.missing_bytes(file, parts)
		if self.free_bytes < missing_bytes:
			raise InsufficientFreeSpace()

		try:
			file_parts = self._files[file]
		except KeyError:
			file_parts = {}
			self._files[file] = file_parts

		for part_ind, part_bytes in parts:
			file_parts[part_ind] = max(file_parts.get(part_ind, 0), part_bytes)

		self._used_bytes += missing_bytes

		return missing_bytes
