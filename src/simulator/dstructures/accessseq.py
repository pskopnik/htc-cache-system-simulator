from array import array
import itertools
import math
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ..cache.accesses import SimpleAccessReader
from ..workload import Access, BytesSize, FileID, PartInd, PartSpec


class ReuseTimer(object):
	def __init__(self, accesses: SimpleAccessReader) -> None:
		self._reuse_ind: array[int] = self._build_reuse_ind(accesses)

	def __len__(self) -> int:
		return len(self._reuse_ind)

	def __iter__(self) -> Iterator[int]:
		return iter(self._reuse_ind)

	def reuse_time(self, ind: int) -> Optional[int]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return None

		return self._reuse_ind[ind] - ind

	def reuse_time_inf(self, ind: int) -> Union[int, float]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return math.inf

		return self._reuse_ind[ind] - ind

	def reuse_ind(self, ind: int) -> Optional[int]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return None

		return self._reuse_ind[ind]

	def reuse_ind_inf(self, ind: int) -> Union[int, float]:
		if self._reuse_ind[ind] >= len(self._reuse_ind):
			return math.inf

		return self._reuse_ind[ind]

	def reuse_ind_len(self, ind: int) -> int:
		return self._reuse_ind[ind]

	def _verify(self, accesses: Sequence[Access]) -> None:
		for ind, reuse_ind in enumerate(self._reuse_ind):
			file = accesses[ind].file
			for i in range(ind + 1, reuse_ind):
				assert accesses[i].file != file, 'found earlier reuse ind'
			if reuse_ind != len(self._reuse_ind):
				assert accesses[reuse_ind].file == file, 'invalid reuse ind'

	@staticmethod
	def _build_reuse_ind(accesses: SimpleAccessReader) -> 'array[int]':
		next_access: Dict[FileID, int] = {}
		accesses_length = len(accesses)
		reuse_ind: 'array[int]' = array('Q', itertools.repeat(0, accesses_length))

		for rev_ind, access in enumerate(reversed(accesses)):
			ind = accesses_length - rev_ind - 1
			reuse_ind[ind] = next_access.get(access.file, accesses_length)
			next_access[access.file] = ind

		return reuse_ind


class FullReuseIndex(object):
	"""Builds and provides access to a full index of file re-uses.
	"""

	def __init__(self, accesses: SimpleAccessReader) -> None:
		# Next or previous use index for the same file
		# If there is no following index to ind:
		# _*_use_ind[ind] >= len(_*_use_ind)
		self._prev_use_ind: array[int]
		self._next_use_ind: array[int]
		self._access_ts: array[int]
		# _parts_offset describes which indices in _parts and _part_sizes are
		# associated with access ind i:
		# _parts[range(
		#     _parts_offset[i],
		#     _parts_offset[i+1] if i+1 < len(_parts_offset) else len(_parts),
		# )]
		# And analogously for _part_sizes.
		# (_parts[i], _part_sizes[i]) give the full PartSpec.
		self._parts_offset: array[int]
		self._parts: array[int]
		self._part_sizes: array[BytesSize]

		(
			self._prev_use_ind,
			self._next_use_ind,
			self._access_ts,
			self._parts_offset,
			self._parts,
			self._part_sizes,
		) = self._build(accesses)

		parts_offset = self._parts_offset
		parts_length = len(self._parts)
		def parts_range(ind: int) -> range:
			"""Returns the range of indices of the parts of the access at ``ind``.

			"Fast" method.
			"""
			start = parts_offset[ind]
			end: int
			try:
				end = parts_offset[ind + 1]
			except IndexError:
				end = parts_length

			return range(start, end)

		self._parts_range: Callable[[int], range] = parts_range

	def __len__(self) -> int:
		return len(self._prev_use_ind)

	def prev_use_ind(self, ind: int) -> Optional[int]:
		if self._prev_use_ind[ind] >= len(self._prev_use_ind):
			return None

		return self._prev_use_ind[ind]

	def prev_use_ind_inf(self, ind: int) -> Union[int, float]:
		if self._prev_use_ind[ind] >= len(self._prev_use_ind):
			return math.inf

		return self._prev_use_ind[ind]

	def prev_use_ind_len(self, ind: int) -> int:
		return self._prev_use_ind[ind]

	def next_use_ind(self, ind: int) -> Optional[int]:
		if self._next_use_ind[ind] >= len(self._next_use_ind):
			return None

		return self._next_use_ind[ind]

	def next_use_ind_inf(self, ind: int) -> Union[int, float]:
		if self._next_use_ind[ind] >= len(self._next_use_ind):
			return math.inf

		return self._next_use_ind[ind]

	def next_use_ind_len(self, ind: int) -> int:
		return self._next_use_ind[ind]

	def access_ts(self, ind: int) -> int:
		return self._access_ts[ind]

	def parts(self, ind: int) -> List[PartSpec]:
		r = self._parts_range(ind)
		return list(zip(
			(self._parts[i] for i in r),
			(self._part_sizes[i] for i in r),
		))

	def accessed_after(self, after_ind: int, parts: Sequence[PartSpec]) -> List[PartSpec]:
		return list(self._accessed_following(after_ind, self._next_use_ind, parts))

	def accessed_before(self, before_ind: int, parts: Sequence[PartSpec]) -> List[PartSpec]:
		return list(self._accessed_following(before_ind, self._prev_use_ind, parts))

	def _accessed_following(
		self,
		start_ind: int,
		following_use_ind: 'array[int]',
		parts: Sequence[PartSpec],
	) -> Iterator[PartSpec]:
		# Tracks how many bytes of what part have not been found yet.
		# missing[part_ind] = (size_requested, max_size_found)
		# where size_requested is never changed
		# Possible optimisation: Maintaining missing as front-deletable,
		# sorted list suffices for fast lookup as parts are sorted in
		# self._parts as well.
		missing = {ind: (size, 0) for ind, size in parts}

		# Store variables in the local namespace
		parts_range = self._parts_range
		parts_array = self._parts
		part_sizes_array = self._part_sizes

		accesses_length = len(following_use_ind)
		next_ind = following_use_ind[start_ind]
		while len(missing) > 0 and next_ind < accesses_length:
			for i in parts_range(next_ind):
				part_ind = parts_array[i]
				if part_ind not in missing:
					continue

				part_size = part_sizes_array[i]
				size_requested, max_size_found = missing[part_ind]
				if part_size >= size_requested:
					del missing[part_ind]
					yield part_ind, size_requested
				elif part_size > max_size_found:
					missing[part_ind] = (size_requested, part_size)

			next_ind = following_use_ind[next_ind]

		for part_ind, (_, max_size_found) in missing.items():
			if max_size_found > 0:
				yield part_ind, max_size_found

	def _verify(self, accesses: Sequence[Access]) -> None:
		accesses_length = len(self._prev_use_ind)

		for ind in range(accesses_length):
			file = accesses[ind].file

			next_use_ind = self._next_use_ind[ind]
			for i in range(ind + 1, next_use_ind):
				assert accesses[i].file != file, 'found earlier reuse ind'
			if next_use_ind != accesses_length:
				assert accesses[next_use_ind].file == file, 'invalid reuse ind'

			prev_use_ind = self._prev_use_ind[ind]
			for i in range(prev_use_ind + 1 if prev_use_ind != accesses_length else 0, ind):
				assert accesses[i].file != file, 'found later reuse ind'
			if prev_use_ind != accesses_length:
				assert accesses[prev_use_ind].file == file, 'invalid reuse ind'


			assert accesses[ind].access_ts == self._access_ts[ind], 'mismatching access_ts'

			sorted_parts = sorted(accesses[ind].parts)
			r = self._parts_range(ind)
			assert [ind for ind, _ in sorted_parts] == [self._parts[i] for i in r], \
				'mismatching part indices'
			assert [size for _, size in sorted_parts] == [self._part_sizes[i] for i in r], \
				'mismatching part sizes'

	@staticmethod
	def _build(
		accesses: SimpleAccessReader,
	) -> Tuple['array[int]', 'array[int]', 'array[int]', 'array[int]', 'array[int]', 'array[int]']:
		# Possible optimisation: Could build prev_use_ind from next_use_ind by
		# "reversing the pointer direction".
		# Possible optimisation: Iterate fully before calling len() saves one
		# full iteration. The length is not necessary to know in advance.
		# Possible optimisation: Could specify the exact size of parts and
		# part_sizes by counting during the next_use_ind building. However,
		# this makes asymptotically no difference and is incompatible with the
		# two optimisations described before.

		prev_access: Dict[FileID, int] = {}
		accesses_length = len(accesses)

		prev_use_ind: 'array[int]' = array('Q', itertools.repeat(0, accesses_length))
		access_ts: 'array[int]' = array('Q', itertools.repeat(0, accesses_length))
		parts_offset: 'array[int]' = array('Q', itertools.repeat(0, accesses_length))
		parts: 'array[int]' = array('Q')
		part_sizes: 'array[int]' = array('Q')

		running_offset = 0
		for ind, access in enumerate(accesses):
			prev_use_ind[ind] = prev_access.get(access.file, accesses_length)
			prev_access[access.file] = ind

			access_ts[ind] = access.access_ts

			sorted_parts = sorted(access.parts)
			parts.extend(part_ind for part_ind, _ in sorted_parts)
			part_sizes.extend(part_size for _, part_size in sorted_parts)
			parts_offset[ind] = running_offset
			running_offset += len(sorted_parts)

		del prev_access

		return (
			prev_use_ind,
			ReuseTimer._build_reuse_ind(accesses),
			access_ts,
			parts_offset,
			parts,
			part_sizes,
		)


def change_to_active_files(full_reuse_index: FullReuseIndex, ind: int) -> int:
	accesses_length = len(full_reuse_index)

	accessed_before = full_reuse_index.prev_use_ind_len(ind) != accesses_length
	accessed_after = full_reuse_index.next_use_ind_len(ind) != accesses_length

	if accessed_before and not accessed_after:
		return -1
	elif accessed_after and not accessed_before:
		return 1
	else:
		return 0

def change_to_active_bytes(full_reuse_index: FullReuseIndex, ind: int) -> BytesSize:
	parts = full_reuse_index.parts(ind)

	accessed_after = full_reuse_index.accessed_after(ind, parts)
	accessed_before = full_reuse_index.accessed_before(ind, parts)

	# becoming active: bytes accessed, which are accessed in the future, which are not active
	becoming_active = count_diff_bytes(accessed_after, accessed_before)

	# becoming inactive: bytes accessed, which are active, which are not accessed in the future
	becoming_inactive = count_diff_bytes(accessed_before, accessed_after)

	return becoming_active - becoming_inactive

def count_diff_bytes(parts_1: Iterable[PartSpec], parts_2: Iterable[PartSpec]) -> BytesSize:
	"""Size of the complement of ``parts_2`` relative to ``parts_1``.

	``|parts_1 \\ parts_2|`` in mathematical set notation.

	Only works for unique part indicies in parts_1 and parts_2.

	There cannot be an output of the missing parts, because there is no way
	to specify "overhang" bytes of a part.
	"""

	parts_1_thing = {ind: size for ind, size in parts_1}

	count = 0
	for part_ind, part_size in parts_2:
		if part_ind in parts_1_thing:
			if part_size < parts_1_thing[part_ind]:
				count += parts_1_thing[part_ind] - part_size
			del parts_1_thing[part_ind]

	count += sum(parts_1_thing.values())

	return count

def multi_count_diff_bytes(parts_1: Iterable[PartSpec], parts_2: Iterable[PartSpec]) -> BytesSize:
	"""Size of the complement of ``parts_2`` relative to ``parts_1``.

	``|parts_1 \\ parts_2|`` in mathematical set notation.

	Can handle re-mentions of parts in the ``parts_1`` and ``parts_2``
	iterables. ``parts_1`` is fully read and only afterwards ``parts_2`` is
	iterated and changes are applied for each yielded part.
	"""

	# Tracks how many bytes of what part have not been found yet.
	# remaining[part_ind] = (size_requested, max_size_found)
	# where size_requested is never changed
	#remaining = {ind: (size, 0) for ind, size in parts_1}
	# Save for multi mentions in parts_1, uses the union
	remaining: Dict[PartInd, Tuple[BytesSize, BytesSize]] = dict()
	for part_ind, part_size in parts_1:
		if part_ind in remaining:
			remaining[part_ind] = (max(part_size, remaining[part_ind][0]), 0)
		else:
			remaining[part_ind] = (part_size, 0)

	for part_ind, part_size in parts_2:
		if part_ind not in remaining:
			continue

		size_requested, max_size_found = remaining[part_ind]
		if part_size >= size_requested:
			del remaining[part_ind]
		elif part_size > max_size_found:
			remaining[part_ind] = (size_requested, part_size)

	return sum(
		size_requested - max_size_found for _, (size_requested, max_size_found) in remaining.items()
	)
