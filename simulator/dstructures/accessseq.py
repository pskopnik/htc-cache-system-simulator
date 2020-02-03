from array import array
import itertools
import math
from typing import Dict, Iterator, Optional, Sequence, Union

from ..cache.accesses import SimpleAccessReader
from ..workload import Access, FileID


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
				if accesses[i].file == file:
					raise Exception('Found earlier reuse ind', ind, i, reuse_ind, file)
			if reuse_ind != len(self._reuse_ind):
				if accesses[reuse_ind].file != file:
					raise Exception('Invalid reuse ind', reuse_ind, file)

	@staticmethod
	def _build_reuse_ind(accesses: SimpleAccessReader) -> 'array[int]':
		next_access: Dict[FileID, int] = {}
		accesses_length = len(accesses)
		reuse_time: array[int] = array('Q', itertools.repeat(0, accesses_length))

		for rev_ind, access in enumerate(reversed(accesses)):
			ind = accesses_length - rev_ind - 1
			reuse_time[ind] = next_access.get(access.file, accesses_length)
			next_access[access.file] = ind

		return reuse_time
