import itertools
import math
import pytest # type: ignore[import]
import random
from typing import cast, Iterable, Iterator, List, Sequence

from simulator.workload import Access, FileID
from simulator.dstructures.accessseq import ReuseTimer

def access_seq_from_files(files: Iterable[FileID]) -> List[Access]:
	def generator() -> Iterator[Access]:
		parts = [(0, 1)]

		for ts, file in enumerate(files):
			yield Access(ts, file, parts)

	return list(generator())

def generate_access_seq(n_accesses: int, n_files: int) -> List[Access]:
	files = list(map(str, range(n_files)))

	return access_seq_from_files((random.choice(files) for _ in range(n_accesses)))

def _assert_reuse_timer_equals(r: ReuseTimer, reuse_inds: Sequence[int]) -> None:
	l = len(reuse_inds)
	assert len(r) == l
	assert list(r) == reuse_inds

	for i, reuse_ind in enumerate(reuse_inds):
		assert r.reuse_ind_len(i) == reuse_ind
		assert r.reuse_ind(i) == (reuse_ind if reuse_ind != l else None)
		assert r.reuse_ind_inf(i) == (reuse_ind if reuse_ind != l else math.inf)

		assert r.reuse_time(i) == (reuse_ind - i if reuse_ind != l else None)
		assert r.reuse_time_inf(i) == (reuse_ind - i if reuse_ind != l else math.inf)

def test_reuse_timer_basics() -> None:
	accessed_files = ['a', 'b', 'c', 'a', 'b']
	accesses = access_seq_from_files(accessed_files)

	r = ReuseTimer(accesses)
	r._verify(accesses)
	_assert_reuse_timer_equals(r, [3, 4, 5, 5, 5])

@pytest.mark.parametrize('n_accesses,n_files', ( # type: ignore[misc]
	(100, 10),
	(100, 90),
	(1000, 10),
	(1000, 100),
	(1000, 900),
))
def test_reuse_timer_random(n_accesses: int, n_files: int) -> None:
	accesses = generate_access_seq(n_accesses, n_files)
	r = ReuseTimer(accesses)
	r._verify(accesses)
