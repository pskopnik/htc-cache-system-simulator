import itertools
import math
import pytest
import random
from typing import cast, Iterable, Iterator, List, Sequence

from simulator.workload import Access, FileID
from simulator.dstructures.accessseq import (
	change_to_active_bytes,
	change_to_active_files,
	ReuseTimer,
	FullReuseIndex,
)

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

@pytest.mark.parametrize('n_accesses,n_files', (
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

def test_full_reuse_index_reuses_after_and_before() -> None:
	accesses = [
		Access(0, 'a', [(0, 1)]),
		Access(1, 'a', [(0, 1), (1, 3)]),
		Access(2, 'a', [(1, 3)]),
		Access(3, 'b', [(0, 4)]),
		Access(4, 'b', [(0, 2)]),
		Access(5, 'b', [(0, 1)]),
		Access(6, 'b', [(0, 3)]),
	]
	fri = FullReuseIndex(accesses)

	assert list(fri.reuses_after(0, fri.parts(0))) == [(1, 0, 1, 1)]
	assert list(fri.reuses_after(1, fri.parts(1))) == [(2, 1, 3, 3)]
	assert list(fri.reuses_after(2, fri.parts(2))) == []
	assert list(fri.reuses_after(3, fri.parts(3))) == [(4, 0, 2, 2), (6, 0, 3, 1)]
	assert list(fri.reuses_after(4, fri.parts(4))) == [(5, 0, 1, 1), (6, 0, 2, 1)]
	assert list(fri.reuses_after(5, fri.parts(5))) == [(6, 0, 1, 1)]

	assert list(fri.reuses_before(0, fri.parts(0))) == []
	assert list(fri.reuses_before(1, fri.parts(1))) == [(0, 0, 1, 1)]
	assert list(fri.reuses_before(2, fri.parts(2))) == [(1, 1, 3, 3)]
	assert list(fri.reuses_before(3, fri.parts(3))) == []
	assert list(fri.reuses_before(4, fri.parts(4))) == [(3, 0, 2, 2)]
	assert list(fri.reuses_before(5, fri.parts(5))) == [(4, 0, 1, 1)]
	assert list(fri.reuses_before(6, fri.parts(6))) == [(5, 0, 1, 1), (4, 0, 2, 1), (3, 0, 3, 1)]

def test_full_reuse_index_accessed_after_and_before() -> None:
	accesses = [
		Access(0, 'a', [(0, 1)]),
		Access(1, 'a', [(0, 1), (1, 3)]),
		Access(2, 'a', [(1, 3)]),
		Access(3, 'b', [(0, 4)]),
		Access(4, 'b', [(0, 2)]),
		Access(5, 'b', [(0, 1)]),
		Access(6, 'b', [(0, 3)]),
	]
	fri = FullReuseIndex(accesses)

	assert fri.accessed_after(0, fri.parts(0)) == [(0, 1)]
	assert fri.accessed_after(1, fri.parts(1)) == [(1, 3)]
	assert fri.accessed_after(2, fri.parts(2)) == []
	assert fri.accessed_after(3, fri.parts(3)) == [(0, 3)]
	assert fri.accessed_after(4, fri.parts(4)) == [(0, 2)]
	assert fri.accessed_after(5, fri.parts(5)) == [(0, 1)]
	assert fri.accessed_after(6, fri.parts(6)) == []

	assert fri.accessed_before(0, fri.parts(0)) == []
	assert fri.accessed_before(1, fri.parts(1)) == [(0, 1)]
	assert fri.accessed_before(2, fri.parts(2)) == [(1, 3)]
	assert fri.accessed_before(3, fri.parts(3)) == []
	assert fri.accessed_before(4, fri.parts(4)) == [(0, 2)]
	assert fri.accessed_before(5, fri.parts(5)) == [(0, 1)]
	assert fri.accessed_before(6, fri.parts(6)) == [(0, 3)]

@pytest.mark.parametrize('n_accesses,n_files', (
	(100, 10),
	(100, 90),
	(1000, 10),
	(1000, 100),
	(1000, 900),
))
def test_full_reuse_index_random(n_accesses: int, n_files: int) -> None:
	accesses = generate_access_seq(n_accesses, n_files)
	fri = FullReuseIndex(accesses)
	fri._verify(accesses)

@pytest.mark.parametrize('n_accesses,n_files', (
	(100, 10),
	(100, 90),
	(1000, 10),
	(1000, 100),
	(1000, 900),
))
def test_change_to_active_files_random(n_accesses: int, n_files: int) -> None:
	accesses = generate_access_seq(n_accesses, n_files)

	fri = FullReuseIndex(accesses)
	a = list(itertools.accumulate(change_to_active_files(fri, i) for i in range(len(accesses))))

	assert a[-1] == 0
	for active_files in a:
		active_files <= n_files

def test_change_to_active_files() -> None:
	accesses = access_seq_from_files(['a', 'b', 'b', 'c', 'd', 'b', 'a'])

	fri = FullReuseIndex(accesses)
	a = list(itertools.accumulate(change_to_active_files(fri, i) for i in range(len(accesses))))

	assert a == [1, 2, 2, 2, 2, 1, 0]

@pytest.mark.parametrize('n_accesses,n_files', (
	(100, 10),
	(100, 90),
	(1000, 10),
	(1000, 100),
	(1000, 900),
))
def test_change_to_active_bytes_random(n_accesses: int, n_files: int) -> None:
	accesses = generate_access_seq(n_accesses, n_files)

	fri = FullReuseIndex(accesses)
	a = list(itertools.accumulate(change_to_active_bytes(fri, i) for i in range(len(accesses))))

	assert a[-1] == 0
	for active_bytes in a:
		# From each file only one byte is accessed
		active_bytes <= n_files

def test_change_to_active_bytes() -> None:
	accesses = access_seq_from_files(['a', 'b', 'b', 'c', 'd', 'b', 'a'])

	fri = FullReuseIndex(accesses)
	a = list(itertools.accumulate(change_to_active_bytes(fri, i) for i in range(len(accesses))))

	assert a == [1, 2, 2, 2, 2, 1, 0]
