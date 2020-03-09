import itertools
import pytest # type: ignore[import]
import random
from typing import cast, Iterable, Iterator, List, TypeVar

from simulator.dstructures.binning import (
	BinnedMapping,
	BinnedSparseMapping,
	Binner,
	LinearBinner,
	LogBinner,
)

_T = TypeVar('_T')

def random_bin_nums(binner: Binner, past_size: int=10000, n: int=-1) -> Iterator[int]:
	it: Iterable[int]
	if not binner.bounded and n == -1:
		it = itertools.count()
	else:
		it = range(min(filter(lambda x: x != -1, [binner.bins, n])))

	for i in it:
		first, past = binner.bin_limits(i)
		yield random.randrange(first, past if past != -1 else first + past_size)

def _assert_binner_equals(binner: Binner, edges: List[int]) -> None:
	assert binner.bins == len(edges)
	assert list(binner.bin_edges()) == edges
	limits = zip(edges, itertools.chain(edges[1:], (-1,)))
	assert [binner.bin_limits(bin) for bin in range(binner.bins)] == list(limits)

def _assert_bin_limits(binner: Binner, bins: int) -> None:
	next_first = 0
	for i in range(bins):
		first, past = binner.bin_limits(i)
		assert first == next_first
		assert binner(first) == i
		if past > 0:
			assert binner(past - 1) == i
			assert binner(past) == i + 1
		next_first = past

def _assert_bin_edges(binner: Binner, bins: int) -> None:
	for i, edge in zip(range(bins), binner.bin_edges()):
		assert binner(edge) == i
		if i > 0:
			assert binner(edge - 1) == i - 1

def _assert_binning(binner: Binner, min: int, max: int, n: int=1000) -> None:
	for num in (random.randrange(min, max) for _ in range(n)):
		bin = binner(num)
		assert bin >= 0
		assert not binner.bounded or bin < binner.bins
		bin_first, bin_past = binner.bin_limits(bin)
		assert num >= bin_first
		assert binner.bounded or num < bin_past

def test_log_binner_basics() -> None:

	# Test basic calculations by checking different step values with first=5
	# and last=8.
	#
	#          2^5   2^6   2^7   2^8
	#           32    64   128   256
	# step=1  <     ][    ][    ][ >
	# step=2  <           ][       >
	# step=3  <                 ][ >
	# step=4  <                    >

	b = LogBinner(first=5, last=8, step=1)
	_assert_binner_equals(b, [0, 64, 128, 256])

	b = LogBinner(first=5, last=8, step=2)
	_assert_binner_equals(b, [0, 128])

	b = LogBinner(first=5, last=8, step=3)
	_assert_binner_equals(b, [0, 256])

	b = LogBinner(first=5, last=8, step=4)
	_assert_binner_equals(b, [0])

@pytest.mark.parametrize('step', (1, 2, 3, 4)) # type: ignore[misc]
def test_bounded_log_binner(step: int) -> None:
	b = LogBinner(first=10, last=40, step=step)

	assert b.bins > 0
	assert b.bounded == True

	_assert_bin_limits(b, b.bins)
	_assert_bin_edges(b, b.bins)
	_assert_binning(b, 0, 2 ** 45)

	num = random.randrange(2 ** 10, 2 ** 11)
	for i in range(7):
		old_bin = b(num)
		num *= 2 ** step
		assert b(num) == old_bin + 1

@pytest.mark.parametrize('step', (1, 2, 3, 4)) # type: ignore[misc]
def test_unbounded_log_binner(step: int) -> None:
	b = LogBinner(first=10, step=step)

	assert b.bins == -1
	assert b.bounded == False

	_assert_bin_limits(b, 1000)
	_assert_bin_edges(b, 1000)
	_assert_binning(b, 0, 2 ** 45)

	num = random.randrange(2 ** 10, 2 ** 50)
	for _ in range(10):
		old_bin = b(num)
		num *= 2 ** step
		assert b(num) == old_bin + 1

@pytest.mark.parametrize('width', (1, 2, 10, 100)) # type: ignore[misc]
def test_linear_binner(width: int) -> None:
	b = LinearBinner(width=width)

	assert b.bins == -1
	assert b.bounded == False

	_assert_bin_limits(b, 1000)
	_assert_bin_edges(b, 1000)
	_assert_binning(b, 0, 2 ** 45)

	num = random.randrange(0, width)
	for i in range(10):
		assert b(num) == i
		num += width

# Following is an implementation of test_none_binner using a parametrised
# fixture.

# TODO: mypy error
# Function is missing a type annotation for one or more arguments [no-untyped-def]

@pytest.fixture(params=[ # type: ignore[misc]
	(LogBinner, (1,), {}), # type: ignore[no-untyped-def]
	(LogBinner, (2,), {}),
	(LogBinner, (3,), {}),
	(LogBinner, (4,), {}),
	(LinearBinner, tuple(), {}),
	(LinearBinner, (1,), {}),
	(LinearBinner, (2,), {}),
	(LinearBinner, (10,), {}),
	(LinearBinner, (100,), {}),
])
def unbounded_binner(request) -> Binner:
	return cast(Binner, request.param[0](*request.param[1], **request.param[2]))

def test_unbounded_binner(unbounded_binner: Binner) -> None:
	assert unbounded_binner.bins == -1
	assert unbounded_binner.bounded == False

	_assert_bin_limits(unbounded_binner, 1000)
	_assert_bin_edges(unbounded_binner, 1000)
	_assert_binning(unbounded_binner, 0, 2 ** 45)

def test_bounded_binned_mapping() -> None:
	b = LogBinner(first=10, last=20, step=1)
	m: BinnedMapping[List[int]] = BinnedMapping(b, list)

	# test binner
	assert m.binner is b

	# test bounded
	assert b.bounded == True

	# test __len__()
	assert len(m) == b.bins

	# test __iter__()
	assert list(m) == list(b.bin_edges())

	# test keys()
	assert list(m.keys()) == list(b.bin_edges())

	# set-up: append edge value to each bin
	for edge, num in zip(b.bin_edges(), random_bin_nums(b)):
		m[num].append(edge)

	# test __getitem__()
	for edge, num in zip(b.bin_edges(), random_bin_nums(b)):
		assert m[num] == [edge]

	# test items()
	assert list(m.items()) == list(zip(b.bin_edges(), map(lambda edge: [edge], b.bin_edges())))

	# test values()
	assert list(m.values()) == list(map(lambda edge: [edge], b.bin_edges()))

	# test __getitem__() resolving correctly for bin limits
	for bin in range(b.bins):
		first, past = b.bin_limits(bin)
		if past == -1:
			continue
		assert m[first] is m[past - 1]

	# test values_until()
	until_bin = b.bins // 2
	until_num = b.bin_limits(until_bin)[0]
	assert list(m.values_until(until_num, half_open=True)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), until_bin)),
	)
	assert list(m.values_until(until_num, half_open=False)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), until_bin + 1)),
	)

	# test values_from()
	from_bin = b.bins // 2
	from_num = b.bin_limits(from_bin)[0]
	assert list(m.values_from(from_num, half_open=True)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), from_bin + 1, None)),
	)
	assert list(m.values_from(from_num, half_open=False)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), from_bin, None)),
	)

	# test bin_limits()
	for bin in range(b.bins):
		assert m.bin_limits(bin) == b.bin_limits(bin)

	# test bin_limits_from_num()
	for bin, edge in enumerate(b.bin_edges()):
		assert m.bin_limits_from_num(edge) == b.bin_limits(bin)

def test_unbounded_binned_mapping() -> None:
	b = LogBinner(first=10, step=1)
	m: BinnedMapping[List[int]] = BinnedMapping(b, list)

	check_count = 20

	def sl(it: Iterable[_T], n: int=-1, sub: int=0) -> Iterable[_T]:
		if n == -1:
			n = check_count

		n -= sub

		return itertools.islice(it, n)

	# test binner
	assert m.binner is b

	# test bounded
	assert b.bounded == False

	# test __len__()
	with pytest.raises(TypeError):
		assert len(m) == b.bins

	# test __iter__()
	assert list(sl(m)) == list(sl(b.bin_edges()))

	# test keys()
	assert list(sl(m.keys())) == list(sl(b.bin_edges()))

	# set-up: append edge value to each bin
	for edge, num in zip(b.bin_edges(), random_bin_nums(b, n=check_count)):
		m[num].append(edge)

	# test __getitem__()
	for edge, num in zip(b.bin_edges(), random_bin_nums(b, n=check_count)):
		assert m[num] == [edge]

	# test items()
	assert (
			list(sl(m.items()))
		==
			list(zip(b.bin_edges(), sl(map(lambda edge: [edge], b.bin_edges()))))
	)

	# test values()
	assert list(sl(m.values())) == list(sl(map(lambda edge: [edge], b.bin_edges())))

	# test __getitem__() resolving correctly for bin limits
	for bin in range(check_count):
		first, past = b.bin_limits(bin)
		if past == -1:
			continue
		assert m[first] is m[past - 1]

	# test values_until()
	until_bin = check_count // 2
	until_num = b.bin_limits(until_bin)[0]
	assert list(m.values_until(until_num, half_open=True)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), until_bin)),
	)
	assert list(m.values_until(until_num, half_open=False)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), until_bin + 1)),
	)

	# test values_from()
	from_bin = check_count // 2
	from_num = b.bin_limits(from_bin)[0]
	assert list(sl(m.values_from(from_num, half_open=True), sub=from_bin + 1)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), from_bin + 1, check_count)),
	)
	assert list(sl(m.values_from(from_num, half_open=False), sub=from_bin)) == list(
		map(lambda edge: [edge], itertools.islice(b.bin_edges(), from_bin, check_count)),
	)

	# test bin_limits()
	for bin in range(check_count):
		assert m.bin_limits(bin) == b.bin_limits(bin)

	# test bin_limits_from_num()
	for bin, edge in enumerate(sl(b.bin_edges())):
		assert m.bin_limits_from_num(edge) == b.bin_limits(bin)

def test_unbounded_binned_sparse_mapping() -> None:
	b = LogBinner(first=10, step=1)
	m: BinnedSparseMapping[List[int]] = BinnedSparseMapping(b, list)

	# test binner
	assert m.binner is b

	# test bounded
	assert b.bounded == False

	# empty mapping

	assert len(m) == 0
	assert list(m) == []

	# set-up: append edge value to some bins
	m_edges = list(itertools.islice(b.bin_edges(), 0, 20, 4))
	for edge in m_edges:
		m[edge].append(edge)

	# test __len__()
	assert len(m) == len(m_edges)

	# test __iter__()
	assert list(m) == m_edges

	# test keys()
	assert list(m.keys()) == m_edges

	# test __getitem__()
	for edge in m_edges:
		assert m[edge] == [edge]

	# test items()
	assert list(m.items()) == list(zip(m_edges, map(lambda edge: [edge], m_edges)))

	# test values()
	assert list(m.values()) == list(map(lambda edge: [edge], m_edges))

	# test __getitem__() resolving correctly for bin limits
	for edge in m_edges:
		first, past = b.bin_limits(b(edge))
		if past == -1:
			continue
		assert m[first] is m[past - 1]

	# test items_until()
	until_edge_ind = len(m_edges) // 2
	until_num = m_edges[until_edge_ind]
	assert list(m.items_until(until_num, half_open=True)) == list(zip(
		map(lambda edge: b(edge), itertools.islice(m_edges, until_edge_ind)),
		map(lambda edge: [edge], itertools.islice(m_edges, until_edge_ind)),
	))
	assert list(m.items_until(until_num, half_open=False)) == list(zip(
		map(lambda edge: b(edge), itertools.islice(m_edges, until_edge_ind + 1)),
		map(lambda edge: [edge], itertools.islice(m_edges, until_edge_ind + 1)),
	))

	# test items_from()
	from_edge_ind = len(m_edges) // 2
	from_num = m_edges[from_edge_ind]
	assert list(m.items_from(from_num, half_open=True)) == list(zip(
		map(lambda edge: b(edge), itertools.islice(m_edges, from_edge_ind + 1, None)),
		map(lambda edge: [edge], itertools.islice(m_edges, from_edge_ind + 1, None)),
	))
	assert list(m.items_from(from_num, half_open=False)) == list(zip(
		map(lambda edge: b(edge), itertools.islice(m_edges, from_edge_ind, None)),
		map(lambda edge: [edge], itertools.islice(m_edges, from_edge_ind, None)),
	))

	# test bin_limits_from_num()
	for edge in m_edges:
		assert m.bin_limits_from_num(edge) == b.bin_limits(b(edge))

	# test __delitem__()
	for edge in m_edges[:len(m_edges)//2]:
		del m[edge]
	m_edges = m_edges[len(m_edges)//2:]
	assert len(m) == len(m_edges)
	assert list(m) == m_edges
