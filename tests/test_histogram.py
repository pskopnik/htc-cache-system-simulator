from array import array
import itertools
import pytest # type: ignore[import]
import random
from typing import (
	AbstractSet,
	Callable,
	Collection,
	Iterable,
	Iterator,
	List,
	Optional,
	overload,
	Reversible,
	Tuple,
	TypeVar,
	Sequence,
	Sized,
	Union,
	ValuesView,
)
from typing_extensions import Protocol

from simulator.dstructures.binning import Binner, LogBinner, NoneBinner
from simulator.dstructures.histogram import (
	BinnedCounters,
	BinnedProbabilities,
	CountedProbabilities,
	HalvingBinnedCounters,
)

from .test_binning import random_bin_nums

_T = TypeVar('_T')
_T_num = TypeVar('_T_num', int, float)
_T_co = TypeVar('_T_co', covariant=True)


class _BinnedArrayLike(Collection[int], Protocol[_T_num]):
	@property
	def binner(self) -> Binner: ...

	@property
	def bounded(self) -> bool: ...

	@property
	def total(self) -> _T_num: ...

	@property
	def bin_data(self) -> 'array[_T_num]': ...

	def __getitem__(self, key: int) -> _T_num: ...
	@overload
	def get(self, key: int) -> Optional[_T_num]: ...
	@overload
	def get(self, key: int, default: Union[_T_num, _T]) -> Union[_T_num, _T]: ...
	def items(self) -> AbstractSet[Tuple[int, _T_num]]: ...
	def keys(self) -> AbstractSet[int]: ...
	def values(self) -> ValuesView[_T_num]: ...


def _drop_last_while(pred: Callable[[_T], bool], it: Iterable[_T]) -> Iterable[_T]:
	reversible_it: Reversible[_T]
	if isinstance(it, Reversible):
		reversible_it = it
	else:
		reversible_it = list(it)

	return reversed(list(
		itertools.dropwhile(pred, reversed(reversible_it)),
	))

def _assert_binned_array_equal(a: _BinnedArrayLike[_T_num], values: Sequence[_T_num]) -> None:
	b = a.binner

	def slice_for_assert(it: Iterable[_T]) -> Iterable[_T]:
		if b.bounded:
			return it
		else:
			return itertools.islice(it, len(values))

	def assert_length(container: Sized) -> None:
		if b.bounded:
			assert len(container) == len(values)
		else:
			with pytest.raises(TypeError):
				len(container)

	assert_length(a)

	for num, value in slice_for_assert(zip(random_bin_nums(b), values)):
		assert a[num] == value

		assert num in a
		assert value in a.values()
		assert (num, value) in a.items()
		assert (num, value + 1) not in a.items()

	assert sum(a.bin_data) == a.total
	assert a.total == sum(values)

	if b.bounded:
		assert list(a.bin_data) == list(values)
	else:
		is_zero = lambda val: val == 0
		assert (
				list(_drop_last_while(is_zero, a.bin_data))
			==
				list(_drop_last_while(is_zero, values))
		)

	assert list(slice_for_assert(a.values())) == list(values)
	assert_length(a.items())

	assert list(slice_for_assert(a.items())) == list(zip(b.bin_edges(), values))
	assert_length(a.values())

def _assert_binned_array_basics(
	b: Binner,
	a: _BinnedArrayLike[_T_num],
	check_count: int = 20,
	post_check_count: int = 10,
) -> None:
	def slice_for_assert(it: Iterable[_T]) -> Iterable[_T]:
		if b.bounded:
			return it
		else:
			return itertools.islice(it, check_count + post_check_count)

	assert a.binner is b
	assert a.bounded == b.bounded

	# test len(.) for all views
	if b.bounded:
		assert len(a) == b.bins
		assert len(a.keys()) == b.bins
		assert len(a.values()) == b.bins
		assert len(a.items()) == b.bins

		# Only test length of bin_data if bounded (pre-allocated bin_data)
		assert len(a.bin_data) == b.bins
	else:
		with pytest.raises(TypeError):
			len(a)
		with pytest.raises(TypeError):
			len(a.keys())
		with pytest.raises(TypeError):
			len(a.values())
		with pytest.raises(TypeError):
			len(a.items())

	assert list(slice_for_assert(a)) == list(slice_for_assert(b.bin_edges()))

	assert list(slice_for_assert(a.keys())) == list(slice_for_assert(b.bin_edges()))

def test_bounded_binned_counters() -> None:
	b = LogBinner(first=9, last=20)
	c = BinnedCounters(b)

	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, [0] * b.bins)

	for num in random_bin_nums(b):
		c.increment(num)
	_assert_binned_array_equal(c, [1] * b.bins)

	for num in random_bin_nums(b):
		c[num] += 10
	_assert_binned_array_equal(c, [11] * b.bins)

	for num in random_bin_nums(b):
		c.decrement(num)
	_assert_binned_array_equal(c, [10] * b.bins)

	for num in random_bin_nums(b):
		c.decrement(num, decr=5)
	_assert_binned_array_equal(c, [5] * b.bins)

	for num in random_bin_nums(b):
		c.increment(num, incr=5)
	_assert_binned_array_equal(c, [10] * b.bins)

	for num in random_bin_nums(b):
		c[num] = 1
	_assert_binned_array_equal(c, [1] * b.bins)

	c.reset()
	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, [0] * b.bins)

	# test __getitem__() resolving correctly for bin limits
	for bin in range(b.bins):
		first, past = b.bin_limits(bin)
		c[first] = bin
		if past == -1:
			continue
		assert c[first] == c[past - 1]
	_assert_binned_array_equal(c, list(range(b.bins)))

def test_unbounded_binned_counters() -> None:
	b = LogBinner()
	c = BinnedCounters(b)

	# TODO: extract specifics into fixture, then merge with bounded test

	check_count = 20
	post_check_count = 10

	def counters_list_from_value(val: int) -> List[int]:
		return [val] * check_count + [0] * post_check_count

	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, counters_list_from_value(0))

	for num in random_bin_nums(b, n=check_count):
		c.increment(num)
	_assert_binned_array_equal(c, counters_list_from_value(1))

	for num in random_bin_nums(b, n=check_count):
		c[num] += 10
	_assert_binned_array_equal(c, counters_list_from_value(11))

	for num in random_bin_nums(b, n=check_count):
		c.decrement(num)
	_assert_binned_array_equal(c, counters_list_from_value(10))

	for num in random_bin_nums(b, n=check_count):
		c.decrement(num, decr=5)
	_assert_binned_array_equal(c, counters_list_from_value(5))

	for num in random_bin_nums(b, n=check_count):
		c.increment(num, incr=5)
	_assert_binned_array_equal(c, counters_list_from_value(10))

	for num in random_bin_nums(b, n=check_count):
		c[num] = 1
	_assert_binned_array_equal(c, counters_list_from_value(1))

	c.reset()
	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, counters_list_from_value(0))

	# test __getitem__() resolving correctly for bin limits
	for bin in range(check_count):
		first, past = b.bin_limits(bin)
		c[first] = bin
		if past == -1:
			continue
		assert c[first] == c[past - 1]
	_assert_binned_array_equal(c, list(range(check_count)) + [0] * post_check_count)

def test_halving_binned_counters() -> None:
	# TODO: test unbounded
	b = LogBinner(first=9, last=20)
	# TODO: test passing max_total and passing (max_bin, max_total)
	c = HalvingBinnedCounters(b, factor=1/2, max_bin=10)

	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, [0] * b.bins)

	vals = [8] * b.bins
	for num, val in zip(random_bin_nums(b), vals):
		c[num] = val

	_assert_binned_array_equal(c, vals)

	# Pick number to change
	num = b.bin_limits(random.randrange(b.bins))[0]

	# No update as 10 is the maximum allowed value for each bin
	c[num] = 10
	vals[b(num)] = 10
	_assert_binned_array_equal(c, vals)

	# cause halving through c[.] = ...
	c[num] = 11
	vals = [val // 2 for val in vals]
	vals[b(num)] = 5
	_assert_binned_array_equal(c, vals)

	# cause halving through c.increment
	c.increment(num, incr=6)
	vals = [val // 2 for val in vals]
	vals[b(num)] = 5
	_assert_binned_array_equal(c, vals)

	# cause halving through c.decrement
	c.decrement(num, decr=-6)
	vals = [val // 2 for val in vals]
	vals[b(num)] = 5
	_assert_binned_array_equal(c, vals)

	c.reset()
	_assert_binned_array_basics(b, c)
	_assert_binned_array_equal(c, [0] * b.bins)

def test_binned_probabilities() -> None:
	# TODO: test unbounded
	b = LogBinner(first=9, last=20)
	c = BinnedCounters(b)

	vals = [0] * 4 + [1] * 4 + [3] * 4
	for num, val in zip(random_bin_nums(b), vals):
		c[num] = val

	p = BinnedProbabilities.from_counters(c)

	_assert_binned_array_basics(b, p)

	assert sum(p.bin_data) == p.total
	assert p.total == 1.0

	_assert_binned_array_equal(p, [0.0] * 4 + [1/16] * 4 + [3/16] * 4)

	# Pick number to change
	num = b.bin_limits(random.randrange(b.bins))[0]

	with pytest.raises(TypeError):
		p.increment(num)
	with pytest.raises(TypeError):
		p.decrement(num)
	with pytest.raises(TypeError):
		p.reset()
	with pytest.raises(TypeError):
		p[num] = 0.2

def test_counted_probabilities() -> None:
	# TODO: test unbounded
	b = LogBinner(first=9, last=20)
	c = BinnedCounters(b)

	vals = [0] * 10 + [8] * 2
	for num, val in zip(random_bin_nums(b), vals):
		c[num] = val

	p = CountedProbabilities.from_counters(c)
	with pytest.raises(ValueError):
		# Raises as ewma_factor must be passed
		p.update(c)

	p = CountedProbabilities.from_counters(c, ewma_factor=1/4)
	# Internal counters should now be: [0] * 10 + [8] * 2

	_assert_binned_array_basics(b, p)

	assert sum(p.bin_data) == p.total
	assert p.total == 1.0

	_assert_binned_array_equal(p, [0.0] * 10 + [1/2] * 2)

	vals = [8] * 2 + [0] * 10
	for num, val in zip(random_bin_nums(b), vals):
		c[num] = val

	p.update(c)
	# Internal counters should now be: [2] * 2 + [0] * 8 + [6] * 2

	assert sum(p.bin_data) == p.total
	assert p.total == 1.0

	_assert_binned_array_equal(p, [2/8/2] * 2 + [0.0] * 8 + [6/8/2] * 2)

	p.update(c, ewma_factor=1/2)
	# Internal counters should now be: [5] * 2 + [0] * 8 + [3] * 2

	assert sum(p.bin_data) == p.total
	assert p.total == 1.0

	_assert_binned_array_equal(p, [5/8/2] * 2 + [0.0] * 8 + [3/8/2] * 2)

	# Pick number to change
	num = b.bin_limits(random.randrange(b.bins))[0]

	with pytest.raises(TypeError):
		p.increment(num)
	with pytest.raises(TypeError):
		p.decrement(num)
	with pytest.raises(TypeError):
		p.reset()
	with pytest.raises(TypeError):
		p[num] = 0.2
