from array import array
import itertools
import math
from typing import (
	AbstractSet,
	Callable,
	cast,
	Generic,
	Iterable,
	Iterator,
	Mapping,
	Optional,
	Tuple,
	TypeVar,
	Union,
	ValuesView,
)

from .binning import Binner


_T = TypeVar('_T', int, float)
_T_inner = TypeVar('_T_inner', int, float)

class _BinnedArray(Generic[_T], Mapping[int, _T]):
	_type_code: str = 'Q'
	_zero_value: _T = cast(_T, 0)


	class _ValuesView(Generic[_T_inner], ValuesView[_T_inner]):
		def __init__(self, b_array: '_BinnedArray[_T_inner]') -> None:
			super(_BinnedArray._ValuesView, self).__init__(b_array)
			self._b_array: _BinnedArray[_T_inner] = b_array

		def __len__(self) -> int:
			return len(self._b_array)

		def __contains__(self, el: object) -> bool:
			for count in self._b_array._bins:
				if count == el:
					return True
			if not self._b_array.bounded and el == self._b_array._zero_value:
				return True
			return False

		def __iter__(self) -> Iterator[_T_inner]:
			if self._b_array._binner.bins == -1:
				zero_value = self._b_array._zero_value
				return itertools.chain(self._b_array._bins, itertools.repeat(zero_value))
			else:
				return iter(self._b_array._bins)


	class _ItemsView(Generic[_T_inner], AbstractSet[Tuple[int, _T_inner]]):
		def __init__(self, b_array: '_BinnedArray[_T_inner]') -> None:
			self._b_array: _BinnedArray[_T_inner] = b_array

		def __len__(self) -> int:
			return len(self._b_array)

		def __contains__(self, el: object) -> bool:
			try:
				el = cast('Tuple[int, _T_inner]', el)
				if len(el) != 2:
					return False
				return self._b_array[el[0]] == el[1]
			except (KeyError, TypeError):
				return False

		def __iter__(self) -> Iterator[Tuple[int, _T_inner]]:
			if self._b_array._binner.bins == -1:
				zero_value = self._b_array._zero_value
				return zip(
					self._b_array._binner.bin_edges(),
					itertools.chain(self._b_array._bins, itertools.repeat(zero_value)),
				)
			else:
				return zip(self._b_array._binner.bin_edges(), self._b_array._bins)


	def __init__(self, binner: Binner) -> None:
		self._binner: Binner = binner
		self._bins: array[_T]
		self._total: _T

		self._get_bin: Callable[[int], _T]
		self._set_bin: Callable[[int, _T], None]
		self._increment: Callable[[int, _T], None]

		if self._binner.bins != -1:
			self._get_bin = lambda bin: self._bins[bin]
			self._set_bin = self._set_bin_bounded
			self._increment = self._increment_bounded
		else:
			self._get_bin = self._get_bin_unbounded
			self._set_bin = self._set_bin_unbounded
			self._increment = self._increment_unbounded

		self._init_bins_and_total()

	def _init_bins_and_total(self) -> None:
		zero_value = self._zero_value

		it: Iterable[_T]
		if self._binner.bins == -1:
			it = []
		else:
			it = (zero_value for _ in range(self._binner.bins))

		self._bins = array(self._type_code, it)
		self._total = zero_value

	def _get_bin_unbounded(self, bin: int) -> _T:
		try:
			return self._bins[bin]
		except IndexError:
			return self._zero_value

	def _increment_unbounded(self, bin: int, incr: _T) -> None:
		try:
			self._bins[bin] += incr
			self._total += incr
		except IndexError:
			self._extend_and_set(bin, incr)

	def _set_bin_unbounded(self, bin: int, val: _T) -> None:
		try:
			old_val = self._bins[bin]
			self._bins[bin] = val
			self._total += val - old_val
		except IndexError:
			self._extend_and_set(bin, val)

	def _increment_bounded(self, bin: int, incr: _T) -> None:
		self._bins[bin] += incr
		self._total += incr

	def _set_bin_bounded(self, bin: int, val: _T) -> None:
		old_val = self._bins[bin]
		self._bins[bin] = val
		self._total += val - old_val

	def _extend_and_set(self, bin: int, val: _T) -> None:
		it = itertools.repeat(self._zero_value, bin - len(self._bins) + 1)
		self._bins.extend(it)
		self._bins[bin] = val
		self._total += val

	@property
	def total(self) -> _T:
		return self._total

	@property
	def binner(self) -> Binner:
		return self._binner

	@property
	def bounded(self) -> bool:
		return self._binner.bounded

	@property
	def bin_data(self) -> 'array[_T]':
		return self._bins

	def __iter__(self) -> Iterator[int]:
		return self._binner.bin_edges()

	def __len__(self) -> int:
		if self._binner.bounded:
			return self._binner.bins
		else:
			raise TypeError(
				f'The {self.__class__.__name__} instance is unbounded, len(.) is undefined',
			)

	def __getitem__(self, num: int) -> _T:
		return self._get_bin(self._binner(num))

	def __setitem__(self, num: int, val: _T) -> None:
		self._set_bin(self._binner(num), val)

	def __contains__(self, el: object) -> bool:
		return isinstance(0, int)

	def increment(self, num: int, incr: _T=1) -> None:
		self._increment(self._binner(num), incr)

	def decrement(self, num: int, decr: _T=1) -> None:
		self._increment(self._binner(num), -decr)

	def reset(self) -> None:
		self._init_bins_and_total()

	def items(self) -> '_BinnedArray._ItemsView[_T]':
		return _BinnedArray._ItemsView(self)

	def values(self) -> '_BinnedArray._ValuesView[_T]':
		return _BinnedArray._ValuesView(self)


class _ImmutableMixIn(Generic[_T]):
	_mutating_exception_msg = 'Mutating methods are not supported on {}'

	def __setitem__(self, num: int, val: _T) -> None:
		raise TypeError(self._mutating_exception_msg.format(self.__class__.__name__))

	def increment(self, num: int, incr: _T=1) -> None:
		raise TypeError(self._mutating_exception_msg.format(self.__class__.__name__))

	def decrement(self, num: int, decr: _T=1) -> None:
		raise TypeError(self._mutating_exception_msg.format(self.__class__.__name__))

	def reset(self) -> None:
		raise TypeError(self._mutating_exception_msg.format(self.__class__.__name__))


class _ModifyMixIn(Generic[_T]):
	_type_code: str
	binner: Binner
	_bins: 'array[_T]'
	_total: _T

	def update(self, counters: _BinnedArray[_T], ewma_factor: float) -> None:
		"""Update bin counter values by combining with counters using EWMA.

		ewma_factor is applied to the incoming value counters.bin_data[i] and
		1 - ewma_factor is applied to the current value self.bin_data[i].

		"""

		if not _binners_similar(counters.binner, self.binner):
			# TODO: can self still be updated meaningfully, for example when
			# the first bin_edges align? Or statistical interpolation of each
			# bin (what about first and last bin of infinite size)?
			raise ValueError('counters binning scheme is not matching this binning scheme')

		trsf_func: Callable[[float], _T]
		if self._type_code in ('f', 'd'):
			trsf_func = cast('Callable[[float], _T]', float)
		else:
			trsf_func = cast('Callable[[float], _T]', int)

		self._total = _ewma_update_array(self._bins, counters.bin_data, ewma_factor, trsf_func)

	def set_bin_data(self, data: 'array[_T]') -> None:
		self._bins = data
		self._total = sum(self._bins)


def _ewma_update_array(
	orig: 'array[_T]',
	inp: 'array[_T]',
	ewma_factor: float,
	transform_func: Callable[[float], _T],
) -> _T:
	decay_factor = 1.0 - ewma_factor

	zero = transform_func(0.0)
	total = zero

	if len(orig) < len(inp):
		orig.extend(
			itertools.repeat(zero, len(inp) - len(orig) + 1),
		)

	for i in range(len(inp)):
		# Update each orig element by combining with the value
		# from the corresponding element of inp using EWMA
		val = transform_func(
			ewma_factor * inp[i] + decay_factor * orig[i],
		)
		orig[i] = val
		total += val

	for i in range(len(inp), len(orig)):
		val = transform_func(decay_factor * orig[i])
		orig[i] = val
		total += val

	return total

def _binners_similar(a: Binner, b: Binner) -> bool:
	if a is b:
		return True
	elif a.bins == b.bins:
		return True
	else:
		# TODO: implement equality on Binner
		# TODO: In Binner.__eq__ (base case) compare the bin_edges of the binners
		return False


class BinnedCounters(_ModifyMixIn[int], _BinnedArray[int]):
	_type_code: str = 'Q'
	_zero_value: int = 0


class BinnedFloats(_ModifyMixIn[float], _BinnedArray[float]):
	_type_code = 'd'
	_zero_value: float = 0.0


class HalvingBinnedCounters(_BinnedArray[int]):
	_type_code: str = 'Q'
	_zero_value: int = 0

	def __init__(
		self,
		binner: Binner,
		factor: float,
		max_bin: Optional[int] = None,
		max_total: Optional[int] = None,
	) -> None:
		super(HalvingBinnedCounters, self).__init__(binner)

		if max_bin is None and max_total is None:
			raise ValueError('Either max_bin or max_total must be passed')

		bin_max_inf: Union[int, float] = max_bin if max_bin is not None else math.inf
		total_max_inf: Union[int, float] = max_total if max_total is not None else math.inf

		old_increment = self._increment
		old_set_bin = self._set_bin

		def increment(bin: int, incr: int) -> None:
			old_increment(bin, incr)
			if self._bins[bin] > bin_max_inf or self._total > total_max_inf:
				for i in range(len(self._bins)):
					self._bins[i] = int(self._bins[i] * factor)
				self._total = sum(self._bins)

		def set_bin(bin: int, val: int) -> None:
			old_set_bin(bin, val)
			if self._bins[bin] > bin_max_inf or self._total > total_max_inf:
				for i in range(len(self._bins)):
					self._bins[i] = int(self._bins[i] * factor)
				self._total = sum(self._bins)

		self._increment = increment
		self._set_bin = set_bin


class BinnedProbabilities(_ImmutableMixIn[float], _BinnedArray[float]):
	_type_code = 'd'
	_mutating_exception_msg = 'Mutating methods are not supported on BinnedProbabilities'
	_zero_value: float = 0.0

	@property
	def total(self) -> float:
		return 1.0

	@classmethod
	def from_counters(cls, counters: BinnedCounters) -> 'BinnedProbabilities':
		p = cls(counters.binner)
		total = counters.total
		p._bins = array(cls._type_code, (count / total for count in counters.bin_data))
		return p


class CountedProbabilities(BinnedProbabilities):
	def __init__(self, binner: Binner, ewma_factor: Optional[float]=None) -> None:
		super(CountedProbabilities, self).__init__(binner)
		self._ewma_factor: Optional[float] = ewma_factor
		self._counters_bins: array[int] = array('Q')

	def update(self, counters: BinnedCounters, ewma_factor: Optional[float]=None) -> None:
		"""Update probabilities by combining with counters using EWMA.

		First, the internally stored per-bin counters are updated by combining
		each existing value with the value in the corresponding bin of
		counters. Then, the probabilities are re-calculated from the updated
		internally stored counters.

		"""

		if ewma_factor is None:
			ewma_factor = self._ewma_factor
			if ewma_factor is None:
				raise ValueError('ewma_factor argument must be passed')

		if not _binners_similar(counters.binner, self.binner):
			# TODO: can self still be updated meaningfully, for example when
			# the first bin_edges align? Or statistical interpolation of each
			# bin (what about first and last bin of infinite size)?
			raise ValueError('counters binning scheme is not matching this binning scheme')

		total = _ewma_update_array(self._counters_bins, counters.bin_data, ewma_factor, int)

		self._bins = array(self._type_code, (count / total for count in self._counters_bins))

	@classmethod
	def from_counters(
		cls,
		counters: BinnedCounters,
		ewma_factor: Optional[float] = None,
	) -> 'CountedProbabilities':
		p = cls(counters.binner, ewma_factor=ewma_factor)
		total = counters.total
		bin_data = counters.bin_data
		p._counters_bins = array(bin_data.typecode, bin_data)
		p._bins = array(cls._type_code, (count / total for count in bin_data))
		return p
