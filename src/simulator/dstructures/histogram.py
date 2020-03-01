from array import array
import itertools
import math
from typing import (
	Callable,
	cast,
	Generic,
	Iterable,
	Iterator,
	Mapping,
	Optional,
	Set,
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

	@property
	def _zero_value(self) -> _T:
		return 0

	class _ValuesView(Generic[_T_inner], ValuesView[_T_inner]):
		def __init__(self, b_array: '_BinnedArray[_T_inner]') -> None:
			# The ValuesView constructor is not documented and hence not known
			# to mypy (typeshed)
			super(_BinnedArray._ValuesView, self).__init__(b_array) # type: ignore[call-arg]
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

	class _ItemsView(Generic[_T_inner], Set[Tuple[int, _T_inner]]):
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
			raise TypeError(f'The {self.__class__.__name__} is unbounded, len(.) is undefined')

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


class BinnedCounters(_BinnedArray[int]):
	_type_code: str = 'Q'
	_zero_value: int = 0


class HalvingBinnedCounters(BinnedCounters):
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


class BinnedProbabilities(_BinnedArray[float]):
	_type_code = 'd'
	_mutating_exception_msg = 'Mutating methods are not supported on BinnedProbabilities'
	_zero_value: float = 0.0

	@property
	def total(self) -> float:
		return 1.0

	def __setitem__(self, num: int, val: float) -> None:
		raise TypeError(self._mutating_exception_msg)

	def increment(self, num: int, incr: float=1) -> None:
		raise TypeError(self._mutating_exception_msg)

	def decrement(self, num: int, decr: float=1) -> None:
		raise TypeError(self._mutating_exception_msg)

	def reset(self) -> None:
		raise TypeError(self._mutating_exception_msg)

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

		# TODO: also compare the bin_edges of the self.binner and
		# counters.binner
		if counters.binner.bins != self.binner.bins:
			# TODO: can self still be updated meaningfully, for example when
			# the first bin_edges align? Or statistical interpolation of each
			# bin (what about first and last bin of infinite size)?
			raise ValueError('counters binning scheme is not matching this binning scheme')

		if self.binner.bins == -1 and len(self._counters_bins) < len(counters.bin_data):
			self._counters_bins.extend(
				itertools.repeat(0, len(counters.bin_data) - len(self._counters_bins) + 1),
			)

		total = 0
		for i in range(len(self._counters_bins)):
			# Update each self._counters_bins bin by combining with the value
			# from the corresponding bin of counters using EWMA
			val = int(
				ewma_factor * counters.bin_data[i] + (1 - ewma_factor) * self._counters_bins[i],
			)
			self._counters_bins[i] = val
			total += val

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
