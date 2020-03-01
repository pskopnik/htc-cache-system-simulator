import abc
import itertools
from typing import Callable, cast, Generic, Iterable, Iterator, List, Mapping, Set, Tuple, TypeVar

from .sorted import SortedDefaultDict


# TODO: Binner could also handle floats (numbers, limits)

class Binner(abc.ABC):
	@property
	@abc.abstractmethod
	def bounded(self) -> bool:
		raise NotImplementedError

	@property
	@abc.abstractmethod
	def bins(self) -> int:
		raise NotImplementedError

	@abc.abstractmethod
	def bin_edges(self) -> Iterator[int]:
		raise NotImplementedError

	@abc.abstractmethod
	def bin_limits(self, bin: int) -> Tuple[int, int]:
		raise NotImplementedError

	@abc.abstractmethod
	def __call__(self, num: int) -> int:
		raise NotImplementedError


class NoneBinner(Binner):
	@property
	def bounded(self) -> bool:
		return False

	@property
	def bins(self) -> int:
		return -1

	def bin_edges(self) -> Iterator[int]:
		return iter(itertools.count())

	def bin_limits(self, bin: int) -> Tuple[int, int]:
		return bin, bin+1

	def __call__(self, num: int) -> int:
		return num


class LogBinner(Binner):
	def __init__(self, first: int=0, last: int=-1, step: int=1) -> None:
		self._first: int = first
		self._last: int = last
		self._step: int = step

		self._bins = -1 if last == -1 else (last - first) // step + 1

		self._bin: Callable[[int], int]
		if last == -1:
			self._bin = lambda num: (max(num.bit_length() - 1, first) - first) // step
		else:
			self._bin = lambda num: (min(max(num.bit_length() - 1, first), last) - first) // step

	@property
	def bounded(self) -> bool:
		return self._last != -1

	@property
	def bins(self) -> int:
		return self._bins

	def bin_edges(self) -> Iterator[int]:
		it: Iterable[int]
		if self._bins == -1:
			it = itertools.count(start=self._first+self._step, step=self._step)
		else:
			it = range(self._first+self._step, self._last+1, self._step)

		return itertools.chain((0,), (2 ** i for i in it))

	def bin_limits(self, bin: int) -> Tuple[int, int]:
		real_first = (2 ** (self._first + bin * self._step))
		first: int
		last: int

		if bin == 0:
			first = 0
		else:
			first = real_first

		if self._last != -1 and bin == self._bins - 1:
			past = -1
		else:
			past = real_first * 2 ** self._step

		return first, past

	def __call__(self, num: int) -> int:
		return self._bin(num)


_T_co = TypeVar('_T_co', covariant=True)
_T_co_inner = TypeVar('_T_co_inner', covariant=True)

class BinnedMapping(Generic[_T_co], Mapping[int, _T_co]):
	class _ItemSet(Generic[_T_co_inner], Set[Tuple[int, _T_co_inner]]):
		def __init__(self, mapping: 'BinnedMapping[_T_co_inner]') -> None:
			self._mapping: BinnedMapping[_T_co_inner] = mapping

		def __len__(self) -> int:
			return len(self._mapping)

		def __contains__(self, el: object) -> bool:
			try:
				el = cast('Tuple[int, _T_co_inner]', el)
				if len(el) != 2:
					return False
				return self._mapping[el[0]] == el[1]
			except (KeyError, TypeError):
				return False

		def __iter__(self) -> Iterator[Tuple[int, _T_co_inner]]:
			container = self._mapping._container
			cnst_get_el = self._mapping._construct_or_get_element
			if self._mapping._binner.bins == -1:
				return zip(
					self._mapping._binner.bin_edges(),
					itertools.chain(
						container,
						(cnst_get_el(i) for i in itertools.count(len(container))),
					),
				)
			else:
				return zip(self._mapping._binner.bin_edges(), container)

	def __init__(
		self,
		binner: Binner,
		default_factory: Callable[[], _T_co],
	) -> None:
		self._default_factory: Callable[[], _T_co] = default_factory

		self._binner: Binner = binner
		self._container: List[_T_co]
		self._get_element: Callable[[int], _T_co]

		if self._binner.bins != -1:
			container = list(default_factory() for _ in range(self._binner.bins))
			self._container = container
			self._get_element = lambda bin: container[bin]
		else:
			self._container = []
			self._get_element = self._construct_or_get_element

	def _construct_or_get_element(self, bin: int) -> _T_co:
		try:
			return self._container[bin]
		except IndexError:
			default_factory = self._default_factory
			it = (default_factory() for _ in range(bin - len(self._container) + 1))
			self._container.extend(it)
			return self._container[bin]

	@property
	def binner(self) -> Binner:
		return self._binner

	@property
	def bounded(self) -> bool:
		return self._binner.bounded

	@property
	def default_factory(self) -> Callable[[], _T_co]:
		return self._default_factory

	def __getitem__(self, num: int) -> _T_co:
		return self._get_element(self._binner(num))

	def __iter__(self) -> Iterator[int]:
		return self._binner.bin_edges()

	def __len__(self) -> int:
		if self._binner.bounded:
			return self._binner.bins
		else:
			raise TypeError('The BinnedMapping is unbounded, len(.) is undefined')

	def items(self) -> 'BinnedMapping._ItemSet[_T_co]':
		return BinnedMapping._ItemSet(self)

	def values_until(self, num: int, half_open: bool=True) -> Iterator[_T_co]:
		last_bin = self._binner(num)
		if not half_open:
			last_bin += 1

		if self._binner.bins == -1:
			_ = self._construct_or_get_element(last_bin)

		return itertools.islice(self._container, last_bin)

	def values_from(self, num: int, half_open: bool=False) -> Iterator[_T_co]:
		first_bin = self._binner(num)
		if half_open:
			first_bin += 1

		if self._binner.bins == -1:
			for i in range(first_bin, len(self._container)):
				yield self._container[i]
			for i in itertools.count(len(self._container)):
				yield self._construct_or_get_element(i)
		else:
			for i in range(first_bin, self._binner.bins):
				yield self._container[i]

	def bin_limits(self, bin: int) -> Tuple[int, int]:
		return self._binner.bin_limits(bin)

	def bin_limits_from_num(self, num: int) -> Tuple[int, int]:
		return self._binner.bin_limits(self._binner(num))


class BinnedSparseMapping(Generic[_T_co], Mapping[int, _T_co]):
	def __init__(
		self,
		binner: Binner,
		default_factory: Callable[[], _T_co],
	) -> None:
		self._binner: Binner = binner
		self._dict: SortedDefaultDict[int, _T_co] = SortedDefaultDict(default_factory)

	@property
	def binner(self) -> Binner:
		return self._binner

	@property
	def bounded(self) -> bool:
		return self._binner.bounded

	@property
	def default_factory(self) -> Callable[[], _T_co]:
		return cast(Callable[[], _T_co], self._dict.default_factory)

	def __getitem__(self, num: int) -> _T_co:
		return self._dict[self._binner(num)]

	def __iter__(self) -> Iterator[int]:
		bin_limits = self._binner.bin_limits
		return (bin_limits(bin)[0] for bin in self._dict.keys())

	def __len__(self) -> int:
		return len(self._dict)

	def __delitem__(self, num: int) -> None:
		del self._dict[self._binner(num)]

	def items_until(self, num: int, half_open: bool=True) -> Iterator[Tuple[int, _T_co]]:
		dct = self._dict
		for key in dct.irange(maximum=self._binner(num), inclusive=(True, not half_open)):
			yield key, dct[key]

	def items_from(self, num: int, half_open: bool=False) -> Iterator[Tuple[int, _T_co]]:
		dct = self._dict
		for key in dct.irange(minimum=self._binner(num), inclusive=(not half_open, True)):
			yield key, dct[key]

	def bin_limits_from_num(self, num: int) -> Tuple[int, int]:
		return self._binner.bin_limits(self._binner(num))
