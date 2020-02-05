from dataclasses import dataclass, field
import itertools
import timeit
from typing import Callable, cast, Iterable, List, Optional, Tuple, TypeVar
from .utils import StringSource
from apq import KeyedPQ

"""Benchmark for data structures + operations for the LRU cache eviction
algorithm.
"""

base_length = 10000
active_file_factor = 10
file_size = 1000

_T = TypeVar('_T')

def init_state(col: _T, extend: Optional[Callable[[_T], Callable[[Iterable[str]], None]]]=None) -> Tuple[_T, StringSource]:
	s = StringSource()

	if extend is None:
		extend = cast(Callable[[_T], Callable[[Iterable[str]], None]], lambda c: c.extend)

	extend(col)(s.take(base_length))

	return col, s


class LandlordState(object):
	@dataclass
	class _FileInfo(object):
		size: float = field(init=True)

	def __init__(self) -> None:
		self._pq: KeyedPQ[LandlordState._FileInfo] = KeyedPQ()
		self._rent_threshold: float = 0.0

	def __contains__(self, el: str) -> bool:
		return el in self._pq

	def pop_eviction_candidate(self) -> str:
		file, running_volume_credit, info = self._pq.pop()
		self._rent_threshold = running_volume_credit
		return file

	def remove_file(self, file: str) -> None:
		del self._pq[file]

	def add_file(self, file: str, size: int) -> None:
		cost = size
		credit = cost / size + self._rent_threshold
		self._pq.add(file, credit, LandlordState._FileInfo(size))

	def process_hit(self, file: str, size: int) -> None:
		cost = size
		credit = cost / size + self._rent_threshold
		self._pq.change_value(file, credit)


def landlord_extend(ld: LandlordState) -> Callable[[Iterable[str]], None]:
	def f(it: Iterable[str]) -> None:
		for e in it:
			ld.add_file(e, file_size)

	return f

def time_landlord_hit() -> Tuple[int, float]:
	col, s = init_state(LandlordState(), extend=landlord_extend)

	def do() -> None:
		el = s.rand_existing()
		col.process_hit(el, file_size)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_landlord_replacement() -> Tuple[int, float]:
	col, s = init_state(LandlordState(), extend=landlord_extend)

	def do() -> None:
		col.pop_eviction_candidate()
		col.add_file(next(s), file_size)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_landlord_unif() -> Tuple[int, float]:
	col, s = init_state(LandlordState(), extend=landlord_extend)

	s.take(base_length*active_file_factor)

	def do() -> None:
		el = s.rand_existing()

		if el in col:
			col.process_hit(el, file_size)
		else:
			col.pop_eviction_candidate()
			col.add_file(el, file_size)

	timer = timeit.Timer(do)
	return timer.autorange()

BenchmarkFunction = Callable[[], Tuple[int, float]]

benchmark_functions: List[BenchmarkFunction] = [
	time_landlord_hit,
	time_landlord_replacement,
	time_landlord_unif,
]

def main() -> None:
	res = []

	for f in benchmark_functions:
		rep, dur = f()
		res.append((f.__name__, (dur / rep, rep, dur)))
		print(*res[-1])

if __name__ == '__main__':
	main()
