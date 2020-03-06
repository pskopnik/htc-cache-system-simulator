import itertools
import timeit
from collections import OrderedDict
import random
from .utils import StringSource

from typing import Callable, cast, Deque, Iterable, List, Optional, Tuple, TypeVar

"""Benchmark for data structures + operations for the LRU cache eviction
algorithm.
"""

base_length = 10000
active_file_factor = 10

_T = TypeVar('_T')

def init_state(col: _T, extend: Optional[Callable[[_T], Callable[[Iterable[str]], None]]]=None) -> Tuple[_T, StringSource]:
	s = StringSource()

	if extend is None:
		extend = cast(Callable[[_T], Callable[[Iterable[str]], None]], lambda c: c.extend)

	extend(col)(s.take(base_length))

	return col, s

def ordered_dict_extend(d: OrderedDict[str, None]) -> Callable[[Iterable[str]], None]:
	def f(it: Iterable[str]) -> None:
		for e in it:
			d[e] = None

	return f

def time_list_hit() -> Tuple[int, float]:
	col, s = init_state(List[str]())

	def do() -> None:
		el = random.choice(col)
		ind = col.index(el)
		col.insert(0, el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_replacement() -> Tuple[int, float]:
	col, s = init_state(List[str]())

	def do() -> None:
		col.pop()
		col.insert(0, next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_unif() -> Tuple[int, float]:
	col, s = init_state(List[str]())

	s.take(base_length*active_file_factor)

	def do() -> None:
		el = s.rand_existing()

		try:
			ind = col.index(el)
		except ValueError:
			col.pop()
			col.insert(0, el)
		else:
			del col[ind]
			col.insert(0, el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque_hit() -> Tuple[int, float]:
	col, s = init_state(Deque[str]())

	def do() -> None:
		el = random.choice(col)
		ind = col.index(el)
		col.appendleft(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque_replacement() -> Tuple[int, float]:
	col, s = init_state(Deque[str]())

	def do() -> None:
		col.pop()
		col.appendleft(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque_unif() -> Tuple[int, float]:
	col, s = init_state(Deque[str]())

	s.take(base_length*active_file_factor)

	def do() -> None:
		el = s.rand_existing()

		try:
			ind = col.index(el)
		except ValueError:
			col.pop()
			col.appendleft(el)
		else:
			del col[ind]
			col.appendleft(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_ordered_dict_hit() -> Tuple[int, float]:
	col, s = init_state(OrderedDict[str, None](), extend=ordered_dict_extend)

	def do() -> None:
		el = s.rand_existing()
		col.move_to_end(el, last=False)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_ordered_dict_replacement() -> Tuple[int, float]:
	col, s = init_state(OrderedDict[str, None](), extend=ordered_dict_extend)

	def do() -> None:
		col.popitem()
		el = next(s)
		col[el] = None
		col.move_to_end(el, last=False)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_ordered_dict_replacement_reverse() -> Tuple[int, float]:
	col, s = init_state(OrderedDict[str, None](), extend=ordered_dict_extend)

	def do() -> None:
		col.popitem(last=False)
		el = next(s)
		col[el] = None

	timer = timeit.Timer(do)
	return timer.autorange()

def time_ordered_dict_unif() -> Tuple[int, float]:
	col, s = init_state(OrderedDict[str, None](), extend=ordered_dict_extend)

	s.take(base_length*active_file_factor)

	def do() -> None:
		el = s.rand_existing()

		if el in col:
			col.move_to_end(el, last=False)
		else:
			# replacement (forwards)
			col.popitem()
			el = next(s)
			col[el] = None
			col.move_to_end(el, last=False)

	timer = timeit.Timer(do)
	return timer.autorange()

benchmark_functions = [
	time_list_hit,
	time_list_replacement,
	time_list_unif,
	time_deque_hit,
	time_deque_replacement,
	time_deque_unif,
	time_ordered_dict_hit,
	time_ordered_dict_replacement,
	time_ordered_dict_replacement_reverse,
	time_ordered_dict_unif,
]

def main() -> None:
	res = []

	for f in benchmark_functions:
		rep, dur = f()
		res.append((f.__name__, (dur / rep, rep, dur)))
		# print(*res[-1])

	print("\n")

	for k in sorted(res, key=lambda el: el[1][0]):
		print(*k)

if __name__ == '__main__':
	main()
