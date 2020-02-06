import itertools
import timeit
import random
from .utils import StringSource
from typing import Callable, Deque, List, Set, Tuple

"""Benchmark for data structures + operations for the RAND cache eviction
algorithm.


The operations performed for each replacement are:

 1. Choose a random file from the list (algorithm state) and remove it (also
    evict from storage).
 2. Append the file to be placed to the list (algorithm state).

"""

base_length = 10000

def time_list_choice() -> Tuple[int, float]:
	col: List[str] = []

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		el = random.choice(col)
		col.remove(el)
		col.append(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_ind_del() -> Tuple[int, float]:
	col: List[str] = []

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		del col[ind]
		col.append(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_ind_del_check() -> Tuple[int, float]:
	col: List[str] = []

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		del col[ind]
		el = next(s)
		if el not in col:
			col.append(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_ind_pop() -> Tuple[int, float]:
	col: List[str] = []

	s = StringSource()

	col = []
	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		col[ind] = col[-1]
		col.pop()
		col.append(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_ind_pop_check() -> Tuple[int, float]:
	col: List[str] = []

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		col[ind] = col[-1]
		col.pop()
		el = next(s)
		if el not in col:
			col.append(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_set_del() -> Tuple[int, float]:
	col: List[str] = []
	col_set: Set[str] = set()

	s = StringSource()

	for el in s.take(base_length):
		col.append(el)
		col_set.add(el)

	def do() -> None:
		ind = random.randrange(len(col))

		el = col[ind]
		del col[ind]
		col_set.remove(el)

		el = next(s)
		if el not in col_set:
			col.append(el)
			col_set.add(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_list_set_pop() -> Tuple[int, float]:
	col: List[str] = []
	col_set: Set[str] = set()

	s = StringSource()

	for el in s.take(base_length):
		col.append(el)
		col_set.add(el)

	def do() -> None:
		ind = random.randrange(len(col))

		el = col[ind]
		col[ind] = col[-1]
		col.pop()
		col_set.remove(el)

		el = next(s)
		if el not in col_set:
			col.append(el)
			col_set.add(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque() -> Tuple[int, float]:
	col: Deque[str] = Deque()

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		el = random.choice(col)
		col.remove(el)
		col.append(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque_range() -> Tuple[int, float]:
	col: Deque[str] = Deque()

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		el = col[ind]
		del col[ind]
		col.append(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_deque_ind_check() -> Tuple[int, float]:
	col: Deque[str] = Deque()

	s = StringSource()

	col.extend(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		el = col[ind]
		del col[ind]
		el = next(s)
		if el not in col:
			col.append(el)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_set_iter() -> Tuple[int, float]:
	col: Set[str] = set()

	s = StringSource()

	col.update(s.take(base_length))

	def do() -> None:
		ind = random.randrange(len(col))
		el_to_remove = list(itertools.islice(col, ind, ind + 1))[0]

		col.remove(el_to_remove)

		col.add(next(s))

	timer = timeit.Timer(do)
	return timer.autorange()

BenchmarkFunction = Callable[[], Tuple[int, float]]

benchmark_functions: List[BenchmarkFunction] = [
	time_list_choice,
	time_list_ind_del,
	time_list_ind_del_check,
	time_list_ind_pop,
	time_list_ind_pop_check,
	time_list_set_del,
	time_list_set_pop,
	time_deque,
	time_deque_range,
	time_deque_ind_check,
	time_set_iter,
]

def main() -> None:
	for f in benchmark_functions:
		rep, dur = f()
		print(f.__name__, (dur / rep, rep, dur))

if __name__ == '__main__':
	main()
