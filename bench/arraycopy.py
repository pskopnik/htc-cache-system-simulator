from array import array
from copy import copy
import itertools
import timeit
from typing import Tuple

base_length = 100000

def time_reinit() -> Tuple[int, float]:
	src: array[int] = array('Q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', src)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_reinit_iter() -> Tuple[int, float]:
	src: array[int] = array('Q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', iter(src))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_type_change_reinit() -> Tuple[int, float]:
	src: array[int] = array('q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', src)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_type_change_reinit_iter() -> Tuple[int, float]:
	src: array[int] = array('q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', iter(src))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_copy() -> Tuple[int, float]:
	src: array[int] = array('Q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = copy(src)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_reinit_array_reverse() -> Tuple[int, float]:
	src: array[int] = array('Q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', src)
		dest.reverse()

	timer = timeit.Timer(do)
	return timer.autorange()

def time_init_reversed() -> Tuple[int, float]:
	src: array[int] = array('Q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', reversed(src))

	timer = timeit.Timer(do)
	return timer.autorange()

def time_type_change_array_reverse() -> Tuple[int, float]:
	src: array[int] = array('q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', src)
		dest.reverse()

	timer = timeit.Timer(do)
	return timer.autorange()

def time_type_change_init_reversed() -> Tuple[int, float]:
	src: array[int] = array('q', itertools.repeat(0, base_length))

	def do() -> None:
		dest: array[int] = array('Q', reversed(src))

	timer = timeit.Timer(do)
	return timer.autorange()

benchmark_functions = [
	time_reinit,
	time_reinit_iter,
	time_type_change_reinit,
	time_type_change_reinit_iter,
	time_copy,
	time_reinit_array_reverse,
	time_init_reversed,
	time_type_change_array_reverse,
	time_type_change_init_reversed,
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
