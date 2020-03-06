from array import array
import timeit
from typing import Tuple

def time_access() -> Tuple[int, float]:
	a: array[int] = array('Q', [0] * 10)

	def do() -> None:
		val = a[5]

	timer = timeit.Timer(do)
	return timer.autorange()

def time_try_succeeds() -> Tuple[int, float]:
	a: array[int] = array('Q', [0] * 10)

	def do() -> None:
		val: int
		try:
			val = a[5]
		except IndexError:
			val = 0

	timer = timeit.Timer(do)
	return timer.autorange()

def time_try_raises() -> Tuple[int, float]:
	a: array[int] = array('Q', [0] * 10)

	def do() -> None:
		val: int
		try:
			val = a[20]
		except IndexError:
			val = 0

	timer = timeit.Timer(do)
	return timer.autorange()

benchmark_functions = [
	time_access,
	time_try_succeeds,
	time_try_raises,
]

def main() -> None:
	res = []

	for f in benchmark_functions:
		rep, dur = f()
		res.append((f.__name__, (dur / rep, rep, dur)))
		print(*res[-1])

if __name__ == '__main__':
	main()
