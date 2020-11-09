import timeit
from typing import Callable, Tuple


class A(object):
	def __init__(self) -> None:
		def unbound_method() -> int:
			return 3

		self.unbound_method: Callable[[], int] = unbound_method

		value = 4
		self._value = 4

		def unbound_self_accessing_method() -> int:
			return value

		self.unbound_self_accessing_method: Callable[[], int] = unbound_self_accessing_method

	def bound_method(self) -> int:
		return 3

	def bound_self_accessing_method(self) -> int:
		return self._value


def time_bound() -> Tuple[int, float]:
	a = A()

	def do() -> None:
		val = a.bound_method()

	timer = timeit.Timer(do)
	return timer.autorange()

def time_unbound() -> Tuple[int, float]:
	a = A()

	def do() -> None:
		val = a.unbound_method()

	timer = timeit.Timer(do)
	return timer.autorange()

def time_bound_self_accessing() -> Tuple[int, float]:
	a = A()

	def do() -> None:
		val = a.bound_self_accessing_method()

	timer = timeit.Timer(do)
	return timer.autorange()

def time_unbound_self_accessing() -> Tuple[int, float]:
	a = A()

	def do() -> None:
		val = a.unbound_self_accessing_method()

	timer = timeit.Timer(do)
	return timer.autorange()

benchmark_functions = [
	time_bound,
	time_unbound,
	time_bound_self_accessing,
	time_unbound_self_accessing,
]

def main() -> None:
	res = []

	for f in benchmark_functions:
		rep, dur = f()
		res.append((f.__name__, (dur / rep, rep, dur)))
		print(*res[-1])

if __name__ == '__main__':
	main()
