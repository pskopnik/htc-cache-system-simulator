import itertools
import random
import timeit
from typing import Any, Callable, Deque, List, Set, Tuple, TypeVar

_T = TypeVar('_T')

def ignore_args(f: Callable[[], _T]) -> Callable[..., _T]:
	def _inner(*args: Any, **kwargs: Any) -> _T:
		return f()

	return _inner

def time_ignore_args() -> Tuple[int, float]:
	l: List[int] = list(range(10000))

	def cb() -> None:
		pass

	def do() -> None:
		map(ignore_args(cb), l)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_lambda() -> Tuple[int, float]:
	l: List[int] = list(range(10000))

	def cb() -> None:
		pass

	def do() -> None:
		map(lambda _: cb(), l)

	timer = timeit.Timer(do)
	return timer.autorange()

BenchmarkFunction = Callable[[], Tuple[int, float]]

benchmark_functions: List[BenchmarkFunction] = [
	time_ignore_args,
	time_lambda,
]

def main() -> None:
	for f in benchmark_functions:
		rep, dur = f()
		print(f.__name__, (dur / rep, rep, dur))

if __name__ == '__main__':
	main()
