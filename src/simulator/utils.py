import collections
import itertools
import operator
from typing import Any, Callable, cast, Iterable, Iterator, Optional, TypeVar

_T = TypeVar('_T')

def callback_iterable(cb: Callable[[], None]) -> Iterator[Any]:
	"""Returns an iterator it which calls cb on next(it).
	"""
	return itertools.islice(
		map(cast('Callable[[None], None]', lambda _: cb()), [None]),
		1,
		1,
	)

def repeat_each(it: Iterable[_T], n: int) -> Iterator[_T]:
	"""Returns an iterator which repeats each element of it n times.
	"""
	return itertools.chain.from_iterable(itertools.repeat(el, n) for el in it)

def consume(it: Iterable[Any], n: Optional[int]=None) -> None:
	"""Advance the iterable it n-steps ahead. If n is None, consume entirely.

	Copied from:
	https://docs.python.org/3.7/library/itertools.html#itertools-recipes
	"""
	if n is None:
		collections.deque(it, maxlen=0)
	else:
		next(itertools.islice(it, n, n), None)

def accumulate(
	iterable: Iterable[_T],
	func: Callable[[_T, _T], _T] = operator.add,
	initial: Optional[_T] = None,
) -> Iterator[_T]:
	"""Like itertools.accumulate but with prefixed initial, if not None.

	This replicates the behaviour of itertools.accumulate in Python 3.8.
	"""
	initial_it = [initial] if initial is not None else []

	return itertools.accumulate(
		itertools.chain(initial_it, iterable),
		func,
	)
