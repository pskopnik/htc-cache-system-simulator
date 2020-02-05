import itertools
import random
from typing import Callable, Collection, List, TypeVar

from simulator.dstructures.sorted import SortedDefaultDict

_KT = TypeVar('_KT')
_VT = TypeVar('_VT')

def _assert_order(
	d: SortedDefaultDict[_KT, _VT],
	keys: Collection[_KT],
	val_func: Callable[[_KT], _VT],
) -> None:
	assert list(d) == list(keys)
	assert list(d.keys()) == list(keys)
	assert list(d.values()) == list(map(val_func, keys))
	assert list(d.items()) == list(zip(keys, map(val_func, keys)))

def test_sorted_default_dict_set() -> None:
	d: SortedDefaultDict[int, int] = SortedDefaultDict(lambda: 0)

	l = list(range(10))
	random.shuffle(l)

	for el in l:
		d[el] = el

	_assert_order(d, range(10), lambda x: x)

def test_sorted_default_dict_default_construct() -> None:
	d: SortedDefaultDict[int, List[int]] = SortedDefaultDict(list)

	l = list(range(10))
	random.shuffle(l)

	for el in l:
		d[el].append(el)

	_assert_order(d, range(10), lambda x: [x])

def test_sorted_default_dict_del() -> None:
	d: SortedDefaultDict[int, int] = SortedDefaultDict(lambda: 0)

	l = list(range(10))
	random.shuffle(l)

	for el in l:
		d[el] += el

	for el in range(5):
		del d[el]

	_assert_order(d, range(5, 10), lambda x: x)
