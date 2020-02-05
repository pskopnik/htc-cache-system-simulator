from collections import OrderedDict
from typing import Generic, Iterator, Tuple, TypeVar

from ..workload import FileID

_KeyType = TypeVar('_KeyType')
_ElementType = TypeVar('_ElementType')

# Would be great to inherit from OrderedDict for implementation but not
# inheriting the interface (python/typing#241).
# Unfortunately it is not possible to re-assign the special (dunder) methods
# in __init__.
# Both would speed up implementation.
# https://github.com/python/typing/issues/241

class LRUDict(Generic[_KeyType, _ElementType]):
	def __init__(self) -> None:
		self._odict: OrderedDict[_KeyType, _ElementType] = OrderedDict()

	def __len__(self) -> int:
		return len(self._odict)

	def __contains__(self, key: _KeyType) -> bool:
		return key in self._odict

	def __getitem__(self, key: _KeyType) -> _ElementType:
		return self._odict[key]

	def __setitem__(self, key: _KeyType, el: _ElementType) -> None:
		self._odict[key] = el

	def __delitem__(self, key: _KeyType) -> None:
		del self._odict[key]

	def __iter__(self) -> Iterator[_KeyType]:
		"""Returns an iterator yielding the most recently accessed elements
		first.
		"""
		return iter(self._odict.keys())

	def access(self, key: _KeyType) -> None:
		"""Replicates an element access by moving the element to the front of
		the LRU structure.

		If ensure is False and the element is not in the structure, a KeyError
		will be raised.
		"""
		self._odict.move_to_end(key, last=False)

	def pop(self) -> Tuple[_KeyType, _ElementType]:
		"""Removes the least recently accessed element from the LRU structure.

		Raises a KeyError if there are no elements.
		"""
		return self._odict.popitem()


class FileInfo(object):
	__slots__ = ['size']

	def __init__(self, size: int) -> None:
		self.size = size


class FileLRUDict(LRUDict[FileID, FileInfo]):
	def __init__(self) -> None:
		super(FileLRUDict, self).__init__()
		self._total_size: int = 0

	@property
	def total_size(self) -> int:
		return self._total_size

	def __delitem__(self, file: FileID) -> None:
		info = self[file]
		super(FileLRUDict, self).__delitem__(file)
		self._total_size -= info.size

	def __setitem__(self, file: FileID, info: FileInfo) -> None:
		super(FileLRUDict, self).__setitem__(file, info)
		self._total_size += info.size

	def pop(self) -> Tuple[FileID, FileInfo]:
		popped_tuple = super(FileLRUDict, self).pop()
		self._total_size -= popped_tuple[1].size
		return popped_tuple

	def add_bytes(self, number_of_bytes: int) -> None:
		self._total_size += number_of_bytes

	def remove_bytes(self, number_of_bytes: int) -> None:
		self._total_size -= number_of_bytes

	def add_bytes_to_file(self, file: FileID, number_of_bytes: int) -> None:
		self[file].size += number_of_bytes
		self._total_size += number_of_bytes

	def remove_bytes_to_file(self, file: FileID, number_of_bytes: int) -> None:
		self[file].size -= number_of_bytes
		self._total_size -= number_of_bytes
