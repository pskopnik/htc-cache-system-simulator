import abc
from typing import Any, Callable, cast, Iterable, Iterator, Optional, Sequence, Sized, TypeVar, Reversible
from typing_extensions import Protocol, runtime_checkable

from ..recorder import filter_cache_processor
from ..distributor import AccessAssignment
from ..workload import Access


class AccessSequence(abc.ABC, Sequence[Access]):
	"""Contemplated type spec for efficient containers for access sequences.

	The level of detail provided by this API (through Cursor) is not required
	by any implementation this far.

	A simpler variant is to implement all methods except Cursor. Such an
	implementation can be backed by memory or disk. Disk-backed
	implementations might be improved through a index to byte offset index.
	Such an implementation fulfills Sequence[Access] exactly.

	Even simpler, __getitem__ could be dropped. Such an interface conforms
	exactly to SimpleReader.
	"""
	class Cursor(abc.ABC):
		@property
		@abc.abstractmethod
		def direction(self) -> int:
			raise NotImplementedError

		@abc.abstractmethod
		def get(self) -> Access:
			raise NotImplementedError

		@abc.abstractmethod
		def advance(self, n: int=0) -> None:
			raise NotImplementedError

		@abc.abstractmethod
		def seek(self) -> None:
			raise NotImplementedError

		@abc.abstractmethod
		def next(self) -> Access:
			raise NotImplementedError

		def __next__(self) -> Access:
			return self.next()

		def __iter__(self) -> 'AccessSequence.Cursor':
			return self

	@abc.abstractmethod
	def __getitem__(self, key: Any) -> Any:
		# TODO proper signature
		# Sequence[_T_co] uses:
		# def __getitem__(self, s: slice) -> Sequence[_T_co]: ...
		raise NotImplementedError

	@abc.abstractmethod
	def __iter__(self) -> Iterator[Access]:
		raise NotImplementedError

	@abc.abstractmethod
	def __reversed__(self) -> Iterator[Access]:
		raise NotImplementedError

	@abc.abstractmethod
	def __len__(self) -> int:
		raise NotImplementedError

	@abc.abstractmethod
	def cursor(self) -> 'AccessSequence.Cursor':
		raise NotImplementedError


_T_co = TypeVar('_T_co', covariant=True)

class SimpleReader(Reversible[_T_co], Iterable[_T_co], Sized, Protocol[_T_co]):
	pass


SimpleAccessReader = SimpleReader[Access]
SimpleAssignmentReader = SimpleReader[AccessAssignment]

@runtime_checkable
class _Scopable(Protocol):
	def scope_to_cache_processor(self, cache_proc: int) -> SimpleAccessReader:
		pass


class GenericScoper(object):
	def __init__(self, cache_proc: int, access_reader: SimpleAssignmentReader) -> None:
		self._cache_proc: int = cache_proc
		self._reader: SimpleAssignmentReader = access_reader
		self._len: Optional[int] = None

	def __iter__(self) -> Iterator[Access]:
		return filter_cache_processor(self._cache_proc, iter(self._reader))

	def __reversed__(self) -> Iterator[Access]:
		return filter_cache_processor(self._cache_proc, reversed(self._reader))

	def __len__(self) -> int:
		if self._len is None:
			l = sum(map(lambda _: 1, iter(self)))
			self._len = l
			return l
		else:
			return self._len


def scope_to_cache_processor(cache_proc: int, access_reader: SimpleAssignmentReader) -> SimpleAccessReader:
	if isinstance(access_reader, _Scopable):
		return access_reader.scope_to_cache_processor(cache_proc)
	else:
		return GenericScoper(cache_proc, access_reader)

	# Instead of relying on the isinstance function and _Scopable's runtime
	# checker, the same logic could be implemented using duck typing:

	# try:
	# 	bound_method = cast(Callable[[int], SimpleAccessReader], cast(Any, access_reader).scope_to_cache_processor)
	# 	return bound_method(cache_proc)
	# except:
	# 	return GenericScoper(cache_proc, access_reader)
