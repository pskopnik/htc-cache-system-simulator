import heapq
from typing import Any, Callable, cast, Generic, Iterator, Iterable, List, NamedTuple, Optional, Tuple, TypeVar

TimeStamp = int
_EventType = TypeVar('_EventType')


class EventIterator(Generic[_EventType]):
	def __init__(self, iterable: Iterable[_EventType], key: Callable[[_EventType], TimeStamp]):
		self._key: Callable[[_EventType], TimeStamp] = key
		self._next: Optional[Tuple[TimeStamp, _EventType]] = None
		self._iterable: Iterator[_EventType] = iter(iterable)

	def __iter__(self) -> Iterator[_EventType]:
		return self

	def next(self) -> _EventType:
		if self._next is not None:
			el = self._next[1]
			self._next = None
			return el
		else:
			return next(self._iterable)

	def __next__(self) -> _EventType:
		return self.next()

	def next_if_before(self, ts: TimeStamp) -> Optional[_EventType]:
		self._ensure_next()
		nxt = cast(Tuple[TimeStamp, _EventType], self._next)
		if nxt[0] < ts:
			el = nxt[1]
			self._next = None
			return el
		else:
			return None

	def is_next_before(self, ts: TimeStamp) -> bool:
		self._ensure_next()
		nxt = cast(Tuple[TimeStamp, _EventType], self._next)
		return nxt[0] < ts

	def _ensure_next(self) -> None:
		if self._next is None:
			el = next(self._iterable)
			self._next = (self._key(el), el)


class EventMerger(Generic[_EventType]):
	def __init__(self, streams: Iterable[Iterable[_EventType]], key: Callable[[_EventType], TimeStamp]):
		self._key: Callable[[_EventType], TimeStamp] = key
		self._event_index: int = 0
		self._heap: List[Tuple[TimeStamp, int, _EventType, Iterator[_EventType]]] = []

		for stream in streams:
			it = iter(stream)
			self._push_next(it)

	def __iter__(self) -> Iterator[_EventType]:
		return self

	def __next__(self) -> _EventType:
		try:
			_, _, ev, it = self._heap[0]
		except IndexError:
			raise StopIteration()

		try:
			next_ev = next(it)
		except StopIteration:
			heapq.heappop(self._heap)
		else:
			heapq.heapreplace(self._heap, (
				self._key(next_ev), self._event_index, next_ev, it,
			))
			self._event_index += 1

		return ev

	def _push_next(self, it: Iterator[_EventType]) -> None:
		try:
			ev = next(it)
		except StopIteration:
			return
		heapq.heappush(self._heap, (
			self._key(ev), self._event_index, ev, it,
		))
		self._event_index += 1
