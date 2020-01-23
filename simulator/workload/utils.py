import collections
import itertools
from typing import Any, Callable, cast, Iterable, Iterator, Optional, TypeVar

from . import Job, Submitter, TimeStamp


T = TypeVar('T')

def ignore_args(f: Callable[[], T]) -> Callable[..., T]:
	def _inner(*args: Any, **kwargs: Any) -> T:
		return f()

	return _inner

# TODO
# what is faster / better?
# ignore_args(f)
# or
# lambda _: f()

def callback_iterable(cb: Callable[[], None]) -> Iterator[Any]:
	return filter(lambda _: False, map(ignore_args(cb), [None]))

def repeat_each(iterable: Iterable[T], n: int) -> Iterator[T]:
	return itertools.chain.from_iterable(itertools.repeat(el, n) for el in iterable)

def consume(it: Iterable[Any]) -> None:
	collections.deque(it, maxlen=0)

def accumulate_initial(iterable: Iterable[T], func: Callable[[T, T], T], initial: Optional[T]=None) -> Iterator[T]:
	initial_it = [initial] if initial is not None else []

	return itertools.chain(
		initial_it,
		itertools.accumulate(iterable, func),
	)


class NoneSubmitter(Submitter):
	def __init__(self, start_ts: TimeStamp, origin: Optional[Any]=None) -> None:
		super(NoneSubmitter, self).__init__(start_ts, origin=origin)

	def __iter__(self) -> Iterator[Job]:
		return iter([])


class CallbackWrapSubmitter(Submitter):
	def __init__(
		self,
		submitter: Submitter,
		pre_cb: Callable[[], None]=lambda: None,
		post_cb: Callable[[], None]=lambda: None,
	) -> None:
		super(CallbackWrapSubmitter, self).__init__(submitter.start_ts, origin=submitter.origin)
		self._submitter: Submitter = submitter
		self._pre_cb: Callable[[], None] = pre_cb
		self._post_cb: Callable[[], None] = post_cb

	def __iter__(self) -> Iterator[Job]:
		return itertools.chain(
			callback_iterable(self._pre_cb),
			iter(self._submitter),
			callback_iterable(self._post_cb),
		)

	def __repr__(self) -> str:
		return f'<CallbackWrapSubmitter wrapping {self._submitter!r}>'
