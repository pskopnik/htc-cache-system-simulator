import itertools
from typing import Any, Callable, Iterator, Optional

from . import Job, Submitter, TimeStamp
from ..utils import callback_iterable


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
