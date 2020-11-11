import heapq
from typing import Any, Callable, Iterable, Iterator, List, Sequence, Tuple

from ..workload import Job, Submitter, Task

TimeStamp = int


class TaskMerger(object):
	"""TaskMerger creates a single job stream from a set of tasks.

	It has its own sense of "now", that is the maximum of the upstream time
	(now_fn) and the time stamp of the latest event.
	"""

	def __init__(self, tasks: Iterable[Task], now_fn: Callable[[], TimeStamp]):
		self._now_fn: Callable[[], TimeStamp] = now_fn
		self._max_event_ts: TimeStamp = 0
		self._queue_index: int = 0
		self._heap: List[Tuple[TimeStamp, int, str, Any, Iterator[Any]]] = []

		for task in tasks:
			it = iter(task)
			self._push_next_submitter(it)

	def __iter__(self) -> Iterator[Job]:
		while True:
			try:
				ts, _, kind, el, successor_it = heapq.heappop(self._heap)
			except IndexError:
				return

			self._max_event_ts = ts

			if kind == 'submitter':
				self._push_next_submitter(successor_it)
				self._push_next_job(iter(el))
				continue
			elif kind == 'job':
				self._push_next_job(successor_it)
				yield el
			else:
				raise Exception('Unknown kind {}'.format(kind))

	def _push_next_submitter(self, submitter_it: Iterator[Submitter]) -> None:
		try:
			submitter = next(submitter_it)
			heapq.heappush(self._heap, (
				submitter.start_ts, self._queue_index, 'submitter', submitter, submitter_it,
			))
			self._queue_index += 1
		except StopIteration:
			pass

	def _push_next_job(self, job_it: Iterator[Job]) -> None:
		try:
			next_job = next(job_it)
			if next_job.submit_ts is None:
				next_job.submit_ts = self.now()

			heapq.heappush(self._heap, (
				next_job.submit_ts, self._queue_index, 'job', next_job, job_it,
			))
			self._queue_index += 1
		except StopIteration:
			pass

	def now(self) -> TimeStamp:
		return max(self._max_event_ts, self._now_fn())
