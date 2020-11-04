from typing import Iterable, Iterator

from .merger import TaskMerger
from .scheduler import AccessAssignment, NodeSpec, Scheduler
from ..workload import Job, Task, TimeStamp
from ..workload.stats import StatsCounters

__all__ = ['AccessAssignment', 'Distributor', 'NodeSpec']


class Distributor(object):
	def __init__(self, cache_proc: int, node_specs: Iterable[NodeSpec], tasks: Iterable[Task]):
		self._task_merger: TaskMerger = TaskMerger(tasks, self._now)
		self._stats_collector: StatsCollector = StatsCollector(self._task_merger)
		self._scheduler: Scheduler = Scheduler(cache_proc, node_specs, self._stats_collector)

	@property
	def stats(self) -> StatsCounters:
		return self._stats_collector.stats

	def __iter__(self) -> Iterator[AccessAssignment]:
		return iter(self._scheduler)

	def _now(self) -> TimeStamp:
		return self._scheduler.now()


class StatsCollector(object):
	def __init__(self, jobs_it: Iterable[Job]) -> None:
		self._jobs_it: Iterator[Job] = iter(jobs_it)
		self._counters: StatsCounters = StatsCounters()

	@property
	def stats(self) -> StatsCounters:
		return self._counters

	def __iter__(self) -> Iterator[Job]:
		for job in self._jobs_it:
			for request in job.access_requests:
				self._counters.process_access(request)
			yield job
