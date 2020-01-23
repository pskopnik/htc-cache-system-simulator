from typing import Iterable, Iterator
from .scheduler import AccessAssignment, NodeSpec, Scheduler
from ..workload import Job, Task, TimeStamp
from .stats import StatsCollector
from .merger import TaskMerger

__all__ = ['AccessAssignment']


class Distributor(object):
	def __init__(self, cache_proc: int, node_specs: Iterable[NodeSpec], tasks: Iterable[Task]):
		self._task_merger: TaskMerger = TaskMerger(tasks, self._now)
		self._stats: StatsCollector = StatsCollector(self._task_merger)
		self._scheduler: Scheduler = Scheduler(cache_proc, node_specs, self.stats)

	@property
	def stats(self) -> StatsCollector:
		return self._stats

	def __iter__(self) -> Iterator[AccessAssignment]:
		return iter(self._scheduler)

	def _now(self) -> TimeStamp:
		return self._scheduler.now()
