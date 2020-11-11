from typing import Iterable, Iterator

from .merger import TaskMerger
from .scheduler import AccessAssignment, NodeSpec, Scheduler
from ..workload import Job, Task, TimeStamp
from ..workload.stats import StatsCounters

__all__ = ['AccessAssignment', 'Distributor', 'NodeSpec']


class Distributor(object):
	def __init__(self, node_specs: Iterable[NodeSpec], tasks: Iterable[Task]):
		self._task_merger: TaskMerger = TaskMerger(tasks, self._now)
		self._scheduler: Scheduler = Scheduler(node_specs, self._task_merger)

	def __iter__(self) -> Iterator[AccessAssignment]:
		return iter(self._scheduler)

	def _now(self) -> TimeStamp:
		return self._scheduler.now()
