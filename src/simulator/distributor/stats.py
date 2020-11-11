from typing import Iterable, Iterator

from .scheduler import AccessAssignment
from ..workload.stats import StatsCounters

class AssignmentsStatsCollector(object):
	def __init__(self, assgnm_it: Iterable[AccessAssignment]) -> None:
		self._assgnm_it: Iterator[AccessAssignment] = iter(assgnm_it)
		self._counters: StatsCounters = StatsCounters()

	@property
	def stats(self) -> StatsCounters:
		return self._counters

	def __iter__(self) -> Iterator[AccessAssignment]:
		for assgnm in self._assgnm_it:
			self._counters.process_access(assgnm.access)
			yield assgnm
