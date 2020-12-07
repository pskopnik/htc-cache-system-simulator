import abc
from typing import cast, Iterable, Generator, Iterator, List, Optional
from .processor import Processor, OfflineProcessor, ProcessorGenerator, AccessInfo
from .stats import StatsCollector, StatsCounters
from ..distributor import AccessAssignment
from ..events import EventMerger
from .accesses import scope_to_cache_processor, SimpleAccessReader, SimpleReader


class CacheSystem(abc.ABC):
	@abc.abstractmethod
	def __iter__(self) -> Iterator[AccessInfo]:
		raise NotImplementedError

	@property
	@abc.abstractmethod
	def stats(self) -> StatsCounters:
		raise NotImplementedError

	@abc.abstractmethod
	def reset_after_warm_up(self) -> None:
		raise NotImplementedError


class OnlineCacheSystem(CacheSystem):
	def __init__(self, processors: Iterable[Processor], access_it: Iterable[AccessAssignment]):
		self._processors: List[ProcessorGenerator] = [proc.generator() for proc in processors]
		self._access_it: Iterable[AccessAssignment] = access_it
		self._splitter: Iterator[AccessInfo] = self._split(access_it)
		self._stats_collector: StatsCollector = StatsCollector(self._splitter)

	@property
	def stats(self) -> StatsCounters:
		return self._stats_collector.stats

	def reset_after_warm_up(self) -> None:
		self._stats_collector.reset()

	def __iter__(self) -> Iterator[AccessInfo]:
		return iter(self._stats_collector)

	def _split(self, it: Iterable[AccessAssignment]) -> Iterator[AccessInfo]:
		for cache_proc in self._processors:
			cache_proc.send(None)

		for access_assignment in it:
			cache_proc = self._processors[access_assignment.cache_proc]
			access_info = cache_proc.send(access_assignment.access)
			if access_info is not None:
				yield access_info

		for cache_proc in self._processors:
			try:
				while True:
					cache_proc.send(None)
			except StopIteration:
				pass


class GeneratorOfflineCacheSystem(CacheSystem):
	def __init__(self, processors: Iterable[Processor], access_it: Iterable[AccessAssignment]):
		self._processors: List[ProcessorGenerator] = [proc.generator() for proc in processors]
		self._access_it: Iterable[AccessAssignment] = access_it
		self._splitter: Iterator[AccessInfo] = self._split(access_it)
		self._stats_collector: StatsCollector = StatsCollector(self._splitter)

	@property
	def stats(self) -> StatsCounters:
		return self._stats_collector.stats

	def reset_after_warm_up(self) -> None:
		self._stats_collector.reset()

	def __iter__(self) -> Iterator[AccessInfo]:
		return iter(self._stats_collector)

	def _split(self, it: Iterable[AccessAssignment]) -> Iterator[AccessInfo]:
		for cache_proc in self._processors:
			cache_proc.send(None)

		for access_assignment in it:
			cache_proc = self._processors[access_assignment.cache_proc]
			cache_proc.send(access_assignment.access)

		return iter(EventMerger(
			map(self._filter_none, self._processors),
			lambda access_info: access_info.access.access_ts,
		))

	def _filter_none(self, it: Iterator[Optional[AccessInfo]]) -> Iterator[AccessInfo]:
		return cast(Iterator[AccessInfo], filter(lambda access_info: access_info is not None, it))


class OfflineCacheSystem(CacheSystem):
	def __init__(self, processors: Iterable[OfflineProcessor], accesses: SimpleReader[AccessAssignment]):
		self._accesses: SimpleReader[AccessAssignment] = accesses

		self._access_infos_it = iter(EventMerger(
			self._prepare_processors(processors, accesses),
			lambda access_info: access_info.access.access_ts),
		)
		self._stats_collector: StatsCollector = StatsCollector(self._access_infos_it)

	def _prepare_processors(self, processors: Iterable[OfflineProcessor], accesses: SimpleReader[AccessAssignment]) -> Iterator[Iterator[AccessInfo]]:
		for ind, processor in enumerate(processors):
			yield processor._process_accesses(scope_to_cache_processor(ind, accesses))

	@property
	def stats(self) -> StatsCounters:
		return self._stats_collector.stats

	def reset_after_warm_up(self) -> None:
		self._stats_collector.reset()

	def __iter__(self) -> Iterator[AccessInfo]:
		return iter(self._stats_collector)
