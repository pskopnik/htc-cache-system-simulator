from simulator.workload.physicsgroups.builder import build_physics_groups
from simulator.distributor.scheduler import NodeSpec
from simulator.distributor import Distributor

from simulator import recorder

from simulator.distributor.stats import StatsCollector
from simulator.cache.stats import StatsCollector as CacheStatsCollector

from simulator.cache import OnlineCacheSystem, OfflineCacheSystem
from simulator.cache.storage import Storage

from simulator.cache.algorithms.lru import LRU
from simulator.cache.algorithms.rand import Rand
from simulator.cache.algorithms.landlord import Landlord, Mode
from simulator.cache.algorithms.min import MIN

import argparse
import collections
import itertools
import sys

from typing import Callable, Iterable, Iterator, TypeVar

T = TypeVar('T')

def for_each(f: Callable[[T], None], it: Iterable[T]) -> Iterator[T]:
	return map(
		lambda x: (x, f(x))[0],
		it,
	)

def print_access_stats(stats: StatsCollector, prefix: str="") -> None:
	if len(prefix) > 0 and len(prefix.rstrip()) == len(prefix):
		prefix += " "

	print(prefix + "accesses", stats._total_stats.accesses)
	print(prefix + "files", len(stats._files_stats))
	print(prefix + "total_bytes_accessed", stats._total_stats.total_bytes_accessed)
	print(prefix + "unique_bytes_accessed", stats._total_stats.unique_bytes_accessed)

	print("")

	print(prefix + "avg accesses per byte", stats._total_stats.total_bytes_accessed / stats._total_stats.unique_bytes_accessed)
	print(prefix + "theoretical best byte miss rate", (stats._total_stats.unique_bytes_accessed) / stats._total_stats.total_bytes_accessed)
	print(prefix + "theoretical best byte hit rate", (stats._total_stats.total_bytes_accessed - stats._total_stats.unique_bytes_accessed) / stats._total_stats.total_bytes_accessed)

def print_cache_stats(stats: CacheStatsCollector, prefix: str="") -> None:
	if len(prefix) > 0 and len(prefix.rstrip()) == len(prefix):
		prefix += " "

	print(prefix + "accesses", stats._total_stats.accesses)
	print(prefix + "files", len(stats._files_stats))
	print(prefix + "total_bytes_accessed", stats._total_stats.total_bytes_accessed)
	print(prefix + "unique_bytes_accessed", stats._total_stats.unique_bytes_accessed)
	print(prefix + "bytes_hit", stats._total_stats.bytes_hit)
	print(prefix + "bytes_missed", stats._total_stats.bytes_missed)
	print(prefix + "bytes_added", stats._total_stats.bytes_added)
	print(prefix + "bytes_removed", stats._total_stats.bytes_removed)

	print("")

	print(prefix + "avg accesses per byte", stats._total_stats.total_bytes_accessed / stats._total_stats.unique_bytes_accessed)
	print(prefix + "theoretical best byte miss rate", (stats._total_stats.unique_bytes_accessed) / stats._total_stats.total_bytes_accessed)
	print(prefix + "theoretical best byte hit rate", (stats._total_stats.total_bytes_accessed - stats._total_stats.unique_bytes_accessed) / stats._total_stats.total_bytes_accessed)

	print("")

	print(prefix + "byte miss rate", stats._total_stats.bytes_missed / stats._total_stats.total_bytes_accessed)
	print(prefix + "byte hit rate", stats._total_stats.bytes_hit / stats._total_stats.total_bytes_accessed)


def full_manual() -> None:
	tasks = build_physics_groups()

	# nodes = (NodeSpec(1, 10*1024*1024, 0) for _ in range(1))
	nodes = (NodeSpec(32, 10*1024*1024, 0) for _ in range(31))

	distributor = Distributor(1, nodes, tasks)

	generate_time = 24 * 60 * 60
	# generate_time = (2 * 365 * 24 * 60 * 60) + (30 * 24 * 60 * 60)
	access_it = distributor
	access_it = itertools.takewhile(lambda assgnm: assgnm.access.access_ts < generate_time, access_it)

	with open("test.json", "w") as f:
		assignment_it = recorder.passthrough_record(f, access_it)

		storage = Storage(500 * 2 ** (10 * 3))

		processors = (LRU(storage) for _ in range(1))

		cache_sys = OnlineCacheSystem(processors, assignment_it)

		collections.deque(cache_sys, maxlen=0)

	print_access_stats(distributor.stats, prefix="[dist]")

	print("")
	print("")
	print("")

	print_cache_stats(cache_sys.stats, prefix="[cache sys]")

def record_manual() -> None:
	tasks = build_physics_groups()

	# nodes = (NodeSpec(1, 10*1024*1024, 0) for _ in range(1))
	nodes = (NodeSpec(32, 10*1024*1024, 0) for _ in range(31))

	distributor = Distributor(1, nodes, tasks)

	# generate_time = (2 * 365 * 24 * 60 * 60) + (30 * 24 * 60 * 60)
	generate_time = 30 * 24 * 60 * 60
	access_it = distributor
	access_it = itertools.takewhile(lambda assgnm: assgnm.access.access_ts < generate_time, access_it)

	with open("min_test.json", "w") as f:
		recorder.record(f, access_it)

	print_access_stats(distributor.stats, prefix="[dist]")

def replay_manual() -> None:
	storage = Storage(1 * 2 ** (10 * 4))

	processors = (LRU(storage) for _ in range(1))

	warm_up_time = (1 * 365 * 24 * 60 * 60) # TODO

	with open("min_test.json", "r") as f:
		access_it = recorder.replay(f)
		cache_sys = OnlineCacheSystem(processors, access_it)
		#collections.deque(itertools.takewhile(lambda info: info.access.access_ts < warm_up_time, cache_sys), maxlen=0)
		#print_cache_stats(cache_sys.stats, prefix="[pre reset cache sys]")
		#print("")
		#print("")
		#cache_sys.stats.reset()
		collections.deque(cache_sys, maxlen=0)

	print_cache_stats(cache_sys.stats, prefix="[cache sys]")

if __name__ == '__main__':
	replay_manual()
