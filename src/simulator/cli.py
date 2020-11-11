import argparse
import collections
import csv
import functools
import itertools
import sys
from typing import Any, Callable, cast, Dict, Iterable, Iterator, Optional, Sequence, TextIO, Tuple

from .workload import Access, Task
from .workload.models.pags import (
	build as build_pags,
	load_params as load_pags_params,
)
from .workload.models.pags_single import (
	build as build_pags_single,
	load_params as load_pags_single_params,
)
from .workload.models.random import (
	build as build_random,
	load_params as load_random_params,
)
from .workload.stats import StatsCounters as WorkloadStatsCounters
from .distributor import (
	AccessAssignment,
	Distributor,
	NodeSpec,
)
from .distributor.stats import AssignmentsStatsCollector

from .dstructures.accessseq import change_to_active_bytes, change_to_active_files, FullReuseIndex

from . import recorder

from .cache import CacheSystem, OnlineCacheSystem, OfflineCacheSystem
from .cache.accesses import SimpleAccessReader
from .cache.processor import Processor, OfflineProcessor
from .cache.stats import StatsCounters as CacheStatsCounters
from .cache.storage import Storage

from .cache.algorithms.arc import ARCBit
from .cache.algorithms.eva import EVA
from .cache.algorithms.fifo import FIFO
from .cache.algorithms.greedydual import GreedyDual, Mode as GreedyDualMode
from .cache.algorithms.landlord import Landlord, Mode as LandlordMode
from .cache.algorithms.lru import LRU
from .cache.algorithms.mcf import MCF
from .cache.algorithms.min import MIN
from .cache.algorithms.mind import MIND, MINCod
from .cache.algorithms.obma import OBMA
from .cache.algorithms.rand import Rand
from .cache.algorithms.size import Size

from .utils import consume

def filter_accesses_stop_early(
	it: Iterable[AccessAssignment],
	time: Optional[int],
	accesses: Optional[int],
	total_bytes: Optional[int],
	unique_bytes: Optional[int],
) -> Iterable[AccessAssignment]:

	# Has all(current s < limit s) semantic.
	# I.e. as soon as any limit is surpassed, iteration is ended.

	if time is not None:
		t: int = time # for some reason this is required by mypy
		it = itertools.takewhile(lambda assgnm: assgnm.access.access_ts <= t, it)

	if accesses is not None:
		it = itertools.islice(it, accesses)

	if total_bytes is not None:
		raise NotImplementedError

	if unique_bytes is not None:
		raise NotImplementedError

	return it


class StopEarlyPredicate(recorder.Predicate):
	def __init__(
		self,
		time: Optional[int],
		accesses: Optional[int],
		total_bytes: Optional[int],
		unique_bytes: Optional[int],
	):
		super(StopEarlyPredicate, self).__init__()
		self._time: Optional[int] = time
		self._accesses: Optional[int] = accesses
		self._total_bytes: Optional[int] = total_bytes
		self._unique_bytes: Optional[int] = unique_bytes

		self._accesses_count: int = 0

		if total_bytes is not None:
			raise NotImplementedError

		if unique_bytes is not None:
			raise NotImplementedError

	@property
	def action(self) -> recorder.Predicate.Action:
		return recorder.Predicate.Action.YieldOnly

	@property
	def style(self) -> recorder.Predicate.Style:
		return recorder.Predicate.Style.OneRange

	def __call__(self, assgnm: AccessAssignment) -> bool:
		if self._time is not None:
			return assgnm.access.access_ts <= self._time

		if self._accesses is not None:
			self._accesses_count += 1
			return self._accesses_count <= self._accesses

		return True

	def take_while_not(self, it: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		return iter([])

	def take_while(self, it: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		return iter(filter_accesses_stop_early(
			it,
			self._time,
			self._accesses,
			self._total_bytes,
			self._unique_bytes,
		))


def record(args: Any) -> None:
	tasks = tasks_from_args(args)

	nodes = (NodeSpec(32, 10*1024*1024, 0) for _ in range(100))

	distributor = Distributor(nodes, tasks)

	if args.generate_time is None and args.generate_accesses is None:
		raise Exception('either --generate-time or --generate-accesses must be specified')

	assignment_it = filter_accesses_stop_early(
		distributor,
		args.generate_time,
		args.generate_accesses,
		None,
		None,
	)

	if args.summary_stats_file is not None:
		stats_collector = AssignmentsStatsCollector(assignment_it)

		cache_processors_counter = CacheProcessorsCounter(stats_collector)

		recorder.record_path(args.file_path, cache_processors_counter)

		write_workload_summary_stats_as_csv(stats_collector.stats, cache_processors_counter.count, args.summary_stats_file)
	else:
		recorder.record_path(args.file_path, assignment_it)

def tasks_from_args(args: Any) -> Sequence[Task]:
	if args.model == 'pags':
		return build_pags(load_pags_params(args.model_params_file), seed=args.seed)
	elif args.model == 'pags-single':
		return build_pags_single(load_pags_single_params(args.model_params_file), seed=args.seed)
	elif args.model == 'random':
		return build_random(load_random_params(args.model_params_file), seed=args.seed)
	else:
		raise NotImplementedError

def replay(args: Any) -> None:
	cache_sys = cache_system_from_args(args)

	if args.warm_up_time is not None and args.warm_up_accesses is not None:
		# should have all() semantic if both are supplied
		raise Exception('only either --warm-up-time or --warm-up-accesses may be specified')

	if args.warm_up_time is not None or args.warm_up_accesses is not None:
		if args.warm_up_time is not None:
			warm_up_time = cast(int, args.warm_up_time)
			collections.deque(itertools.takewhile(lambda info: info.access.access_ts < warm_up_time, cache_sys), maxlen=0)

		if args.warm_up_accesses is not None:
			collections.deque(itertools.islice(cache_sys, args.warm_up_accesses), maxlen=0)

		cache_sys.stats.reset()

	if args.cache_info_file_path is not None:
		recorder.record_access_info_path(args.cache_info_file_path, cache_sys)
	else:
		consume(cache_sys)

	if args.summary_stats_file is not None:
		write_cache_summary_stats_as_csv(cache_sys.stats, args.summary_stats_file)

def write_cache_summary_stats_as_csv(counters: CacheStatsCounters, file: TextIO) -> None:
	writer = csv.writer(file)

	writer.writerow([
		'accesses',
		'files',
		'total_bytes_accessed',
		'unique_bytes_accessed',
		'files_hit',
		'files_missed',
		'bytes_hit',
		'bytes_missed',
		'bytes_added',
		'bytes_removed',
	])

	writer.writerow([
		counters.total_stats.accesses,
		len(counters.files_stats),
		counters.total_stats.total_bytes_accessed,
		counters.total_stats.unique_bytes_accessed,
		counters.total_stats.files_hit,
		counters.total_stats.files_missed,
		counters.total_stats.bytes_hit,
		counters.total_stats.bytes_missed,
		counters.total_stats.bytes_added,
		counters.total_stats.bytes_removed,
	])

def write_workload_summary_stats_as_csv(counters: WorkloadStatsCounters, cache_processors_count: int, file: TextIO) -> None:
	writer = csv.writer(file)

	writer.writerow([
		'accesses',
		'files',
		'total_bytes_accessed',
		'unique_bytes_accessed',
		'cache_processors_count',
	])

	writer.writerow([
		counters.total_stats.accesses,
		len(counters.files_stats),
		counters.total_stats.total_bytes_accessed,
		counters.total_stats.unique_bytes_accessed,
		cache_processors_count,
	])

def cache_system_from_args(args: Any) -> CacheSystem:
	processor_factory, online, offline = processor_factory_from_args(args)
	processors: Iterator[Processor]

	if args.shared_storage:
		storage = Storage(args.storage_size)
		processors = (processor_factory(storage) for _ in range(args.cache_processors_count))
	else:
		processors = map(lambda f: f(Storage(args.storage_size)), itertools.repeat(processor_factory, args.cache_processors_count))

	if online:
		access_it = filter_accesses_stop_early(
			recorder.replay_path(args.file_path),
			args.process_time,
			args.process_accesses,
			None,
			None,
		)

		return OnlineCacheSystem(processors, access_it)
	elif offline:
		predicate: Optional[recorder.Predicate] = None
		if args.process_time is not None or args.process_accesses is not None:
			predicate = StopEarlyPredicate(
				args.process_time,
				args.process_accesses,
				None,
				None,
			)

		reader = recorder.Reader(args.file_path, predicate=predicate)
		return OfflineCacheSystem(cast(Iterator[OfflineProcessor], processors), reader)
	else:
		raise NotImplementedError

def processor_factory_from_args(args: Any) -> Tuple[Callable[[Storage], Processor], bool, bool]:
	user_args: str = args.cache_processor_args if args.cache_processor_args is not None else ''
	partial = functools.partial

	if args.cache_processor == 'arcbit':
		return partial(ARCBit, ARCBit.Configuration.from_user_args(user_args)), True, False
	elif args.cache_processor == 'eva':
		return partial(EVA, EVA.Configuration.from_user_args(user_args)), True, False
	elif args.cache_processor == 'fifo':
		return FIFO, True, False
	elif args.cache_processor == 'greedydual':
		return partial(GreedyDual, GreedyDual.Configuration.from_user_args(user_args)), True, False
	elif args.cache_processor == 'landlord':
		return partial(Landlord, Landlord.Configuration.from_user_args(user_args)), True, False
	elif args.cache_processor == 'lru':
		return LRU, True, False
	elif args.cache_processor == 'mcf':
		return MCF, True, False
	elif args.cache_processor == 'min':
		return MIN, False, True
	elif args.cache_processor == 'mincod':
		return partial(MINCod, MINCod.Configuration.from_user_args(user_args)), False, True
	elif args.cache_processor == 'mind':
		return partial(MIND, MIND.Configuration.from_user_args(user_args)), False, True
	elif args.cache_processor == 'obma':
		return partial(OBMA, OBMA.Configuration.from_user_args(user_args)), False, True
	elif args.cache_processor == 'rand':
		return Rand, True, False
	elif args.cache_processor == 'size':
		return Size, True, False
	else:
		raise NotImplementedError

def convert_accesses_to_monitoring(args: Any) -> None:
	writer = csv.writer(sys.stdout)
	writer.writerow([
		'Inputfiles',
		'JobCurrentStartExecutingDate',
		'CompletionDate',
		'WaitingTime',
		'Walltime',
	])

	for assgnm in recorder.replay(args.file):
		writer.writerow([
			assgnm.access.file,
			assgnm.access.access_ts,
			0,
			0,
			0,
		])

def workload_stats(args: Any) -> None:
	if args.summary_stats_file is None and args.accesses_stats_file is None and args.files_stats_file is None:
		raise Exception('At least one "*_stats_file" parameter has to be specified.')

	reader = recorder.Reader(args.file_path)
	it: Iterable[AccessAssignment] = reader

	if args.cache_processor_index is not None:
		raise NotImplementedError('Cannot scope to cache processor.')

	it = collector = AssignmentsStatsCollector(reader)

	if args.accesses_stats_file is not None:
		it = write_yield_accesses_stats_as_csv(reader, collector, args.accesses_stats_file)

	it = cache_processors_counter = CacheProcessorsCounter(it)

	consume(it)

	if args.files_stats_file is not None:
		write_files_stats_as_csv(collector.stats, args.files_stats_file)

	if args.summary_stats_file is not None:
		write_workload_summary_stats_as_csv(collector.stats, cache_processors_counter.count, args.summary_stats_file)


class CacheProcessorsCounter(object):
	def __init__(self, it: Iterable[AccessAssignment]) -> None:
		self._it: Iterable[AccessAssignment] = it
		self._count: int = 0

	@property
	def count(self) -> int:
		return self._count

	def __iter__(self) -> Iterator[AccessAssignment]:
		max_cache_processor_index = 0
		self._count = max_cache_processor_index + 1
		for assgnm in self._it:
			if assgnm.cache_proc > max_cache_processor_index:
				max_cache_processor_index = assgnm.cache_proc
				self._count = max_cache_processor_index + 1
			yield assgnm


def write_yield_accesses_stats_as_csv(
	reader: recorder.Reader,
	collector: AssignmentsStatsCollector,
	stats_file: TextIO,
) -> Iterator[AccessAssignment]:
	full_reuse_index = FullReuseIndex(reader.drop_cache_processor())

	writer = csv.writer(stats_file)
	writer.writerow([
		'time',
		'file_key',
		'access_size',
		'access_parts_count',
		'cache_processor',
		'total_accesses',
		'total_bytes',
		'total_unique_bytes',
		'total_unique_files',
		'active_files',
		'active_bytes',
	])

	active_files = 0
	active_bytes = 0
	for ind, assignment in enumerate(collector):
		active_files += change_to_active_files(full_reuse_index, ind)
		active_bytes += change_to_active_bytes(full_reuse_index, ind)

		writer.writerow([
			assignment.access.access_ts,
			assignment.access.file,
			sum(size for _, size in assignment.access.parts),
			len(assignment.access.parts),
			assignment.cache_proc,
			collector.stats.total_stats.accesses,
			collector.stats.total_stats.total_bytes_accessed,
			collector.stats.total_stats.unique_bytes_accessed,
			len(collector.stats.files_stats),
			active_files,
			active_bytes,
		])

		yield assignment

def write_files_stats_as_csv(counters: WorkloadStatsCounters, stats_file: TextIO) -> None:
	writer = csv.writer(stats_file)
	writer.writerow([
		'file_key',
		'total_accesses',
		'total_bytes_accessed',
		'unique_bytes_accessed',
		'parts_accessed',
		'first_access_time',
		'last_access_time',
	])

	for file_stats in counters.files_stats:
		writer.writerow([
			file_stats.id,
			file_stats.accesses,
			file_stats.total_bytes_accessed,
			file_stats.unique_bytes_accessed,
			len(file_stats.parts),
			file_stats.first_access_time,
			file_stats.last_access_time,
		])

parser = argparse.ArgumentParser(description='Simulate HTC cache system.')
subparsers = parser.add_subparsers(dest='command', required=True)

parser_record = subparsers.add_parser('record', help='Generate cache-seen accesses by jobs yielded by a workload generator and scheduled to a node set.')
parser_record.add_argument('-f', '--file', required=True, type=str, dest='file_path', help='output file to which the accesses are written.')
parser_record.add_argument('--generate-accesses', type=int, help='number of accesses to generate. Limit; iterates as long as all limits hold semantic.')
parser_record.add_argument('--generate-time', type=int, help='number of seconds to generate. Limit; iterates as long as all limits hold semantic.') # missing: unique bytes read, total bytes read
parser_record.add_argument('--summary-stats-file', type=argparse.FileType('w'), help='output file to which summary stats about the entire access sequence (headers and one row) are written as CSV.')
parser_record.add_argument('--model', required=True, type=str, help='workload model used to generate the workload.')
parser_record.add_argument('--model-params-file', required=True, type=argparse.FileType('r'), help='JSON file containing the parameters for the workload model.')
parser_record.add_argument('--seed', type=int, help='Seed for initialising the pseudo-random number generator. If not passed a seed is chosen by the python default behaviour.')

parser_replay = subparsers.add_parser('replay', help='Perform cache algorithms on a recorded sequence of accesses.')
parser_replay.add_argument('-f', '--file', required=True, type=str, dest='file_path', help='input file from which the accesses are read.')
parser_replay.add_argument('--warm-up-accesses', type=int, help='number of accesses considered cache warm-up.') # all() or or() semantic? # may actually consume one access more
parser_replay.add_argument('--warm-up-time', type=int, help='number of seconds considered cache warm-up.') # missing: unique bytes read, total bytes read
parser_replay.add_argument('--process-accesses', type=int, help='number of accesses to be processed (including warm-up). Limit; iterates as long as all limits hold semantic.')
parser_replay.add_argument('--process-time', type=int, help='number of seconds to be processed (including warm-up). Limit; iterates as long as all limits hold semantic.') # missing: unique bytes read, total bytes read
parser_replay.add_argument('--cache-processors-count', type=int, default=1, help='number of simulated cache processors, must match recorded accesses.')
parser_replay.add_argument('--cache-processor', required=True, type=str, help='cache processor type (algorithm to use) to be simulated.')
parser_replay.add_argument('--cache-processor-args', type=str, help='arguments passed to the each cache processor.')
parser_replay.add_argument('--storage-size', required=True, type=int, help='size of the cache storage volumes in bytes.')
parser_replay.add_argument('--non-shared-storage', action='store_false', dest='shared_storage', help='each cache processor receives its own storage volume if set.')
parser_replay.add_argument('--cache-info-file', type=str, dest='cache_info_file_path', help='output file to which cache info data is written, i.e. hit and miss info for each access.')
parser_replay.add_argument('--summary-stats-file', type=argparse.FileType('w'), help='output file to which summary stats about the cache system (headers and one row) are written as CSV.')

parser_convert_accesses_to_monitoring = subparsers.add_parser('convert-accesses-to-monitoring', help='Converts a file of access sequences to the same format as used for monitoring data (job trace).')
parser_convert_accesses_to_monitoring.add_argument('-f', '--file', required=True, type=argparse.FileType('r'), help='input file from which the accesses are read.')

parser_workload_stats = subparsers.add_parser('workload-stats', help='Computes comprehensive stats on an access sequence.')
parser_workload_stats.add_argument('-f', '--file', required=True, type=str, dest='file_path', help='input file from which the accesses are read.')
parser_workload_stats.add_argument('--accesses-stats-file', type=argparse.FileType('w'), help='output file to which stats about accesses (one row per access) are written as CSV.')
parser_workload_stats.add_argument('--files-stats-file', type=argparse.FileType('w'), help='output file to which stats about files (one row per file) are written as CSV.')
parser_workload_stats.add_argument('--summary-stats-file', type=argparse.FileType('w'), help='output file to which summary stats about the entire access sequence (headers and one row) are written as CSV.')
parser_workload_stats.add_argument('--cache-processor-index', type=int, help='index of the cache processor for which stats are computed. If omitted, all cache processors are included.')

def main() -> None:
	args = parser.parse_args(sys.argv[1:])

	if args.command == 'record':
		record(args)
	elif args.command == 'replay':
		replay(args)
	elif args.command == 'convert-accesses-to-monitoring':
		convert_accesses_to_monitoring(args)
	elif args.command == 'workload-stats':
		workload_stats(args)
	else:
		raise NotImplementedError(f'No command {args.command!r}, see --help for usage info.')

if __name__ == '__main__':
	main()
