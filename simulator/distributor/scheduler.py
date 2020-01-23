import heapq
from typing import cast, Iterable, Iterator, List, Tuple
import math
from ..workload import Access, AccessScheme, Job, TimeStamp
from ..events import EventIterator


class NodeSpec(object):
	__slots__ = ['cores', 'core_throughput', 'cache_proc']

	def __init__(self, cores: int, core_throughput: int, cache_proc: int):
		self.cores = cores
		self.core_throughput = core_throughput
		self.cache_proc = cache_proc


class Core(object):
	__slots__ = ['node_spec']

	def __init__(self, node_spec: NodeSpec):
		self.node_spec = node_spec


class AccessAssignment(object):
	__slots__ = ['access', 'cache_proc']

	def __init__(self, access: Access, cache_proc: int):
		self.access: Access = access
		self.cache_proc: int = cache_proc


class JobAssignment(object):
	__slots__ = ['job', 'core', 'start_ts', 'end_ts', 'total_bytes_read']

	def __init__(self, job: Job, core: Core, start_ts: TimeStamp, end_ts: TimeStamp, total_bytes_read: int):
		self.job: Job = job
		self.core: Core = core
		self.start_ts: TimeStamp = start_ts # Job is started at / after start_ts
		self.end_ts: TimeStamp = end_ts # Job has been completed at end_ts
		self.total_bytes_read: int = total_bytes_read


class JobScheduler(object):
	"""Schedules jobs to cores.

    Artificially stretches the end time to the end of the second.

      job_start_ts   true end job_end_ts
      |                     | |
	  ____________________________________
	t 0   1   2   3   4   5   6   7   8
	"""
	def __init__(self, cache_proc: int, node_specs: Iterable[NodeSpec], jobs_it: Iterable[Job]):
		self._jobs_it: Iterable[Job] = jobs_it

		self._queue_index: int = 0
		self._heap: List[Tuple[TimeStamp, int, Core]] = list(self._create_core_state(node_specs))
		self.latest_event_ts: int = 0

	def _create_core_state(self, node_specs: Iterable[NodeSpec]) -> Iterable[Tuple[TimeStamp, int, Core]]:
		for node_spec in node_specs:
			for i in range(node_spec.cores):
				el = (0, self._queue_index, Core(node_spec))
				self._queue_index += 1
				yield el

	def __iter__(self) -> Iterator[JobAssignment]:
		for job in self._jobs_it:
			free_ts, _, core = self._heap[0]
			job_start_ts = max(cast(int, job.submit_ts), free_ts)
			self.latest_event_ts = job_start_ts
			total_bytes_read = sum(
				bytes_read for scheme in job.access_schemes for _, bytes_read in scheme.parts
			)
			job_end_ts = job_start_ts + math.ceil(total_bytes_read / core.node_spec.core_throughput)

			heapq.heapreplace(self._heap, (job_end_ts, self._queue_index, core))
			self._queue_index += 1

			yield JobAssignment(job, core, job_start_ts, job_end_ts, total_bytes_read)


class AccessScheduler(object):
	"""Schedules individual accesses as observed by cache processors.

	Invariant: All accesses before the first event on the heap have been
	yielded.

	1) All jobs which start before the first event on the heap have been added
	   to the heap. Ensured by _prepare_heap().

	2) All accesses before the first event on the heap from jobs which have
	   been added to the heap have been yielded. Ensured by the heap in
	   combination with __iter__.


    What is the model for overlapping accesses?
    How is start and end defined? (no need for the definition of the end of an access)

        end of access / start of next
            |
      access_ts
      |      -
      job_start_ts   true end job_end_ts
      |                     | |
	  ____________________________________
	t 0   1   2   3   4   5   6   7   8

	"""
	def __init__(self, assignment_it: Iterable[JobAssignment]):
		self._assignment_it: EventIterator[JobAssignment] = EventIterator(
			assignment_it,
			lambda assignment: assignment.start_ts,
		)
		self._heap: List[Tuple[TimeStamp, int, JobAssignment, int, Iterator[AccessScheme]]] = []
		self._queue_index: int = 0
		self.latest_event_ts: int = 0

	def __iter__(self) -> Iterator[AccessAssignment]:
		while True:
			try:
				self._prepare_heap()
			except StopIteration:
				return

			ts, _, assignment, bytes_read, it = self._heap[0]
			self.latest_event_ts = ts
			try:
				scheme = next(it)
			except StopIteration:
				# All schemes processed, this job is done
				heapq.heappop(self._heap)
				continue

			bytes_read += sum(
				bytes_read for _, bytes_read in scheme.parts
			)
			next_access_ts = assignment.start_ts + math.floor(
				bytes_read / assignment.core.node_spec.core_throughput,
			)
			heapq.heapreplace(self._heap, (
				next_access_ts,
				self._queue_index,
				assignment,
				bytes_read,
				it,
			))
			self._queue_index += 1

			yield AccessAssignment(
				Access(
					ts,
					scheme.file,
					scheme.parts,
				),
				assignment.core.node_spec.cache_proc,
			)

	def _prepare_heap(self) -> None:
		try:
			ts, _, _, _, _ = self._heap[0]
		except IndexError:
			# PQ is empty, retrieve next job assignment...
			# May throw StopIteration which is propagated upwards
			job_assignment = self._assignment_it.next()
			self._push_assignment(job_assignment)
			ts = job_assignment.start_ts

		while True:
			try:
				opt_job_assignment = self._assignment_it.next_if_before(ts)
			except StopIteration:
				break
			if opt_job_assignment is None:
				break
			self._push_assignment(opt_job_assignment)

	def _push_assignment(self, assignment: JobAssignment) -> None:
		heapq.heappush(self._heap, (
			assignment.start_ts,
			self._queue_index,
			assignment,
			0,
			iter(assignment.job.access_schemes),
		))
		self._queue_index += 1


class Scheduler(object):
	def __init__(self, cache_proc: int, node_specs: Iterable[NodeSpec], jobs_it: Iterable[Job]):
		self._job_scheduler: JobScheduler = JobScheduler(cache_proc, node_specs, jobs_it)
		self._access_scheduler: AccessScheduler = AccessScheduler(self._job_scheduler)

	def __iter__(self) -> Iterator[AccessAssignment]:
		return iter(self._access_scheduler)

	def now(self) -> TimeStamp:
		return max(
			self._job_scheduler.latest_event_ts,
			self._access_scheduler.latest_event_ts,
		)
