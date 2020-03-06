import abc
import itertools
import timeit
from typing import cast, Iterable, Iterator, List, Optional, Sequence, Tuple

# This benchmarks replicates the Predicate system of the recorder module.
#
# There are two interfaces available for evaluating predicates on an iterable
# of accesses. This benchmarks allows to compare the two.
# The specialised "take_while" interface allows the use of itertools and other
# C-implemented iterators. This can speed up execution for some kind of
# predicates by saving invocations of interpreted Python code.
#
# This benchmark indicates that the speed-up is not very significant. In the
# best case scenario, the speed up is 2. The predicate can be implemented with
# virtually no Python code for that scenario. The predicate is evaluated in an
# I/O + deserialisation process where I/O dominates run-time.
# The take_while interface should thus be removed if the opportunity for a
# refactor arises.

FileID = str
PartSpec = Tuple[int, int] # (part_ind, bytes_read)
TimeStamp = int

class Access(object):
	__slots__ = ['access_ts', 'file', 'parts']

	def __init__(self, access_ts: TimeStamp, file: FileID, parts: Sequence[PartSpec]):
		self.access_ts: TimeStamp = access_ts
		self.file: FileID = file
		self.parts: Sequence[PartSpec] = parts


class AccessAssignment(object):
	__slots__ = ['access', 'cache_proc']

	def __init__(self, access: Access, cache_proc: int):
		self.access: Access = access
		self.cache_proc: int = cache_proc


def filter_accesses_stop_early(
	it: Iterable[AccessAssignment],
	time: Optional[int],
	accesses: Optional[int],
) -> Iterable[AccessAssignment]:
	if time is not None:
		t: int = time
		it = itertools.takewhile(lambda assgnm: assgnm.access.access_ts <= t, it)

	if accesses is not None:
		it = itertools.islice(it, accesses)

	return it


class Predicate(abc.ABC):
	@abc.abstractmethod
	def __call__(self, assgnm: AccessAssignment) -> bool:
		raise NotImplementedError

	def take_while(self, assgnm: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		raise NotImplementedError

	def take_while_not(self, assgnm: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		raise NotImplementedError


class StopEarlyPredicate(Predicate):
	def __init__(
		self,
		time: Optional[int],
		accesses: Optional[int],
	):
		super(StopEarlyPredicate, self).__init__()
		self._time: Optional[int] = time
		self._accesses: Optional[int] = accesses

		self._accesses_count: int = 0

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
		))


def evaluate_with_take_while_interface(p: Predicate, assgnm_it: Iterator[AccessAssignment]) -> int:
	length: int = 0
	read_count: int = 0
	taken_count: int = 0

	def wrap_for_read_count(it: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		nonlocal read_count
		for el in it:
			read_count += 1
			yield el

	it = wrap_for_read_count(assgnm_it)

	for _ in p.take_while_not(it):
		taken_count += 1

	if read_count > taken_count:
		# take_while_not() over-read: at least one assgnm was not yielded
		length += read_count - taken_count

	for _ in p.take_while(it):
		length += 1

	return length

def evaluate_with_call_interface(p: Predicate, assgnm_it: Iterator[AccessAssignment]) -> int:
	length: int = 0
	read_count: int = 0

	def wrap_for_read_count(it: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		nonlocal read_count
		for el in it:
			read_count += 1
			yield el

	it = wrap_for_read_count(assgnm_it)

	for assgnm in it:
		if p(assgnm):
			break

		length += 1

	length += 1

	for assgnm in it:
		if not p(assgnm):
			break

		length += 1

	return length

def generate_assignments(n: int) -> Iterator[AccessAssignment]:
	ts: int = 0
	file: FileID = ""
	parts: List[PartSpec] = []

	for _ in range(n):
		yield AccessAssignment(Access(ts, file, parts), 0)
		ts += 10

def time_accesses_take_while_interface() -> Tuple[int, float]:
	def do() -> None:
		it = generate_assignments(10000)
		predicate = StopEarlyPredicate(time=None, accesses=6000)
		evaluate_with_take_while_interface(predicate, it)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_accesses_call_interface() -> Tuple[int, float]:
	def do() -> None:
		it = generate_assignments(10000)
		predicate = StopEarlyPredicate(time=None, accesses=6000)
		evaluate_with_call_interface(predicate, it)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_time_take_while_interface() -> Tuple[int, float]:
	def do() -> None:
		it = generate_assignments(10000)
		predicate = StopEarlyPredicate(time=6000*10, accesses=None)
		evaluate_with_take_while_interface(predicate, it)

	timer = timeit.Timer(do)
	return timer.autorange()

def time_time_call_interface() -> Tuple[int, float]:
	def do() -> None:
		it = generate_assignments(10000)
		predicate = StopEarlyPredicate(time=6000*10, accesses=None)
		evaluate_with_call_interface(predicate, it)

	timer = timeit.Timer(do)
	return timer.autorange()

benchmark_functions = [
	time_accesses_take_while_interface,
	time_accesses_call_interface,
	time_time_take_while_interface,
	time_time_call_interface,
]

def main() -> None:
	res = []

	for f in benchmark_functions:
		rep, dur = f()
		res.append((f.__name__, (dur / rep, rep, dur)))
		# print(*res[-1])

	print("\n")

	for k in sorted(res, key=lambda el: el[1][0]):
		print(*k)

if __name__ == '__main__':
	main()
