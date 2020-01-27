from io import SEEK_SET, SEEK_CUR, SEEK_END
from os import PathLike
from typing import Any, Callable, cast, Dict, Iterable, Iterator, Optional, TextIO, Tuple
from .distributor import AccessAssignment
from .workload import Access
import json
from enum import auto, Enum
import abc


class _EndOfFile(Exception):
	pass


class _ReplayStateAccessor(object):
	def __init__(self, file: TextIO) -> None:
		self._file: TextIO = file

	@property
	def file(self) -> TextIO:
		return self._file


def replay(file: TextIO) -> Iterator[AccessAssignment]:
	return _replay(file)

def reverse_replay(file: TextIO) -> Iterator[AccessAssignment]:
	return _reverse_replay(file)

def replay_path(path: 'PathLike[Any]') -> Iterator[AccessAssignment]:
	with open(path) as file:
		for assgnm in replay(file):
			yield assgnm

def reverse_replay_path(path: 'PathLike[Any]') -> Iterator[AccessAssignment]:
	with open(path) as file:
		for assgnm in reverse_replay(file):
			yield assgnm

def _replay(
	file: TextIO,
	begin_pos: int = 0,
	end_pos: Optional[int] = None,
	state_cb: Optional[Callable[[_ReplayStateAccessor], None]] = None,
) -> Iterator[AccessAssignment]:
	file.seek(begin_pos, SEEK_SET)

	# This hacky workaround exposes the internal state of the _replay()
	# iterator to a caller while using a Python generator function.
	if state_cb is not None:
		state_cb(_ReplayStateAccessor(file))

	while True:
		try:
			# TODO will over-read: should abort at end_pos
			yield _read_access(file)
			if end_pos is not None and file.tell() >= end_pos:
				break
		except _EndOfFile:
			break

def _reverse_replay(
	file: TextIO,
	begin_pos: int = 0,
	end_pos: Optional[int] = None,
) -> Iterator[AccessAssignment]:
	if end_pos is not None:
		file.seek(end_pos, SEEK_SET)
	else:
		file.seek(0, SEEK_END)

	buf = ""
	chunk_size = 8 * 1024
	exhausted = False
	start_ind = 0

	# pos is the absolute index in the file at which buf starts
	pos = file.tell()
	new_pos = pos

	while True:
		# Read chunk from file (in reverse direction)
		new_pos = file.seek(max(begin_pos, pos - chunk_size), SEEK_SET)
		read_chunk = file.read(pos - new_pos)
		if len(read_chunk) < chunk_size:
			exhausted = True

		buf = read_chunk + buf
		pos = new_pos

		while True:
			buf = buf.rstrip()

			try:
				start_ind = buf.rindex('\n') + 1
			except ValueError:
				if exhausted:
					# at beginning of file, '\n' at -1
					start_ind = 0
					# TODO: is this if required or result of invalid parameters?
					# Could occur if the first chunk starts exactly at the file's start
					if len(buf) == 0:
						return
				else:
					# read previous chunk into buf
					break

			l = buf[start_ind:]
			buf = buf[:start_ind]
			dct = json.loads(l)
			yield _dct_to_access(dct)

			if exhausted and start_ind == 0:
				return

def filter_cache_processor(cache_proc: int, it: Iterable[AccessAssignment]) -> Iterator[Access]:
	return map(
		lambda assgnm: assgnm.access,
		filter(lambda assgnm: assgnm.cache_proc == cache_proc, it),
	)

def record(file: TextIO, it: Iterable[AccessAssignment]) -> None:
	for access in it:
		_write_access(file, access)

def passthrough_record(file: TextIO, it: Iterable[AccessAssignment]) -> Iterator[AccessAssignment]:
	for access in it:
		_write_access(file, access)
		yield access

def _read_access(file: TextIO) -> AccessAssignment:
	l = file.readline()
	if len(l) == 0:
		raise _EndOfFile()

	dct = json.loads(l)
	return _dct_to_access(dct)

def _write_access(file: TextIO, assgnm: AccessAssignment) -> None:
	json.dump(_access_to_dct(assgnm), file)
	file.write('\n')

def _access_to_dct(assgnm: AccessAssignment) -> Dict[str, Any]:
	return {
		'access': {
			'access_ts': assgnm.access.access_ts,
			'file': assgnm.access.file,
			'parts': assgnm.access.parts,
		},
		'cache_proc': assgnm.cache_proc,
	}

def _dct_to_access(dct: Dict[str, Any]) -> AccessAssignment:
	access = dct['access']
	return AccessAssignment(
		Access(
			access['access_ts'],
			access['file'],
			# TODO: These __getitem__ calls are expensive!
			list(map(cast(Callable[[Any], Tuple[int, int]], tuple), access['parts'])),
		),
		dct['cache_proc'],
	)


class _ForwardsCursor(object):
	def __init__(self, file: TextIO, cache_proc: int) -> None:
		file.seek(0, SEEK_SET)
		self._file: TextIO = file
		self._cache_proc: int = cache_proc

	def __iter__(self) -> Iterator[Access]:
		return self

	def __next__(self) -> Access:
		while True:
			try:
				assgnm = _read_access(self._file)
				if assgnm.cache_proc == self._cache_proc:
					return assgnm.access
			except _EndOfFile:
				raise StopIteration()


class Predicate(abc.ABC):
	"""ABC for Predicate checkers.

	__call__ is called for each access assignment in the order of the
	sequence. Generally speaking, the predicate is evaluated for an assignment
	sequence by performing __call__ on each assignment in order.

	If the predicates has any kind of internal state dependent on the access
	stream, a new instance must be created for each reader.
	"""

	class Action(Enum):
		NoOp = auto() # No action is taken
		YieldOnly = auto() # Only yield if self(assgnm) == True

	class Style(Enum):
		OneRange = auto() # There is one contiguous range for which the predicate holds

	@property
	@abc.abstractmethod
	def action(self) -> 'Predicate.Action':
		raise NotImplementedError

	@property
	@abc.abstractmethod
	def style(self) -> 'Predicate.Style':
		raise NotImplementedError

	@abc.abstractmethod
	def __call__(self, assgnm: AccessAssignment) -> bool:
		raise NotImplementedError

	def take_while(self, assgnm: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		"""Re-yield all assignments until the predicate holds.

		This method may be implemented optionally by checkers in order to
		accelerate evaluation. It is tried for checkers of appropriate style
		before using __call__.

		TODO: Should parameter and return types be Iterable[AccessAssignment].
		"""
		raise NotImplementedError

	def take_while_not(self, assgnm: Iterator[AccessAssignment]) -> Iterator[AccessAssignment]:
		"""Re-yield all assignments as long as the predicate fails.

		This method may be implemented optionally by checkers in order to
		accelerate evaluation. It is tried for checkers of appropriate style
		before using __call__.

		TODO: Should parameter and return types be Iterable[AccessAssignment].
		"""
		raise NotImplementedError


class Reader(object):
	"""Represents a sequence of access assignments readable from a file.

	Reader implements the SimpleReader[AccessAssignment] interface.
	Most importantly, Reader provides the __iter__ and __reversed__ methods to
	create iterators reading assignments ad-hoc from the path.

	The file is represented by its path on the local file system. The file is
	opened once for each iterator instance.

	Predicates:

	The most basic use-case for predicates is stopping processing of the access
	assignments after a number of assignments. The issue here is that the Reader
	instance should provide a static view onto its list of access assignments.
	That precludes filtering of __iter__ by the caller. __reversed__ and
	__len__ would cause problems especially.

	More generally, a predicate is a boolean value retrievable for each access
	assignment in the sequence. Predicates may have specific effects on the
	behaviour of Reader. For example, to skip the access assignment and not
	return it. Other predicates may be passed on to the caller, e.g. whether
	an access assignment is part of the warm-up range. Range-style predicates
	can be checked and enforced more efficiently.

	"""

	class CacheProcessorScoped(object):
		def __init__(self, reader: 'Reader', cache_proc: int):
			self._reader: 'Reader' = reader
			self._cache_proc: int = cache_proc
			self._len: Optional[int] = None

		def __iter__(self) -> Iterator[Access]:
			return filter_cache_processor(self._cache_proc, iter(self._reader))

		def __reversed__(self) -> Iterator[Access]:
			return filter_cache_processor(self._cache_proc, reversed(self._reader))

		def __len__(self) -> int:
			if self._len is None:
				l = sum(map(lambda _: 1, iter(self)))
				self._len = l
				return l
			else:
				return self._len

	def __init__(self, path: 'PathLike[Any]', predicate: Optional[Predicate]=None) -> None:
		self._path: 'PathLike[Any]' = path
		self._predicate: Optional[Predicate] = predicate

		self._unevaluated_predicate = predicate is not None
		self._begin_pos: int = 0
		self._end_pos: Optional[int] = None
		self._len: Optional[int] = None

	def __iter__(self) -> Iterator[AccessAssignment]:
		if self._unevaluated_predicate:
			self._evaluate_predicate()

		length: int = 0

		with open(self._path) as file:
			it = _replay(
				file,
				begin_pos = self._begin_pos,
				end_pos = self._end_pos,
			)

			for assgnm in it:
				length += 1
				yield assgnm

		if self._len is None:
			self._len = length

	def __reversed__(self) -> Iterator[AccessAssignment]:
		if self._unevaluated_predicate:
			self._evaluate_predicate()

		length: int = 0

		with open(self._path) as file:
			it = _reverse_replay(
				file,
				begin_pos = self._begin_pos,
				end_pos = self._end_pos,
			)

			for assgnm in it:
				length += 1
				yield assgnm

		if self._len is None:
			self._len = length

	def __len__(self) -> int:
		if self._len is None:
			if self._unevaluated_predicate:
				self._evaluate_predicate()
			else:
				sum(map(lambda _: 0, iter(self))) # TODO consume(iter(self))

		return cast(int, self._len)

	def scope_to_cache_processor(self, cache_proc: int) -> 'Reader.CacheProcessorScoped':
		return Reader.CacheProcessorScoped(self, cache_proc)

	def _evaluate_predicate(self) -> None:
		p = cast(Predicate, self._predicate)

		if p.style is not Predicate.Style.OneRange:
			raise NotImplementedError(
				f'Only supports Predicate in OneRange Style, has style {p.style}',
			)

		if p.action is not Predicate.Action.YieldOnly:
			raise NotImplementedError(
				f'Only supports Predicate with YieldOnly Action, has action {p.action}',
			)

		# Only evaluates a Predicate of OneRange style with YieldOnly action.

		replay_state: Optional[_ReplayStateAccessor] = None

		def state_cb(state_accessor: _ReplayStateAccessor) -> None:
			nonlocal replay_state
			replay_state = state_accessor

		read_count: int = 0

		def open_and_replay() -> Iterator[AccessAssignment]:
			nonlocal read_count
			with open(self._path) as file:
				for assgnm in _replay(file, state_cb=state_cb):
					read_count += 1
					yield assgnm

		it = open_and_replay()

		begin_pos: int = 0
		end_pos: int = 0
		end: int = 0
		length: int = 0

		def read_state() -> None:
			nonlocal end
			# This is an optimistic cast, but will hold as long as _replay
			# calls state_cb before yielding for the first time.
			end = cast(_ReplayStateAccessor, replay_state).file.tell()

		try:
			taken_count: int = 0
			for _ in p.take_while_not(it):
				taken_count += 1
				read_state()

			# takes last invalid ~> is at the start of first valid
			# -> read_state() ~> end = pos()
			# oracle stops OR takes and checks first valid
			# -> StopIteration

			begin_pos = end

			if read_count > taken_count:
				# take_while_not() over-read: at least one assgnm was not yielded
				length += read_count - taken_count
				read_state()

			for _ in p.take_while(it):
				length += 1
				read_state()

			# takes last valid ~> is at the start of first invalid
			# -> read_state() ~> end = pos()
			# oracle stops OR takes and checks first invalid
			# -> StopIteration

			end_pos = end

		except NotImplementedError:
			for assgnm in it:
				if p(assgnm):
					begin_pos = end
					break

				length += 1
				read_state()

			length += 1
			read_state()

			for assgnm in it:
				if not p(assgnm):
					end_pos = end
					break

				length += 1
				read_state()

		self._begin_pos = begin_pos
		self._end_pos = end_pos
		self._len = length

		self._unevaluated_predicate = False


