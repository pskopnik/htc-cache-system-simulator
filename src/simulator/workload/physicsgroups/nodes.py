import abc
import itertools
import functools
import random
from typing import Any, Callable, cast, Iterable, Iterator, Generic, Optional, Tuple, TypeVar
from typing_extensions import Protocol

from .. import PartsGenerator, Submitter, TimeStamp
from ..dataset import DataSet, DataSetSubmitter
from ..utils import ignore_args, CallbackWrapSubmitter, NoneSubmitter


# TODO
# General design question:
# Is a class a spec container (factory capable of creating many instances
# adhering to the spec) and thus to be used by all "similar" nodes or is an
# instance a node-specific state container?

Distribution = Callable[[], float]

zero_dist: Distribution = lambda: 0.0

def const_dist(c: float) -> Distribution:
	return lambda: c

EventChannel = Iterable[TimeStamp]

def delaying_channel(
	channel: EventChannel,
	delay_duration_dist: Distribution = zero_dist,
) -> EventChannel:
	return (ts + int(delay_duration_dist()) for ts in channel)

def skipping_channel(
	channel: EventChannel,
	skip_count_dist: Distribution = zero_dist,
) -> EventChannel:
	skip = int(skip_count_dist())
	for ts in channel:
		if skip > 0:
			skip -= 1
			continue

		skip = int(skip_count_dist())
		yield ts

# TODO should be named something more general than GrowthModel
GrowthModel = Iterable[Tuple[TimeStamp, int]]

SubmitterScaffold = Callable[[TimeStamp], Submitter]


class ProcessingModel(Protocol):
	"""Protocol (interface) for processing models.

	A processing model is a scaffold of a scaffold (a factory, i.e. an
	instance constructor, which has some properties already given) for
	Submitter instances.
	"""

	def __call__(
		self,
		input_data_set: DataSet,
		output_data_set: DataSet,
		origin: Optional[Any] = None,
	) -> Callable[[TimeStamp], Submitter]:
		...


class Node(abc.ABC):
	_data_set: DataSet
	_name: Optional[str]

	def __init__(self, data_set: Optional[DataSet]=None, name: Optional[str]=None) -> None:
		self._data_set: DataSet = data_set if data_set is not None else DataSet()
		self._name: Optional[str] = name

	@property
	def data_set(self) -> DataSet:
		return self._data_set

	def __iter__(self) -> Iterator[Submitter]:
		raise NotImplementedError

	def __str__(self) -> str:
		identifier_str = repr(self._name) if self._name is not None else f'at 0x{id(self):x}'
		return f'{self.__class__.__name__} {identifier_str}'

	def __repr__(self) -> str:
		name_str = f'name={self._name!r}' if self._name is not None else ''
		return (
			f'{self.__class__.__name__}({name_str})'
		)



class DistributionSchedule(object):
	def __init__(self, dist: Distribution, yield_zero: bool=True):
		self._dist = dist
		self._yield_zero: bool = yield_zero

	def __iter__(self) -> Iterator[TimeStamp]:
		# PY3.8 accumulate() has keyword arg initial which can be used instead of iterator chaining
		# initial = 0 if self._yield_zero else None

		initial_it = [0] if self._yield_zero else []

		return itertools.chain(
			initial_it,
			itertools.accumulate(map(int, map(ignore_args(self._dist), itertools.repeat(None)))),
		)

	@classmethod
	def from_rand_variate(
		cls,
		f: Any,
		*args: Any,
		yield_zero: bool = True,
		**kwargs: Any,
	) -> 'DistributionSchedule':
		return cls(functools.partial(f, *args, **kwargs), yield_zero=yield_zero)


class LinearGrowthModel(object):
	def __init__(
		self,
		initial_size: int,
		channel: EventChannel,
		growth_rate: float,
		max_size: int=0,
	) -> None:
		self._initial_size: int = initial_size
		self._schedule: EventChannel = channel
		self._growth_rate: float = growth_rate
		self._max_size: int = max_size

	def __iter__(self) -> Iterator[Tuple[TimeStamp, int]]:
		size = self._initial_size
		prev_ts = 0

		for ts in self._schedule:
			size += int(self._growth_rate * (ts - prev_ts))

			if self._max_size > 0 and size > self._max_size:
				size = self._max_size

			yield ts, size


class PassiveNode(Node):
	def __init__(self, growth_model: GrowthModel, output_file_size: int, name: Optional[str]=None):
		super(PassiveNode, self).__init__(name=name)

		self._data_set.reinitialise(file_size=output_file_size, name=f'output of {self!s}')
		self._growth_model: GrowthModel = growth_model

		self._iter_growth_model, self._update_growth_model = itertools.tee(self._growth_model, 2)

	def update_channel(self) -> EventChannel:
		"""Constructs and returns a channel describing the node's updates.

		Must only be called once, the returned channel can be teed.
		"""
		return (ts for ts, _ in self._update_growth_model)

	def _crt_grow_data_set_cb(self, total_size: int) -> Callable[[], None]:
		def inner() -> None:
			if self._data_set.size < total_size:
				# TODO is shrinking necessary?
				self._data_set.replace(size=total_size)
			else:
				self._data_set.grow(self._data_set.size - total_size)

		return inner

	def __iter__(self) -> Iterator[Submitter]:
		for ts, size in self._iter_growth_model:
			yield CallbackWrapSubmitter(
				NoneSubmitter(ts, origin=self),
				post_cb = self._crt_grow_data_set_cb(size),
			)


class PhysicsProcessingModel(object):
	def __init__(
		self,
		parts_generator: PartsGenerator,
		job_read_size: int,
		output_fraction: float,
		output_file_size: int,
	):
		self.parts_generator: PartsGenerator = parts_generator # parts generator is specific to a single node in this simulation
		self.job_read_size: int = job_read_size
		self.output_fraction: float = output_fraction
		self.output_file_size: int = output_file_size

	def __call__(
		self,
		input_data_set: DataSet,
		output_data_set: DataSet,
		origin: Optional[Any] = None,
	) -> SubmitterScaffold:
		output_data_set.reinitialise(file_size=self.output_file_size)

		submitter_scaffold = DataSetSubmitter.scaffold(
			input_data_set,
			self.parts_generator,
			self.job_read_size,
			origin = origin,
		)

		def update_data_set() -> None:
			output_size = int(input_data_set.size * self.output_fraction)
			output_data_set.replace(size=output_size)

		def inner(ts: TimeStamp) -> Submitter:
			return CallbackWrapSubmitter(
				submitter_scaffold(ts),
				post_cb = update_data_set,
			)

		return inner


class ComputingNode(Node):
	def __init__(
		self,
		trigger: EventChannel,
		input_data_set: DataSet,
		processing_model: ProcessingModel,
		name: Optional[str] = None,
	) -> None:
		super(ComputingNode, self).__init__(name=name)

		self._data_set.reinitialise(name=f'output of {self!s}')

		# input_data_set must have been properly initialised
		# I.e. file_size > 0. Otherwise processing_model() lacks information.

		self._trigger: EventChannel = trigger
		self._input_data_set: DataSet = input_data_set
		self._processing_model: ProcessingModel = processing_model

		self._submitter_scaffold: SubmitterScaffold = processing_model(
			input_data_set,
			self._data_set,
			origin = self,
		)

	def __iter__(self) -> Iterator[Submitter]:
		for ts in self._trigger:
			yield self._submitter_scaffold(ts)
