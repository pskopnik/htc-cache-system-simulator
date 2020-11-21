import functools
import itertools
import math
from typing import AnyStr, cast, IO, Iterable, Iterator, List, Optional
from typing_extensions import TypedDict
from random import Random

from .. import AccessRequest, BytesRate, BytesSize, Job, Submitter, TimeStamp
from ..jsonparams import load_validate_transform
from ..nodes import (
	ComputingNode,
	delaying_channel,
	DistributionSchedule,
	LinearGrowthModel,
	Node,
	PassiveNode,
	PhysicsProcessingModel,
	skipping_channel,
)
from ..schemes import NonCorrelatedSchemesGenerator
from ..units import MiB, GiB, TiB, day
from ...utils import repeat_each


class SimpleNoiseNode(Node):
	class _Submitter(Submitter):
		def __init__(self, node: 'SimpleNoiseNode') -> None:
			super(SimpleNoiseNode._Submitter, self).__init__(0, origin=node)

			self._node: SimpleNoiseNode = node

		def _files_generator(self) -> Iterator[str]:
			base = f'{id(self)}/'

			dir_it: Iterable[str] = itertools.repeat('')
			if self._node._files_per_directory > 0:
				dir_it = repeat_each(
					(f'{i}/' for i in itertools.count()),
					self._node._files_per_directory,
				)

			return (
				f'{base}{dir}{i}' for i, dir in enumerate(dir_it)
			)

		def __iter__(self) -> Iterator[Job]:
			submit_rate = self._node._submit_rate
			read_size = self._node._file_size
			parts = [(0, read_size)]

			total_submitted: int = 0
			ts: TimeStamp = 0
			for file in self._files_generator():
				yield Job(ts, [AccessRequest(file, parts)])

				total_submitted += read_size # TODO is this safe (ever growing)?
				ts = math.ceil(total_submitted / submit_rate)


	def __init__(
		self,
		file_size: BytesSize,
		files_per_directory: int,
		submit_rate: BytesRate,
		name: Optional[str] = None,
	) -> None:
		super(SimpleNoiseNode, self).__init__(name=name)

		self._file_size: BytesSize = file_size
		self._files_per_directory: int = files_per_directory
		self._submit_rate: BytesRate = submit_rate

	def __iter__(self) -> Iterator[Submitter]:
		yield SimpleNoiseNode._Submitter(self)


class Spec(object):
	class NormalDistribution(TypedDict):
		mu: float
		sigma: float


	class Schedule(TypedDict):
		normal_distribution: 'Spec.NormalDistribution'


	class LognormalDistribution(TypedDict):
		mu: float
		sigma: float


	class DelaySchedule(TypedDict):
		lognormal_distribution: 'Spec.LognormalDistribution'


	class AODParams(TypedDict):
		initial_size: BytesSize
		final_size: BytesSize
		growth_rate: BytesRate
		file_size: BytesSize
		files_per_directory: int
		schedule: 'Spec.Schedule'


	class SkimParams(TypedDict):
		node_spread: int
		read_fraction: float
		job_read_size: BytesSize
		output_fraction: float
		file_size: BytesSize
		delay_schedule: 'Spec.DelaySchedule'


	class AnaParams(TypedDict):
		node_spread: int
		read_fraction: float
		job_read_size: BytesSize
		output_fraction: float
		file_size: BytesSize
		schedule: 'Spec.Schedule'


	class SimpleNoiseParams(TypedDict):
		file_size: BytesSize
		files_per_directory: int
		submit_rate: BytesRate


	class RequiredParams(TypedDict):
		aod: 'Spec.AODParams'
		skim: 'Spec.SkimParams'
		ana: 'Spec.AnaParams'


	class Params(RequiredParams, total=False):
		simple_noise: 'Spec.SimpleNoiseParams'


def load_params(params_file: IO[AnyStr]) -> Spec.Params:
	return cast(Spec.Params, load_validate_transform(
		params_file,
		'pags_parameters_schema.json',
		transformations = [
			(('aod', 'initial_size'), 'bytes_size'),
			(('aod', 'final_size'), 'bytes_size'),
			(('aod', 'growth_rate'), 'bytes_rate'),
			(('aod', 'file_size'), 'bytes_size'),
			(('skim', 'job_read_size'), 'bytes_size'),
			(('skim', 'file_size'), 'bytes_size'),
			(('ana', 'job_read_size'), 'bytes_size'),
			(('ana', 'file_size'), 'bytes_size'),
			(('simple_noise', 'file_size'), 'bytes_size'),
			(('simple_noise', 'submit_rate'), 'bytes_rate'),
		],
	))

# C_0 is the name of the configuration consisting of these parameter values.
# It is used as the base configuration for the evaluation of the cache policies.
c_0_params: Spec.Params = {
	'aod': {
		'initial_size': 10 * TiB,
		'final_size': 100 * TiB,
		'growth_rate': round((100 * TiB - 10 * TiB) / (2 * 365 * day)),
		'file_size': 5 * GiB,
		'files_per_directory': 1000,
		'schedule': {
			'normal_distribution': {
				'mu': 90 * day,
				'sigma': 15 * day,
			},
		},
	},
	'skim': {
		'node_spread': 7,
		'read_fraction': 0.2,
		'job_read_size': 10 * GiB,
		'output_fraction': 0.01,
		'file_size': 100 * MiB,
		'delay_schedule': {
			'lognormal_distribution': {
				'mu': math.log(2 * day),
				'sigma': 1.4, # Chosen to give reasonable percentiles
			},
		},
	},
	'ana': {
		'node_spread': 2,
		'read_fraction': 0.8,
		'job_read_size': 1 * GiB,
		'output_fraction': 0.05,
		'file_size': 50 * MiB,
		'schedule': {
			'normal_distribution': {
				'mu': 7 * day,
				'sigma': 2 * day,
			},
		},
	},
}

def build(params: Spec.Params, seed: Optional[int]=None) -> List[Node]:
	random = Random(seed)

	nodes: List[Node] = []
	grid_layer: List[PassiveNode] = []
	skim_layer: List[Node] = []
	ana_layer: List[Node] = []

	root_node = PassiveNode(
		LinearGrowthModel(
			params['aod']['initial_size'],
			DistributionSchedule.from_rand_variate( # schedule
				random.normalvariate, # dist function
				params['aod']['schedule']['normal_distribution']['mu'],
				params['aod']['schedule']['normal_distribution']['sigma'],
			),
			params['aod']['growth_rate'],
			params['aod']['final_size'],
		),
		params['aod']['file_size'],
		params['aod']['files_per_directory'],
		name = 'pseudo AOD grid task',
	)
	grid_layer.append(root_node)

	nodes.extend(grid_layer)

	for skim_parent in grid_layer:
		channels = itertools.tee(skim_parent.update_channel(), params['skim']['node_spread'])
		schemes_generator = NonCorrelatedSchemesGenerator(
			params['skim']['node_spread'],
			params['skim']['read_fraction'],
		)

		for i, channel in enumerate(channels):
			node = ComputingNode(
				delaying_channel( # trigger
					channel, # channel
					functools.partial( # delay_distribution_dist
						random.lognormvariate, # dist function
						params['skim']['delay_schedule']['lognormal_distribution']['mu'],
						params['skim']['delay_schedule']['lognormal_distribution']['sigma'],
					),
				),
				skim_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					params['skim']['job_read_size'],
					params['skim']['output_fraction'],
					params['skim']['file_size'],
				),
				name = f'skimming task #{i}',
			)
			skim_layer.append(node)

	nodes.extend(skim_layer)

	for ana_parent in skim_layer:
		schemes_generator = NonCorrelatedSchemesGenerator(
			params['ana']['node_spread'],
			params['ana']['read_fraction'],
		)

		for i in range(params['ana']['node_spread']):
			node = ComputingNode(
				skipping_channel( # trigger
					DistributionSchedule.from_rand_variate( # channel
						random.normalvariate, # dist function
						params['ana']['schedule']['normal_distribution']['mu'],
						params['ana']['schedule']['normal_distribution']['sigma'],
					),
				),
				ana_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					params['ana']['job_read_size'],
					params['ana']['output_fraction'],
					params['ana']['file_size'],
				),
				name = f'analysis task #{i}',
			)
			ana_layer.append(node)

	nodes.extend(ana_layer)

	if 'simple_noise' in params:
		simple_noise_params = params['simple_noise']
		if simple_noise_params['submit_rate'] > 0.0 and simple_noise_params['file_size'] > 0:
			simple_noise_node = SimpleNoiseNode(
				simple_noise_params['file_size'],
				simple_noise_params['files_per_directory'],
				simple_noise_params['submit_rate'],
			)
			nodes.append(simple_noise_node)

	return nodes
