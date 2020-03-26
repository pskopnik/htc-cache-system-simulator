from typing import AnyStr, cast, IO, List
from typing_extensions import TypedDict
import random

from .. import BytesRate, BytesSize
from ..jsonparams import load_validate_transform
from ..nodes import (
	ComputingNode,
	DistributionSchedule,
	Node,
	PassiveNode,
	PhysicsProcessingModel,
	ReplaceModel,
	skipping_channel,
)
from ..schemes import NonCorrelatedSchemesGenerator
from ..units import day, GiB, TiB


class Spec(object):
	class NormalDistribution(TypedDict):
		mu: float
		sigma: float


	class Schedule(TypedDict):
		normal_distribution: 'Spec.NormalDistribution'


	class RootParams(TypedDict):
		size: BytesSize
		file_size: BytesSize
		schedule: 'Spec.Schedule'


	class ComputingParams(TypedDict):
		node_spread: int
		read_fraction: float
		job_read_size: BytesSize
		schedule: 'Spec.Schedule'


	class Params(TypedDict):
		root: 'Spec.RootParams'
		computing: 'Spec.ComputingParams'


def load_params(params_file: IO[AnyStr]) -> Spec.Params:
	return cast(Spec.Params, load_validate_transform(
		params_file,
		'pags_single_parameters_schema.json',
		transformations = [
			(('root', 'size'), 'bytes_size'),
			(('root', 'file_size'), 'bytes_size'),
			(('computing', 'job_read_size'), 'bytes_size'),
		],
	))

example_params: Spec.Params = {
	'root': {
		'size': 10 * TiB,
		'file_size': 1 * GiB,
		'schedule': {
			'normal_distribution': {
				'mu': 30 * day,
				'sigma': 15 * day,
			},
		},
	},
	'computing': {
		'node_spread': 7,
		'read_fraction': 0.2,
		'job_read_size': 2 * GiB,
		'schedule': {
			'normal_distribution': {
				'mu': 7 * day,
				'sigma': 2 * day,
			},
		},
	},
}

def build(params: Spec.Params) -> List[Node]:
	nodes: List[Node] = []
	root_layer: List[PassiveNode] = []
	children_layer: List[Node] = []

	root_node = PassiveNode(
		ReplaceModel(
			params['root']['size'],
			DistributionSchedule.from_rand_variate( # schedule
				random.normalvariate, # dist function
				params['root']['schedule']['normal_distribution']['mu'],
				params['root']['schedule']['normal_distribution']['sigma'],
			),
		),
		params['root']['file_size'],
		name = 'pseudo root task',
	)
	root_layer.append(root_node)
	nodes.extend(root_layer)

	for parent in root_layer:
		schemes_generator = NonCorrelatedSchemesGenerator(
			params['computing']['node_spread'],
			params['computing']['read_fraction'],
		)

		for i in range(params['computing']['node_spread']):
			node = ComputingNode(
				skipping_channel( # trigger
					DistributionSchedule.from_rand_variate( # channel
						random.normalvariate, # dist function
						params['computing']['schedule']['normal_distribution']['mu'],
						params['computing']['schedule']['normal_distribution']['sigma'],
					),
				),
				parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					params['computing']['job_read_size'],
					0.0, # output_fraction
					1, # file_size
				),
				name = f'computing task #{i}',
			)
			children_layer.append(node)

	nodes.extend(children_layer)

	return nodes
