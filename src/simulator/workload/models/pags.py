import functools
import itertools
import math
from typing import List
from typing_extensions import TypedDict
import random

from .. import BytesRate, BytesSize
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


	class Params(TypedDict):
		aod: 'Spec.AODParams'
		skim: 'Spec.SkimParams'
		ana: 'Spec.AnaParams'



# C_0 is the name of the configuration consisting of these parameter values.
# It is used as the base configuration for the evaluation of the cache policies.
c_0_params: Spec.Params = {
	'aod': {
		'initial_size': 10 * TiB,
		'final_size': 100 * TiB,
		'growth_rate': round((100 * TiB - 10 * TiB) / (2 * 365 * day)),
		'file_size': 5 * GiB,
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
		'job_read_size': 2 * GiB,
		'output_fraction': 0.1,
		'file_size': 100 * MiB,
		'delay_schedule': {
			'lognormal_distribution': {
				'mu': math.log(2 * day),
				'sigma': 1.4, # TODO detail calculation
				# R plot
				# tibble(
				# 	x = seq(1, 60*24*60*60, 60*60),
				# 	dens = dlnorm(x, meanlog=log(2*24*60*60), sdlog=1.4),
				# 	dist = plnorm(x, meanlog=log(2*24*60*60), sdlog=1.4)
				# ) %>%
				# 	ggplot() + geom_line(aes(x=x/24/60/60, y=dist))
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

def build(params: Spec.Params=c_0_params) -> List[Node]:
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

	return nodes
