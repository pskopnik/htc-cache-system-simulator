import functools
import itertools
import math
from typing import List
import random

from .. import TimeStamp
from ..nodes import delaying_channel, skipping_channel, Node, PassiveNode, ReplaceModel, LinearGrowthModel, DistributionSchedule, ComputingNode, PhysicsProcessingModel
from ..schemes import NonCorrelatedSchemesGenerator
from ..units import MiB, GiB, TiB, day

def build() -> List[Node]:
	nodes: List[Node] = []
	grid_layer: List[PassiveNode] = []
	skim_layer: List[Node] = []
	ana_layer: List[Node] = []

	root_node = PassiveNode(
		LinearGrowthModel(
			10 * TiB, # initial size
			DistributionSchedule.from_rand_variate( # schedule
				random.normalvariate, # dist function
				90 * day, # mu
				15 * day, # sigma
			),
			(100 * TiB - 10 * TiB) / (2 * 365 * day), # growth_rate
			100 * TiB, # max_size
		),
		5 * GiB, # file_size
		name = 'pseudo AOD grid task',
	)
	grid_layer.append(root_node)

	nodes.extend(grid_layer)

	for skim_parent in grid_layer:
		no_of_children = 7
		read_fraction = 0.2

		channels = itertools.tee(skim_parent.update_channel(), no_of_children)
		schemes_generator = NonCorrelatedSchemesGenerator(no_of_children, read_fraction)

		for i, channel in enumerate(channels):
			node = ComputingNode(
				delaying_channel( # trigger
					channel, # channel
					functools.partial( # delay_distribution_dist
						random.lognormvariate, # dist function
						math.log(2 * day), # mu
						1.4, # sigma # TODO detail calculation
					),
				),
				skim_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					2 * GiB, # job_read_size
					0.1, # output_fraction
					100 * MiB, # file_size
				),
				name = f'skimming task #{i}',
			)
			skim_layer.append(node)

	nodes.extend(skim_layer)

	for ana_parent in skim_layer:
		no_of_children = 2
		read_fraction = 0.8

		schemes_generator = NonCorrelatedSchemesGenerator(no_of_children, read_fraction)

		for i in range(no_of_children):
			node = ComputingNode(
				skipping_channel( # trigger
					DistributionSchedule.from_rand_variate( # channel
						random.normalvariate, # dist function
						7 * day, # mu
						2 * day, # sigma
					),
				),
				ana_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					1 * GiB, # job_read_size
					0.05, # output_fraction
					50 * MiB, # file_size
				),
				name = f'analysis task #{i}',
			)
			ana_layer.append(node)

	nodes.extend(ana_layer)

	return nodes
