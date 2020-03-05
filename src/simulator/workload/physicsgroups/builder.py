from .nodes import delaying_channel, skipping_channel, Node, PassiveNode, LinearGrowthModel, DistributionSchedule, ComputingNode, PhysicsProcessingModel
from .. import TimeStamp
from ..schemes import NonCorrelatedSchemesGenerator
from typing import List
import math
import random
import itertools
import functools

class LayerSpec(object):
	spread: int
	# correlation_scheme: CorrelationScheme = from_name("none")


class Builder(object):
	pass


KB: int = 2 ** (10 * 1)
MB: int = 2 ** (10 * 2)
GB: int = 2 ** (10 * 3)
TB: int = 2 ** (10 * 4)
PB: int = 2 ** (10 * 5)

Minute: TimeStamp = 60
Hour: TimeStamp = 60 * 60
Day: TimeStamp = 24 * 60 * 60

def build_physics_groups() -> List[Node]:
	nodes: List[Node] = []
	grid_layer: List[PassiveNode] = []
	skim_layer: List[Node] = []
	calib_layer: List[Node] = []

	root_node = PassiveNode(
		LinearGrowthModel(
			10 * TB, # initial size
			DistributionSchedule.from_rand_variate( # schedule
				random.normalvariate, # dist function
				90 * Day, # mu
				15 * Day, # sigma
			),
			(100 * TB - 10 * TB) / (2 * 365 * Day), # growth_rate
			100 * TB, # max_size
		),
		5 * GB, # output_file_size
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
						math.log(2 * Day), # mu
						1.4, # sigma # TODO detail calculation
					),
				),
				skim_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					2 * GB, # job_read_size
					0.1, # output_fraction
					100 * MB, # output_file_size
				),
				name = f'skim task #{i}',
			)
			skim_layer.append(node)

	nodes.extend(skim_layer)

	for calib_parent in skim_layer:
		no_of_children = 2
		read_fraction = 0.8

		schemes_generator = NonCorrelatedSchemesGenerator(no_of_children, read_fraction)

		for i in range(no_of_children):
			node = ComputingNode(
				skipping_channel( # trigger
					DistributionSchedule.from_rand_variate( # channel
						random.normalvariate, # dist function
						7 * Day, # mu
						2 * Day, # sigma
					),
				),
				calib_parent.data_set, # input_data_set
				PhysicsProcessingModel( # processing_model
					schemes_generator.with_index(i), # parts_generator
					1 * GB, # job_read_size
					0.05, # output_fraction
					50 * MB, # output_file_size
				),
				name = f'calib task #{i}',
			)
			calib_layer.append(node)

	nodes.extend(calib_layer)

	return nodes
