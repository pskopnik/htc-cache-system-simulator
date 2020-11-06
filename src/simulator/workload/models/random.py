import itertools
import math
from typing import AnyStr, cast, IO, Iterator, List, Optional, Tuple
from typing_extensions import TypedDict
from random import Random

from .. import (
	AccessRequest,
	BytesSize,
	FileID,
	Job,
	PartsGenerator,
	PartSpec,
	Submitter,
	TimeStamp,
)
from ..nodes import Node
from ..dataset import DataSet
from ..jsonparams import load_validate_transform
from ..schemes import NonCorrelatedSchemesGenerator
from ..units import MiB, GiB, TiB

BytesRate = int


class RandomNode(Node):
	class _Submitter(Submitter):
		def __init__(self, node: 'RandomNode') -> None:
			super(RandomNode._Submitter, self).__init__(0, origin=node)

			self._node: RandomNode = node

		def __iter__(self) -> Iterator[Job]:
			data_set = self._node._input_data_set
			submit_rate = self._node._submit_rate

			# cached_file_size = data_set.file_size
			# cached_parts = self._node._parts_generator.parts(cached_file_size)
			# cached_bytes_accessed = sum(byte_count for _, byte_count in cached_parts)

			def create_access_scheme(file: FileID) -> Tuple[AccessRequest, BytesSize]:
				# nonlocal data_set, cached_file_size, cached_bytes_accessed, cached_parts

				parts = self._node._random.choice(self._node._schemes)
				return AccessRequest(file, parts), sum(byte_count for _, byte_count in parts)
				# if data_set.file_size != cached_file_size:
				# 	cached_file_size = data_set.file_size
				# 	cached_parts = self._node._parts_generator.parts(cached_file_size)
				# 	cached_bytes_accessed = sum(byte_count for _, byte_count in cached_parts)
				# return AccessRequest(file, cached_parts), cached_bytes_accessed

			total_submitted: int = 0
			ts: TimeStamp = 0
			for _ in itertools.repeat(None):
				file = self._node._random.choice(data_set.file_list)

				access_scheme, bytes_submitted = create_access_scheme(file)

				yield Job(ts, [access_scheme])

				total_submitted += bytes_submitted # TODO is this safe (ever growing)?
				ts = math.ceil(total_submitted / submit_rate)


	def __init__(
		self,
		input_data_set: DataSet,
		submit_rate: float,
		schemes: List[List[PartSpec]],
		# parts_generator: PartsGenerator,
		random: Random,
		name: Optional[str] = None,
	) -> None:
		super(RandomNode, self).__init__(name=name)

		self._input_data_set: DataSet = input_data_set
		self._submit_rate: float = submit_rate
		self._schemes: List[List[PartSpec]] = schemes
		#self._parts_generator: PartsGenerator = parts_generator
		self._random: Random = random

	def __iter__(self) -> Iterator[Submitter]:
		yield RandomNode._Submitter(self)


class Spec(object):
	class DataSet(TypedDict):
		size: BytesSize
		file_size: BytesSize


	class Params(TypedDict):
		data_set: 'Spec.DataSet'
		no_of_tasks: int
		read_fraction: float
		submit_rate: BytesRate


def load_params(params_file: IO[AnyStr]) -> Spec.Params:
	return cast(Spec.Params, load_validate_transform(
		params_file,
		'random_parameters_schema.json',
		transformations = [
			(('data_set', 'size'), 'bytes_size'),
			(('data_set', 'file_size'), 'bytes_size'),
			(('submit_rate',), 'bytes_rate'),
		],
	))

example_params: Spec.Params = {
	'data_set': {
		'size': 1 * TiB,
		'file_size': 1 * GiB,
	},
	'no_of_tasks': 7,
	'read_fraction': 0.2,
	'submit_rate': 1 * MiB,
}

def build(params: Spec.Params, seed: Optional[int]=None) -> List[Node]:
	random = Random(seed)

	nodes: List[Node] = []

	data_set = DataSet(
		file_size = params['data_set']['file_size'],
		size = params['data_set']['size'],
		name = 'input',
	)

	schemes_generator = NonCorrelatedSchemesGenerator(
		params['no_of_tasks'],
		params['read_fraction'],
	)

	# for i in range(params['no_of_tasks']):
	node = RandomNode(
		data_set, # input_data_set
		params['submit_rate'], # submit_rate
		[schemes_generator.parts(i, data_set.file_size) for i in range(params['no_of_tasks'])],
		# schemes_generator.with_index(i), # parts_generator
		random,
		name = f'computing task',
		# name = f'computing task #{i}',
	)
	nodes.append(node)

	return nodes
