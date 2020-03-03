import collections
import functools
import math
import pytest # type: ignore[import]
from typing import Dict, List

from simulator.workload.schemes import NonCorrelatedSchemesGenerator

@pytest.mark.parametrize( # type: ignore[misc]
	'file_size,number,fraction',
	(
		(1024*1024*1024, 7, 0.2),
	),
)
def test_non_correlated_schemes_generator(file_size: int, number: int, fraction: float) -> None:
	s = NonCorrelatedSchemesGenerator(number, fraction)
	schemes = [s.parts(i, file_size) for i in range(number)]

	scheme_byte_counts = [sum(part[1] for part in scheme) for scheme in schemes]
	assert scheme_byte_counts == [scheme_byte_counts[0]] * number
	assert scheme_byte_counts[0] / file_size - fraction < 0.0001

	parts_dict: Dict[int, List[int]] = functools.reduce(
		lambda d, part: (d[part[0]].append(part[1]), d)[1], # type: ignore[func-returns-value]
		(part for parts in schemes for part in parts),
		collections.defaultdict(list),
	)

	for index, byte_counts in parts_dict.items():
		exp = byte_counts[0]
		for byte_count in byte_counts:
			assert byte_count == exp

	total_read_fraction = sum(byte_counts[0] for _, byte_counts in parts_dict.items()) / file_size
	assert total_read_fraction - (1 - (1-fraction) ** number) < 0.0001
