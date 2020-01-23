from typing import List
from . import AccessScheme, FileID, PartSpec, PartsGenerator


class NonCorrelatedSchemesGenerator(object):
	def __init__(self, number: int):
		self.number = number
		self._parts_number = 2 ** number

	def parts(self, index: int, total_bytes: int) -> List[PartSpec]:
		scheme_parts_number = 2 ** (self.number - 1)

		min_part_bytes = total_bytes // scheme_parts_number
		additional_bytes = total_bytes % scheme_parts_number

		parts: List[PartSpec] = []

		for i in range(scheme_parts_number):

			# Insert 1 bit at index into binary representation of i
			part_index = (((i << 1 >> index) | 1) << index) | (i & ((1 << index) - 1))

			part_bytes = min_part_bytes
			if i < additional_bytes:
				part_bytes += 1

			parts.append((part_index, part_bytes))

		return parts

	def access_scheme(self, index: int, file: FileID, total_bytes: int) -> AccessScheme:
		return AccessScheme(file, self.parts(index, total_bytes))

	class WithIndex(PartsGenerator):
		def __init__(self, generator: 'NonCorrelatedSchemesGenerator', index: int):
			self._generator: NonCorrelatedSchemesGenerator = generator
			self._index: int = index

		def parts(self, total_bytes: int) -> List[PartSpec]:
			return self._generator.parts(self._index, total_bytes)

		def access_scheme(self, file: FileID, total_bytes: int) -> AccessScheme:
			return self._generator.access_scheme(self._index, file, total_bytes)

	def with_index(self, index: int) -> WithIndex:
		return self.WithIndex(self, index)
