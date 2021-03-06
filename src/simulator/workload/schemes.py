from typing import List

from . import AccessRequest, FileID, PartSpec, PartsGenerator


class NonCorrelatedSchemesGenerator(object):
	def __init__(self, number: int, fraction: float) -> None:
		self._number: int = number
		self._fraction: float = fraction
		self._parts_number: int = 2 ** number

	@property
	def number(self) -> int:
		return self._number

	@property
	def fraction(self) -> float:
		return self._fraction

	def parts(self, index: int, total_bytes: int) -> List[PartSpec]:
		scheme_parts_number = 2 ** (self._number - 1)

		parts: List[PartSpec] = []

		for i in range(scheme_parts_number):

			# Insert 1 bit at index into binary representation of i
			part_index = (((i << 1 >> index) | 1) << index) | (i & ((1 << index) - 1))

			containing_schemes = bin(part_index).count('1')
			part_bytes = round(total_bytes * (
					self._fraction ** containing_schemes
						*
					(1 - self._fraction) ** (self._number - containing_schemes)
			))

			parts.append((part_index, part_bytes))

		return parts

	def access_request(self, index: int, file: FileID, total_bytes: int) -> AccessRequest:
		return AccessRequest(file, self.parts(index, total_bytes))

	class WithIndex(PartsGenerator):
		def __init__(self, generator: 'NonCorrelatedSchemesGenerator', index: int) -> None:
			self._generator: NonCorrelatedSchemesGenerator = generator
			self._index: int = index

		def parts(self, total_bytes: int) -> List[PartSpec]:
			return self._generator.parts(self._index, total_bytes)

		def access_request(self, file: FileID, total_bytes: int) -> AccessRequest:
			return self._generator.access_request(self._index, file, total_bytes)

	def with_index(self, index: int) -> WithIndex:
		return self.WithIndex(self, index)
