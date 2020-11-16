from pathlib import PurePosixPath
from typing import Callable, cast, Hashable, Iterable, List, Optional
from typing_extensions import Protocol

from ..workload import Access

ClassID = str


class Classifier(Protocol):
	def __call__(self, access: Access) -> Hashable:
		pass


class Combine(object):
	def __init__(self, classifiers: Iterable[Classifier]) -> None:
		self._classifiers: List[Classifier] = list(classifiers)

	def __call__(self, access: Access) -> Hashable:
		return tuple(map(lambda classifier: classifier(access), self._classifiers))


class Constant(object):
	def __init__(self, const: str) -> None:
		self._const = const

	@property
	def const(self) -> str:
		return self._const

	def __call__(self, access: Access) -> Hashable:
		return self._const


class DirectoryName(object):
	def __init__(self, from_file: Optional[int]=-1, from_root: Optional[int]=-1) -> None:
		if from_file != -1 and from_root != -1:
			raise ValueError('Either from_file or from_root must equal -1, i.e. be disabled')
		if from_file == -1 and from_root == -1:
			raise ValueError('Either from_file or from_root must not equal -1, i.e. be enabled')

		def class_from_file(access: Access) -> str:
			return str(PurePosixPath(access.file).parents[cast(int, from_file)])

		def class_from_root(access: Access) -> str:
			p = PurePosixPath(access.file)
			# Deduct 2: Offset length vs. index and ignore pathlib root ('/' or '.')
			return str(p.parents[len(p.parts) - 2 - cast(int, from_root)])

		self._class_fn: Callable[[Access], str]

		if from_file != -1:
			self._class_fn = class_from_file
		else:
			self._class_fn = class_from_root

	def __call__(self, access: Access) -> str:
		return self._class_fn(access)
