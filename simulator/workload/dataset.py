from typing import Any, Iterable, Iterator, List, Optional
import math
from . import AccessScheme, FileID, Job, PartSpec, PartsGenerator, Submitter, TimeStamp
from itertools import repeat
from .utils import repeat_each


class DataSet(object):
	"""DataSet represents a conceptual data set of equisized files.

	A DataSet instance is persistent, it describes a conceptual grouping of
	files although all files contained may change over time.

	DataSet is a concise representation made up of the list of files (as
	path-like file ids) and the size per files, file_size. The file_size and
	size-like parameters are expressed in bytes or another suitable unit.

	The size attribute describes the current total size of the data set, that
	is len(file_list) * file_size. That means size-like parameters generally
	end up rounded up to the next multiple of file_size.

	The files_per_directory attribute describes how many files are located in
	a directory. If files_per_directory is greater than zero, the id space is
	split into uniquely named directories:

    <numeric data set id>/<numeric directory id>/<numeric file id>

    If files_per_directory is 0 the sub-directory is omitted:

    <numeric data set id>/<numeric file id>

	The generation parameter gives an indication of the 'version' of the data
	set. All operations modifying the file list increment the generation.

	The constructor initialises the list of files if size is greater than 0.
	"""

	def __init__(self, file_size: int=0, files_per_directory: int=0, name: Optional[str]=None, size: int=0) -> None:
		self._file_size: int = file_size
		self._files_per_directory: int = files_per_directory
		self._name: Optional[str] = name

		self._file_list: List[FileID] = []
		self._next_id: int = 0
		self._next_dir_id: int = 0
		self._generation: int = 0

		if size > 0:
			self.grow(size)

	@property
	def file_size(self) -> int:
		return self._file_size

	@property
	def files_per_directory(self) -> int:
		return self._files_per_directory

	@property
	def name(self) -> Optional[str]:
		return self._name

	@property
	def generation(self) -> int:
		return self._generation

	@property
	def file_list(self) -> List[FileID]:
		return self._file_list

	@property
	def size(self) -> int:
		return len(self._file_list) * self._file_size

	def __str__(self) -> str:
		identifier_str = repr(self._name) if self._name is not None else f'at 0x{id(self):x}'
		return f'DataSet {identifier_str} @ {self._generation}g'

	def __repr__(self) -> str:
		name_str = f', name={self._name!r}' if self._name is not None else ''
		return (
			f'DataSet(file_size={self._file_size!r}, ' +
			f'files_per_directory={self._files_per_directory!r}{name_str}, ' +
			f'_internals=<generation={self._generation!r}, ' +
			f'num_files={len(self._file_list)!r}, size={self.size!r}, addr={id(self)!r}>)'
		)

	def reinitialise(
		self,
		file_size: Optional[int] = None,
		files_per_directory: Optional[int] = None,
		name: Optional[str] = None,
		size: int = 0,
	) -> None:
		"""Reinitialises selected data set parameters.

		The new values of file_size, files_per_directory and name are taken on
		if not None and the file_list is cleared. If size is greater than 0, a
		list of files is generated. generation is not reset.

		This method is useful for late and staggered initialisation.
		"""
		if file_size is not None:
			self._file_size = file_size
		if files_per_directory is not None:
			self._files_per_directory = files_per_directory
		if name is not None:
			self._name = name

		self._file_list.clear()

		if size > 0:
			self.grow(size)

	def replace(self, size: Optional[int]=None) -> None:
		"""Replaces all files in the data set.

		May result in the total size being up to file_size larger than size
		(intuition: file_size is much smaller than size).

		Leads to an increment of generation.
		"""
		size = size if size is not None else self.size
		self._file_list.clear()
		self.grow(size)

	def grow(self, extent: int) -> None:
		"""Grows the data set by adding new files.

		extent defines the total number of bytes to be added.

		May grow up to file_size over extent (intuition: file_size is much
		smaller than extent).

		Leads to an increment of generation.
		"""
		self._generation += 1
		self._file_list.extend(self._gen_files(extent))

	def _gen_files(self, total_size: int) -> Iterable[FileID]:
		"""Generates unique files conforming of totalling total_size.

		The generated file IDs are globally unique within the program as long
		as the DataSet instance remains alive (in memory). The
		files_per_directory attribute is also respected.
		"""
		if self._file_size <= 0:
			if total_size > 0:
				raise Exception(
					f'Attempting to generate file list of total size {total_size} ' +
					f'(greater zero) with file_size=0'
				)
			else:
				return []

		# the file iterable has ids [first_id, end_id)
		first_id = self._next_id
		end_id = first_id + math.ceil(total_size / self._file_size)
		self._next_id = end_id

		dir_it: Iterable[str] = repeat('')

		if self._files_per_directory > 0:
			# the directory iterable has ids [first_dir_id, end_dir_id)
			first_dir_id = self._next_dir_id
			end_dir_id = first_dir_id + math.ceil(end_id - first_id)
			self._next_dir_id = end_dir_id

			dir_it = repeat_each(
				(f'{i}/' for i in range(first_dir_id, end_dir_id)),
				self._files_per_directory,
			)

		base = f'{id(self)}/'

		return (
			f'{base}{dir}{i}' for dir, i in zip(dir_it, range(first_id, end_id))
		)


class DataSetSubmitter(Submitter):
	class Scaffold(object):
		def __init__(
			self,
			data_set: DataSet,
			parts: List[PartSpec],
			files_per_job: int,
			origin: Optional[Any] = None
		):
			self._data_set = data_set
			self._parts = parts
			self._files_per_job = files_per_job
			self._origin = origin

		def __call__(self, start_ts: TimeStamp, origin: Optional[Any]=None) -> 'DataSetSubmitter':
			if origin is None:
				origin = self._origin
			return DataSetSubmitter(
				start_ts,
				self._data_set,
				self._parts,
				self._files_per_job,
				origin = origin,
			)

	def __init__(
		self,
		start_ts: TimeStamp,
		data_set: DataSet,
		parts: List[PartSpec],
		files_per_job: int,
		origin: Optional[Any] = None,
	) -> None:
		super(DataSetSubmitter, self).__init__(start_ts, origin=origin)
		self._data_set: DataSet = data_set
		self._parts: List[PartSpec] = parts
		self._files_per_job: int = files_per_job

	def __iter__(self) -> Iterator[Job]:
		access_schemes: List[AccessScheme] = []

		for file in self._data_set.file_list:
			access_schemes.append(AccessScheme(file, self._parts))
			if len(access_schemes) == self._files_per_job:
				yield Job(None, access_schemes.copy())
				access_schemes.clear()

		if len(access_schemes) > 0:
			yield Job(None, access_schemes.copy())

	@classmethod
	def scaffold(
		cls,
		data_set: DataSet,
		read_fraction: float,
		parts_gen: PartsGenerator,
		job_read_size: int,
		origin: Optional[Any] = None,
	) -> Scaffold:
		file_size_read = math.floor(data_set.file_size * read_fraction)
		# Each job reads a maximum of job_read_size bytes
		files_per_job = job_read_size // file_size_read # implicit floor
		if files_per_job < 1:
			raise Exception(
				f'Attempting to scaffold a DataSetSubmitter which would read not a single file ' +
				f'per job, job_read_size={job_read_size}, file_size_read={file_size_read}'
			)
		parts = parts_gen.parts(file_size_read)
		return cls.Scaffold(data_set, parts, files_per_job, origin=origin)


# def example():
# 	schemes_generator = NonCorrelatedSchemesGenerator(number)
# 	data_set = DataSet(2 ** (10 * 4), 10 * 2 ** (10 * 2))

# 	parts_gen = schemes_generator.with_index(index)
# 	submitter_scaffold = DataSetSubmitter.scaffold(data_set, fraction, parts_gen, job_size)

# 	for i, el in enumerate(something):
# 		ts = 34 * i
# 		yield submitter_scaffold(ts)
