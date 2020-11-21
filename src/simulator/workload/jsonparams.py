import json
import jsonschema
import pkg_resources
import re
from typing import Any, AnyStr, cast, Iterable, IO, Tuple

from . import BytesRate, BytesSize
from .units import bytes_size_units

FieldPath = Iterable[str]
FieldType = str
TransformFieldSpec = Tuple[FieldPath, FieldType]

_bytes_rate_re = re.compile('^(?P<number>\\d+(\\.\\d+)?) ((?P<prefix>[KMGTPEZY])?i)?B/s$')
_bytes_size_re = re.compile('^(?P<number>\\d+(\\.\\d+)?) ((?P<prefix>[KMGTPEZY])?i)?B$')

def parse_bytes_rate(s: str) -> BytesRate:
	match = _bytes_rate_re.fullmatch(s)
	if match is None:
		raise ValueError(f'Invalid bytes rate expression {s!r}')
	prefix = match.group('prefix') if match.group('prefix') is not None else ''
	return round(float(match.group('number')) * bytes_size_units[prefix + 'iB'])

def parse_bytes_size(s: str) -> BytesSize:
	match = _bytes_size_re.fullmatch(s)
	if match is None:
		raise ValueError(f'Invalid bytes size expression {s!r}')
	prefix = match.group('prefix') if match.group('prefix') is not None else ''
	return round(float(match.group('number')) * bytes_size_units[prefix + 'iB'])

_transformers = {
	'bytes_size': parse_bytes_size,
	'bytes_rate': parse_bytes_rate,
}

# Build the complete import path for the sub-package 'models'
_models_package = '.'.join(__name__.split('.')[:-1] + ['models'])

def load_validate_transform(
	params_file: IO[AnyStr],
	schema_file_name: str,
	transformations: Iterable[TransformFieldSpec] = [],
	schema_package: str = _models_package,
) -> Any:
	with pkg_resources.resource_stream(schema_package, schema_file_name) as schema_file:
		schema = json.load(schema_file)

	instance = json.load(params_file)

	jsonschema.validate(instance, schema)

	for path, typ in transformations:
		el = instance
		path_list = list(path)

		try:
			for item_name in path_list[:-1]:
				el = el[item_name]

			orig_value = el[path_list[-1]]
		except KeyError:
			continue

		el[path_list[-1]] = _transformers[typ](orig_value)

	return instance
