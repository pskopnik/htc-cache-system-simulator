from io import BytesIO, StringIO
import jsonschema
import pytest # type: ignore[import]

from simulator.workload.jsonparams import (
	load_validate_transform,
	parse_bytes_rate,
	parse_bytes_size,
)
from simulator.workload.units import MiB, GiB

def test_parse_bytes_rate() -> None:
	assert parse_bytes_rate('0 B/s') == 0
	assert parse_bytes_rate('1 B/s') == 1
	assert parse_bytes_rate('1 iB/s') == 1
	assert parse_bytes_rate('1.5 MiB/s') == round(1.5 * MiB)
	assert parse_bytes_rate('200 GiB/s') == 200 * GiB
	assert parse_bytes_rate('0 GiB/s') == 0

	with pytest.raises(ValueError):
		parse_bytes_rate('0')
	with pytest.raises(ValueError):
		parse_bytes_rate('1.5 MiB')
	with pytest.raises(ValueError):
		parse_bytes_rate('200 GB/s')
	with pytest.raises(ValueError):
		parse_bytes_rate('.1 GiB/s')
	with pytest.raises(ValueError):
		parse_bytes_rate('-200 GiB/s')
	with pytest.raises(ValueError):
		parse_bytes_rate('GiB/s 200')

def test_parse_bytes_size() -> None:
	assert parse_bytes_size('0 B') == 0
	assert parse_bytes_size('1 B') == 1
	assert parse_bytes_size('1 iB') == 1
	assert parse_bytes_size('1.5 MiB') == round(1.5 * MiB)
	assert parse_bytes_size('200 GiB') == 200 * GiB
	assert parse_bytes_size('0 GiB') == 0

	with pytest.raises(ValueError):
		parse_bytes_size('0')
	with pytest.raises(ValueError):
		parse_bytes_size('1.5 MiB/s')
	with pytest.raises(ValueError):
		parse_bytes_size('200 GB')
	with pytest.raises(ValueError):
		parse_bytes_size('.1 GiB')
	with pytest.raises(ValueError):
		parse_bytes_size('-200 GiB')
	with pytest.raises(ValueError):
		parse_bytes_size('GiB 200')

_correct_example = """
{
	"count": 15,
	"fraction": 0.5,
	"size": "10 GiB",
	"rate": "1.5 MiB/s"
}
"""

_incorrect_example = """
{
	"count": -15,
	"fraction": 10000
}
"""

def test_load_validate_transform() -> None:
	assert load_validate_transform(
		StringIO(_correct_example),
		'test_jsonparams_simple_schema.json',
		schema_package = __name__,
	)['count'] == 15

	assert load_validate_transform(
		BytesIO(_correct_example.encode()),
		'test_jsonparams_simple_schema.json',
		schema_package = __name__,
	)['count'] == 15

	with pytest.raises(FileNotFoundError):
		load_validate_transform(
			StringIO(_correct_example),
			'non_existing',
			schema_package = __name__,
		)

	with pytest.raises(jsonschema.ValidationError):
		load_validate_transform(
			StringIO(_incorrect_example),
			'test_jsonparams_simple_schema.json',
			schema_package = __name__,
		)

	assert load_validate_transform(
		StringIO(_correct_example),
		'test_jsonparams_simple_schema.json',
		schema_package = __name__,
		transformations = [
			(('size',), 'bytes_size'),
			(('rate',), 'bytes_rate'),
		],
	)['size'] == 10 * GiB
