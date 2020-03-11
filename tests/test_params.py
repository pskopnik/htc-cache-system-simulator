import itertools
import pytest # type: ignore[import]
from shlex import shlex
import string
from typing import Callable, cast, Dict, Generic, Iterable, Iterator, List, TypeVar

from simulator.params import (
	amend_shlex,
	EvaluatedField,
	Field,
	parse_user_args,
	SimpleField,
	tokens_until,
)

_T = TypeVar('_T')
_V = TypeVar('_V')


class _DummyField(Field, Generic[_T]):
	def __init__(
		self,
		name: str,
		value: _T,
		read_func: Callable[[shlex], None] = lambda _: None,
	) -> None:
		super(_DummyField, self).__init__(name)
		self._value: _T = value
		self._read_func: Callable[[shlex], None] = read_func

	def parse(self, lex: shlex) -> EvaluatedField[_T]:
		self._read_func(lex)
		return EvaluatedField(self, self._value)

	@staticmethod
	def read_one_token(lex: shlex) -> None:
		lex.get_token()
		return None

def _controls_from_lex(lex: shlex) -> Dict[str, str]:
	return {
		'commenters': lex.commenters,
		'wordchars': lex.wordchars,
		'whitespace': lex.whitespace,
		'escape': lex.escape,
		'quotes': lex.quotes,
		'escapedquotes': lex.escapedquotes,
	}

def _without_key(d: Dict[_T, _V], key: _T) -> Dict[_T, _V]:
	return dict(filter(lambda kv: kv[0] != key, d.items()))

@pytest.mark.parametrize( # type: ignore[misc]
	'attr',
	(
		'commenters',
		'wordchars',
		'whitespace',
		'escape',
		'quotes',
		'escapedquotes',
	),
)
def test_amend_shlex(attr: str) -> None:

	def without_attr(d: Dict[str, _V]) -> Dict[str, _V]:
		return _without_key(d, attr)

	def get_attr_value(lex: shlex) -> str:
		return cast(str, getattr(lex, attr))

	lex = shlex('', posix=True)

	# Store the original control attrs
	orig = _controls_from_lex(lex)
	# diff consists of one char in the control attr and one char which is not
	diff = get_attr_value(lex)[0] + list(set(string.printable).difference(get_attr_value(lex)))[0]

	with amend_shlex(lex, **{attr: 'myval'}):
		assert without_attr(_controls_from_lex(lex)) == without_attr(orig)
		assert get_attr_value(lex) == 'myval'

	assert _controls_from_lex(lex) == orig

	with amend_shlex(lex, **{'add_' + attr: diff}):
		assert without_attr(_controls_from_lex(lex)) == without_attr(orig)
		assert set(get_attr_value(lex)).issuperset(orig[attr])
		assert set(diff).issubset(get_attr_value(lex))
		assert len(get_attr_value(lex)) == len(set(get_attr_value(lex)))

	assert _controls_from_lex(lex) == orig

	with amend_shlex(lex, **{'rm_' + attr: diff}):
		assert without_attr(_controls_from_lex(lex)) == without_attr(orig)
		assert set(get_attr_value(lex)).issubset(orig[attr])
		assert set(diff).isdisjoint(get_attr_value(lex))
		assert len(get_attr_value(lex)) == len(set(get_attr_value(lex)))

	assert _controls_from_lex(lex) == orig

def test_tokens_until() -> None:
	lex = shlex('some tokens +', posix=True)
	assert list(tokens_until(lex, '+')) == ['some', 'tokens']
	assert lex.get_token() == '+'

	lex = shlex('some tokens,', posix=True)
	assert list(tokens_until(lex, (',', '|'))) == ['some', 'tokens']
	assert lex.get_token() == ','

	lex = shlex('some tokens |', posix=True)
	assert list(tokens_until(lex, (',', '|'))) == ['some', 'tokens']
	assert lex.get_token() == '|'

@pytest.mark.parametrize( # type: ignore[misc]
	'a,fieldb,another_field',
	(
		('some text', 34, 'text'),
	),
)
def test_parse_user_args(a: str, fieldb: int, another_field: str) -> None:
	class Collector(object):
		a: str
		fieldb: int
		another_field: str
		unset_field: int

	c = Collector()

	parse_user_args(
		'a=,fieldb=,another_field=',
		c,
		[
			_DummyField('a', a),
			_DummyField('fieldb', fieldb),
			_DummyField('another_field', another_field),
		],
	)

	assert c.a == a
	assert c.fieldb == fieldb
	assert c.another_field == another_field
	with pytest.raises(AttributeError):
		c.unset_field

	c = Collector()

	parse_user_args(
		'a=tok,fieldb=tok,another_field=tok',
		c,
		[
			_DummyField('a', a, read_func=_DummyField.read_one_token),
			_DummyField('fieldb', fieldb, read_func=_DummyField.read_one_token),
			_DummyField('another_field', another_field, read_func=_DummyField.read_one_token),
		],
	)

	assert c.a == a
	assert c.fieldb == fieldb
	assert c.another_field == another_field
	with pytest.raises(AttributeError):
		c.unset_field


@pytest.mark.parametrize( # type: ignore[misc]
	'raw,value',
	(
		('asdf123', 'asdf123'),
		('asdf jk_l', 'asdf jk_l'),
		('asdf,asdf', 'asdf'),
		('"asdf\\" asdf"', 'asdf" asdf'),
		('asdf\\,asdf', 'asdf,asdf'),
		('!$%&/()[]{}<>?_-.;:#+*', '!$%&/()[]{}<>?_-.;:#+*'),
	),
)
def test_simple_field(raw: str, value: str) -> None:
	conv_call_counter = 0

	def conv(val: str) -> str:
		nonlocal value, conv_call_counter
		conv_call_counter += 1
		assert val == value
		return val + 'postfix'

	field_name = 'some_field_name'
	field = SimpleField(field_name, conv)

	lex = shlex(raw, posix=True)
	evaluated_field = field.parse(lex)

	assert evaluated_field.name == field_name
	assert evaluated_field.value == value + 'postfix'
	assert conv_call_counter == 1

def test_parse_simple() -> None:
	class Collector(object):
		a: int
		b: str
		another_field: str

	c = Collector()

	parse_user_args(
		'a=-334,b=tag,another_field=size&dirname',
		c,
		[
			SimpleField('a', int),
			SimpleField('b', str),
			SimpleField('another_field', str)
		],
	)

	assert c.a == -334
	assert c.b == 'tag'
	assert c.another_field == 'size&dirname'
