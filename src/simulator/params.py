from abc import ABC, abstractmethod
from contextlib import contextmanager
from shlex import shlex
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, List, Optional, TypeVar, Union

_T = TypeVar('_T')

"""
What kind of user inputs are there?

344 - simple int
- 10_000_00 - not so simple int
22.3 - simple float
-22.3e45 - not so simple float
10 GiB - annotated number (int result, float user input)
greedy - string tag
total_size - string tag containing _ characters
average active bytes - string with spaces (multi-word)
access_size&dirname - sub-parseable string
access_size&dirname(top=1) - sub-parseable string
"""


class Field(ABC):
	def __init__(
		self,
		name: str,
	) -> None:
		self._name: str = name

	@property
	def name(self) -> str:
		return self._name

	@abstractmethod
	def parse(self, lex: shlex) -> 'EvaluatedField[Any]':
		"""Reads and interprets all tokens in lex until ',' as a field's value.

		That means ',' must be first token returned by lex after parse has
		returned. That can be accomplished by using lex.push_token() or the
		tokens_until helper. Alternatively, lex may reach EOF.
		"""
		raise NotImplementedError


class EvaluatedField(Generic[_T]):
	def __init__(
		self,
		field: Field,
		value: _T,
		meta_data: Optional[Dict[str, Any]] = None,
	):
		self._field: Field = field
		self._value: _T = value
		self._meta_data: Dict[str, Any] = meta_data if meta_data is not None else {}

	@property
	def field(self) -> Field:
		return self._field

	@property
	def name(self) -> str:
		return self._field.name

	@property
	def value(self) -> _T:
		return self._value

	@property
	def meta_data(self) -> Dict[str, Any]:
		return self._meta_data


class SimpleField(Generic[_T], Field):
	def __init__(
		self,
		name: str,
		conv: Callable[[str], _T],
	) -> None:
		super(SimpleField, self).__init__(name)
		self._conv: Callable[[str], _T] = conv

	def _read(self, lex: shlex) -> str:
		with amend_shlex(
			lex,
			add_wordchars = '!$%&/()[]{}<>?_-.;:#+*',
			commenters = '',
			whitespace = '',
		):
			return ''.join(tokens_until(lex, ','))

	def parse(self, lex: shlex) -> EvaluatedField[_T]:
		return EvaluatedField(self, self._conv(self._read(lex)))


def parse_user_args(user_args: str, dest: Any, fields: Iterable[Field]) -> None:
	"""Parse user args according to fields and store values in dest.
	"""
	lex = shlex(user_args, posix=True)
	fields_dict = dict((field.name, field) for field in fields)
	evaluated_fields: List[EvaluatedField[Any]] = []

	tok = lex.get_token()
	while tok != lex.eof:
		if tok == lex.eof:
			raise Exception('unexpected eof')
		elif len(tok) == 0:
			raise Exception(f'name is too short {tok!r}')
		elif tok not in fields_dict:
			raise Exception(f'unknown name {tok!r}')

		name = tok

		tok = lex.get_token()
		if tok != '=':
			raise Exception(f'not expected {tok!r}')

		evaluated_field = fields_dict[name].parse(lex)
		evaluated_fields.append(evaluated_field)
		setattr(dest, evaluated_field.name, evaluated_field.value)

		tok = lex.get_token()
		if tok == lex.eof:
			break
		elif tok != ',':
			raise Exception(f'not expected {tok!r}')

		tok = lex.get_token()

	return

@contextmanager
def amend_shlex(
	lex: shlex,
	add_commenters: Optional[str] = None,
	rm_commenters: Optional[str] = None,
	commenters: Optional[str] = None,
	add_wordchars: Optional[str] = None,
	rm_wordchars: Optional[str] = None,
	wordchars: Optional[str] = None,
	add_whitespace: Optional[str] = None,
	rm_whitespace: Optional[str] = None,
	whitespace: Optional[str] = None,
	add_escape: Optional[str] = None,
	rm_escape: Optional[str] = None,
	escape: Optional[str] = None,
	add_quotes: Optional[str] = None,
	rm_quotes: Optional[str] = None,
	quotes: Optional[str] = None,
	add_escapedquotes: Optional[str] = None,
	rm_escapedquotes: Optional[str] = None,
	escapedquotes: Optional[str] = None,
) -> Iterator[shlex]:
	"""Context manager temporarily changing shlex control variables.
	"""
	orig_commenters = lex.commenters
	orig_wordchars = lex.wordchars
	orig_whitespace = lex.whitespace
	orig_escape = lex.escape
	orig_quotes = lex.quotes
	orig_escapedquotes = lex.escapedquotes

	if add_commenters is not None:
		lex.commenters += ''.join(c for c in add_commenters if c not in lex.commenters)
	if rm_commenters is not None:
		lex.commenters = ''.join(c for c in lex.commenters if c not in rm_commenters)
	if commenters is not None:
		lex.commenters = commenters

	if add_wordchars is not None:
		lex.wordchars += ''.join(c for c in add_wordchars if c not in lex.wordchars)
	if rm_wordchars is not None:
		lex.wordchars = ''.join(c for c in lex.wordchars if c not in rm_wordchars)
	if wordchars is not None:
		lex.wordchars = wordchars

	if add_whitespace is not None:
		lex.whitespace += ''.join(c for c in add_whitespace if c not in lex.whitespace)
	if rm_whitespace is not None:
		lex.whitespace = ''.join(c for c in lex.whitespace if c not in rm_whitespace)
	if whitespace is not None:
		lex.whitespace = whitespace

	if add_escape is not None:
		lex.escape += ''.join(c for c in add_escape if c not in lex.escape)
	if rm_escape is not None:
		lex.escape = ''.join(c for c in lex.escape if c not in rm_escape)
	if escape is not None:
		lex.escape = escape

	if add_quotes is not None:
		lex.quotes += ''.join(c for c in add_quotes if c not in lex.quotes)
	if rm_quotes is not None:
		lex.quotes = ''.join(c for c in lex.quotes if c not in rm_quotes)
	if quotes is not None:
		lex.quotes = quotes

	if add_escapedquotes is not None:
		lex.escapedquotes += ''.join(c for c in add_escapedquotes if c not in lex.escapedquotes)
	if rm_escapedquotes is not None:
		lex.escapedquotes = ''.join(c for c in lex.escapedquotes if c not in rm_escapedquotes)
	if escapedquotes is not None:
		lex.escapedquotes = escapedquotes

	try:
		yield lex
	finally:
		lex.commenters = orig_commenters
		lex.wordchars = orig_wordchars
		lex.whitespace = orig_whitespace
		lex.escape = orig_escape
		lex.quotes = orig_quotes
		lex.escapedquotes = orig_escapedquotes

def tokens_until(lex: shlex, until_tok: Union[str, Iterable[str]]) -> Iterator[str]:
	until_toks = [until_tok,] if isinstance(until_tok, str) else list(until_tok)
	while True:
		tok = lex.get_token()
		if tok == lex.eof:
			return
		elif tok in until_toks:
			lex.push_token(tok)
			return
		else:
			yield tok
