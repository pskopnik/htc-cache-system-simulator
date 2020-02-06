from apq import KeyedPQ

from .mcf import MCF


class Size(MCF):
	"""Processor evicting the file with the greatest file size.
	"""

	class State(MCF.State):
		def __init__(self) -> None:
			super(Size.State, self).__init__()
			self._pq = KeyedPQ(max_heap=True)

	def _init_state(self) -> 'Size.State':
		return Size.State()
