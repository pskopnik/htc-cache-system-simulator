from . import BytesSize, TimeStamp

iB: BytesSize = 1
KiB: BytesSize = 2 ** (10 * 1)
MiB: BytesSize = 2 ** (10 * 2)
GiB: BytesSize = 2 ** (10 * 3)
TiB: BytesSize = 2 ** (10 * 4)
PiB: BytesSize = 2 ** (10 * 5)
EiB: BytesSize = 2 ** (10 * 6)
ZiB: BytesSize = 2 ** (10 * 7)
YiB: BytesSize = 2 ** (10 * 8)

bytes_size_units = {
	'iB': iB,
	'KiB': KiB,
	'MiB': MiB,
	'GiB': GiB,
	'TiB': TiB,
	'PiB': PiB,
	'EiB': EiB,
	'ZiB': ZiB,
	'YiB': YiB,
}

minute: TimeStamp = 60
hour: TimeStamp = 60 * 60
day: TimeStamp = 24 * 60 * 60
week: TimeStamp = 7 * 24 * 60 * 60

time_stamp_units = {
	'minute': minute,
	'hour': hour,
	'day': day,
	'week': week,
}
