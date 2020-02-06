from setuptools import setup, find_packages

with open('README.md') as file:
	long_description = file.read()

setup(
	name="simulator",
	version="0.1.dev0",
	description="Simulates a HTC cache system",
	author = "Paul Skopnik",
	author_email = "paul@skopnik.me",
	long_description = long_description,
	long_description_content_type = 'text/markdown',
	package_data = {'simulator': ['py.typed']},
	packages = find_packages('src'),
	package_dir = {'': 'src'},
	zip_safe = False,
	install_requires = [
		'apq',
		'bjec',
		'orjson',
		'typing-extensions',
	],
	entry_points = {
		'console_scripts': ['simulator=simulator.cli:main'],
	},
)
