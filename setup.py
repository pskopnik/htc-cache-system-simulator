from setuptools import setup, find_packages

with open('README.md') as file:
    long_description = file.read()

setup(
    name = "simulator",
    version = "1.0.1",
    description = "Simulates a cache system in a high-throughput computing environment",
    author = "Paul Skopnik",
    author_email = "paul@skopnik.me",
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    package_data = {
        'simulator': ['py.typed'],
        'simulator.workload.models': ['*.json'],
    },
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    zip_safe = False,
    python_requires = '>= 3.7, < 4',
    install_requires = [
        'apq >= 0.10.0, < 0.20.0',
        'jsonschema >= 3.2.0, < 4.0.0',
        'orjson >= 2.5.1, < 3.0.0',
        'setuptools',
        'sortedcontainers >= 2.2.0, < 3.0.0',
        'typing-extensions',
    ],
    entry_points = {
        'console_scripts': ['simulator=simulator.cli:main'],
    },
)
