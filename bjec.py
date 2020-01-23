from bjec import job, build, P, Factory, Join
from bjec.build import GitRepo, Make
from bjec.runner import SubprocessRunner, ProcessArgs, Stdout
from bjec.processor import Threading, Inline
from bjec.generator import Chain, Combine, Literal, RepeatG, Product
from bjec.params import Function
from bjec.collector import Concatenate, CSV, Demux
from bjec.config import config

from os.path import join
import os


@job()
def out_out(j):
	j.generator(
		Combine(
			Chain(
				Literal(processor="landlord", processor_args="fetch"),
				# Literal(processor="greedydual", processor_args="total"),
				Product(
					processor = ["min", "rand", "lru"],
				),
			),
			Product(
				storage_size = [100, 200, 400, 500, 1000, 2000, 5000, 6000, 7000, 8000, 9000, 10000],
			),
		),
	)

	j.processor(Threading(
		config.User.get("cpu_count", os.cpu_count())
	))

	j.runner(SubprocessRunner.factory(
		"pipenv",
		input = ProcessArgs(
			"run", "python", "-m", "simulator.cli",
			"replay",
			"-f", "min_test.json",
			"--cache-processor", P("processor"),
			"--storage-size", Function(lambda p: p["storage_size"] * 2 ** (10 * 3)),
			"--no-header",
		),
		output = Stdout(),
	))

	coll = j.collector(CSV(
		file_path = "out_out_eval.csv",
		before_all = [[
			"processor",
			"storage_size",
			"accesses",
			"files",
			"total_bytes_accessed",
			"unique_bytes_accessed",
			"files_hit",
			"files_missed",
			"bytes_hit",
			"bytes_missed",
			"bytes_added",
			"bytes_removed",
		]],
		before_row = [P("processor"), P("storage_size")],
	))


	j.artefact(result=coll.aggregate)

	j.after(lambda j: print("Wrote results to", j.artefacts["result"].name))
	j.after(lambda j: map(lambda f: f.close(), j.artefacts["result"]))


@job()
def min_test(j):
	j.generator(
		Combine(
			Chain(
				Literal(processor="landlord", processor_args="fetch"),
				# Literal(processor="greedydual", processor_args="total"),
				Product(
					processor = ["min", "rand", "lru"],
				),
			),
			Product(
				storage_size = [100, 200, 400, 500, 1000, 2000, 5000, 6000, 7000, 8000, 9000, 10000],
			),
		),
	)

	j.processor(Threading(
		config.User.get("cpu_count", os.cpu_count())
	))

	j.runner(SubprocessRunner.factory(
		"pipenv",
		input = ProcessArgs(
			"run", "python", "-m", "simulator.cli",
			"replay",
			"-f", "min_test.json",
			"--cache-processor", P("processor"),
			"--storage-size", Function(lambda p: p["storage_size"] * 2 ** (10 * 3)),
			"--no-header",
		),
		output = Stdout(),
	))

	coll = j.collector(CSV(
		file_path = "min_test_eval.csv",
		before_all = [[
			"processor",
			"storage_size",
			"accesses",
			"files",
			"total_bytes_accessed",
			"unique_bytes_accessed",
			"files_hit",
			"files_missed",
			"bytes_hit",
			"bytes_missed",
			"bytes_added",
			"bytes_removed",
		]],
		before_row = [P("processor"), P("storage_size")],
	))


	j.artefact(result=coll.aggregate)

	j.after(lambda j: print("Wrote results to", j.artefacts["result"].name))
	j.after(lambda j: map(lambda f: f.close(), j.artefacts["result"]))

@job()
def two_months(j):
	j.generator(
		Combine(
			Chain(
				Literal(processor="landlord", processor_args="fetch"),
				# Literal(processor="greedydual", processor_args="total"),
				Product(
					processor = ["min", "rand", "lru"],
				),
			),
			Product(
				storage_size = [100, 200, 400, 500, 1000, 2000, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000, 16000, 18000, 20000, 22000, 24000, 26000],
			),
		),
	)

	j.processor(Threading(
		config.User.get("cpu_count", os.cpu_count())
	))

	j.runner(SubprocessRunner.factory(
		"pipenv",
		input = ProcessArgs(
			"run", "python", "-m", "simulator.cli",
			"replay",
			"-f", "two_months.json",
			"--cache-processor", P("processor"),
			"--storage-size", Function(lambda p: p["storage_size"] * 2 ** (10 * 3)),
			"--no-header",
		),
		output = Stdout(),
	))

	coll = j.collector(CSV(
		file_path = "two_months_eval.csv",
		before_all = [[
			"processor",
			"storage_size",
			"accesses",
			"files",
			"total_bytes_accessed",
			"unique_bytes_accessed",
			"files_hit",
			"files_missed",
			"bytes_hit",
			"bytes_missed",
			"bytes_added",
			"bytes_removed",
		]],
		before_row = [P("processor"), P("storage_size")],
	))


	j.artefact(result=coll.aggregate)

	j.after(lambda j: print("Wrote results to", j.artefacts["result"].name))
	j.after(lambda j: map(lambda f: f.close(), j.artefacts["result"]))
