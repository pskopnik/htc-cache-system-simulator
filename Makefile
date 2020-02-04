PIPENV := $(shell which pipenv)
ifneq ($(PIPENV),)
	PYTHON := $(shell $(PIPENV) --py)
else
	PYTHON := $(shell which python)
endif

type-check:
	$(PIPENV) run mypy -p simulator
	$(PIPENV) run mypy -p tests

test:
	$(PIPENV) run pytest tests

.PHONY: test type-check
