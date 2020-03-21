PIPENV := $(shell which pipenv)
ifneq ($(PIPENV),)
	PYTHON := $(shell $(PIPENV) --py)
else
	PYTHON := $(shell which python)
endif

ifneq ($(TEST_PATTERN),)
	TEST_FLAGS := -k $(TEST_PATTERN)
endif

type-check:
	$(PIPENV) run mypy src/simulator
	$(PIPENV) run mypy tests

test:
ifeq ($(NO_MYPY),)
		$(PIPENV) run mypy tests
endif
	$(PIPENV) run pytest tests $(TEST_FLAGS)

.PHONY: test type-check
