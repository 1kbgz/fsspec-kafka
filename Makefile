#########
# BUILD #
#########
.PHONY: develop-py develop-rs develop
develop-py:
	uv pip install -e .[develop]

develop-rs:
	make -C rust develop

develop: develop-rs develop-py  ## setup project for development

.PHONY: requirements-py requirements-rs requirements
requirements-py:  ## install prerequisite python build requirements
	uv pip install -r pyproject.toml --extra develop

requirements-rs:  ## install prerequisite rust build requirements
	make -C rust requirements

requirements: requirements-rs requirements-py  ## setup project for development

.PHONY: build-py build-rs build
build-py:
	python -m build -w -n

build-rs:
	make -C rust build

build: build-rs build-py  ## build the project

.PHONY: install
install:  ## install python library
	uv pip install .

#########
# LINTS #
#########
.PHONY: lint-py lint-rs lint-docs lint lints
lint-py:  ## run python linter with ruff
	python -m ruff check fsspec_kafka
	python -m ruff format --check fsspec_kafka

lint-rs:  ## run rust linter
	make -C rust lint

lint-docs:  ## lint docs with mdformat and codespell
	python -m mdformat --check README.md $(wildcard docs/src) 
	python -m codespell_lib README.md $(wildcard docs/src) 

lint: lint-rs lint-py lint-docs  ## run project linters

# alias
lints: lint

.PHONY: fix-py fix-rs fix-docs fix format
fix-py:  ## fix python formatting with ruff
	python -m ruff check --fix fsspec_kafka
	python -m ruff format fsspec_kafka

fix-rs:  ## fix rust formatting
	make -C rust fix

fix-docs:  ## autoformat docs with mdformat and codespell
	python -m mdformat README.md $(wildcard docs/src) 
	python -m codespell_lib --write README.md $(wildcard docs/src) 

fix: fix-rs fix-py fix-docs  ## run project autoformatters

# alias
format: fix

################
# Other Checks #
################
.PHONY: check-dist check-types checks check

check-dist:  ## check python sdist and wheel with check-dist
	check-dist -v

check-types:  ## check python types with ty
	python -m ty check

checks: check-dist check-types

# alias
check: checks

#########
# TESTS #
#########
.PHONY: test-py tests-py coverage-py
test-py:  ## run python tests
	python -m pytest -v fsspec_kafka/tests

# alias
tests-py: test-py

coverage-py:  ## run python tests and collect test coverage
	python -m pytest -v fsspec_kafka/tests --cov=fsspec_kafka --cov-report term-missing --cov-report xml

.PHONY: test-rs tests-rs coverage-rs
test-rs:  ## run rust tests
	make -C rust test

# alias
tests-rs: test-rs

coverage-rs:  ## run rust tests and collect test coverage
	make -C rust coverage

.PHONY: test coverage tests
test: test-py test-rs  ## run all tests

coverage: coverage-py coverage-rs  ## run all tests and collect test coverage

# alias
tests: test

###############
# INTEGRATION #
###############
# Live broker integration tests, gated on KAFKA_TEST_*BOOTSTRAP env vars.
# Uses podman-compose by default; override with `make COMPOSE="docker compose" ...`.
COMPOSE ?= podman-compose
COMPOSE_FILE ?= ci/docker-compose.yml
KAFKA_TEST_BOOTSTRAP ?= 127.0.0.1:59092
KAFKA_TEST_KERBEROS_BOOTSTRAP ?= localhost:59094
# Not exported, so plain `make test` does not trigger the gated tests.

.PHONY: kafka-up kafka-wait kafka-down kafka-logs test-integration

kafka-up:  ## start the Redpanda, KDC, and Kerberos Kafka fixtures
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build

kafka-wait:  ## block until both brokers accept connections
	@echo "waiting for redpanda + kerberos kafka to become ready..."
	@for i in $$(seq 1 60); do \
		if $(COMPOSE) -f $(COMPOSE_FILE) exec -T redpanda rpk cluster health --exit-when-healthy >/dev/null 2>&1 \
			&& $(COMPOSE) -f $(COMPOSE_FILE) exec -T kafka-kerberos /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-kerberos:9092 >/dev/null 2>&1; then \
			echo "brokers ready"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "timed out waiting for brokers"; $(COMPOSE) -f $(COMPOSE_FILE) ps; exit 1

kafka-down:  ## stop the fixtures and remove their volumes
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

kafka-logs:  ## tail the fixture logs
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f

test-integration:  ## run the env-gated integration tests against running fixtures
	cd rust && KAFKA_TEST_BOOTSTRAP="$(KAFKA_TEST_BOOTSTRAP)" cargo test --test redpanda
	KAFKA_TEST_BOOTSTRAP="$(KAFKA_TEST_BOOTSTRAP)" \
	KAFKA_TEST_KERBEROS_BOOTSTRAP="$(KAFKA_TEST_KERBEROS_BOOTSTRAP)" \
	KRB5_CONFIG="$(CURDIR)/ci/kerberos/krb5.conf" \
	KRB5CCNAME="FILE:$(CURDIR)/ci/kerberos/keytabs/client.ccache" \
	python -m pytest -v fsspec_kafka/tests/integration --cov=fsspec_kafka --cov-append --cov-report term-missing --cov-report xml

###########
# VERSION #
###########
.PHONY: show-version patch minor major

show-version:  ## show current library version
	@bump-my-version show current_version

patch:  ## bump a patch version
	@bump-my-version bump patch

minor:  ## bump a minor version
	@bump-my-version bump minor

major:  ## bump a major version
	@bump-my-version bump major

########
# DIST #
########
.PHONY: dist-py-wheel dist-py-sdist dist-rs dist-check dist publish

dist-py-wheel:  ## build python wheel
	python -m cibuildwheel --output-dir dist

dist-py-sdist:  ## build python sdist
	python -m build --sdist -o dist

dist-rs:  ## build rust dists
	make -C rust dist

dist-check:  ## run python dist checker with twine
	python -m twine check dist/*

dist: clean build dist-rs dist-py-wheel dist-py-sdist dist-check  ## build all dists

publish: dist  ## publish python assets

#########
# CLEAN #
#########
.PHONY: deep-clean clean

deep-clean: ## clean everything from the repository
	git clean -fdx

clean: ## clean the repository
	rm -rf .coverage coverage cover htmlcov logs build dist *.egg-info

############################################################################################

.PHONY: help

# Thanks to Francoise at marmelab.com for this
.DEFAULT_GOAL := help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

print-%:
	@echo '$*=$($*)'
