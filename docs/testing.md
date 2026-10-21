# Testing

## Levels

| Level | What | Where | Status |
|---|---|---|---|
| Static checks | Lint and format: Python (ruff), dbt SQL (sqlfluff), YAML (yamllint), shell (shellcheck) | `tests/lint.sh` | In place |
| Unit tests | Pure logic in `lib/`: pipeline config validation, schema drift checks, file checksums, rendering the source URI, retention rules, alert routing | `tests/unit/`, pytest | In place |
| DAG integrity | Every DAG imports and follows the platform's rules; every pipeline config builds a DAG with the expected schedule, ordering and tasks | `tests/dags/` | In place |
| dbt project integrity | Rules every dbt model must follow, checked on dbt's manifest: build locks, contracts, primary keys, classifications, retentions, descriptions | `tests/dbt/test_dbt_project.py` | In place |
| dbt unit tests | Transformation logic with given inputs and expected outputs: staging parsing and data-quality codes, fact amounts, SCD2 versioning (full refresh and incremental), aggregates | `dbt/models/**/_*unit_tests.yml`, run by `tests/dbt/` | In place |
| Integration | Platform components against a real warehouse: loads, schema drift, publishing, column grants, retention, permissions | `warehouse-db/tests/` | Permissions in place; the rest is future work (see [Future work](future-work.md)) |
| End-to-end | The whole pipeline on the Compose stack, checking the published results and audit logs | – | Future work |
| Data tests | Checks on the data in every run: contracts, dbt data tests, data-quality thresholds | dbt, in the pipelines | In place |

## Running the tests

```bash
docker compose run --rm tests                        # all tests; those needing the warehouse need the stack running
docker compose run --rm tests tests/unit -k drift    # pytest arguments, e.g. selecting tests
```

Tests that need the warehouse are skipped when it is not reachable, unless `REQUIRE_WAREHOUSE` is set, as it should be in CI.

The `tests` service, behind the `test` profile so `docker compose up` does not start it, runs pytest in the `test` stage of the Airflow image (`airflow/Dockerfile`): the platform image plus the test packages. Tests therefore run with exactly the Airflow, Cosmos and dbt versions the platform runs, and the test packages never reach the platform image. The working copies of `lib/`, `pipelines/`, `airflow/dags/`, `dbt/` and `tests/` are mounted, so changes need no rebuild.

## Static checks

```bash
docker compose run --rm lint
```

`tests/lint.sh` runs every check, then fails if any did, with the linters of the test image, so nothing needs installing:

| Tool | Checks | Settings |
|---|---|---|
| ruff | Python lint (pyflakes, pycodestyle, import order, likely bugs, outdated syntax) and formatting | `pyproject.toml` |
| sqlfluff | dbt SQL, rendered with dbt's functions and the project's macros (Jinja templater) | `dbt/.sqlfluff` |
| yamllint | YAML | `.yamllint.yml` |
| shellcheck | Shell scripts | defaults |

Each tool is adapted to the style the code already follows, rather than its defaults: lines of 110 characters for Python and SQL, lowercase SQL, and a few SQL style rules turned off (listed in `dbt/.sqlfluff`).

- sqlfluff renders models without dbt, which needs no warehouse; `dbt/.sqlfluff_library/` stands in for the dbt_utils macros the models use. Generic tests (`tests/generic/`) are not linted: they are mostly Jinja loops, which sqlfluff's automatic fixes break.
- Unit test fixtures may exceed the YAML line length, so that each row stays on one line.
- To apply the fixes the tools can make: `ruff check --fix .`, `ruff format .` and, from `dbt/`, `sqlfluff fix models tests`, in the test image.

## DAG integrity tests

`tests/dags/test_dags.py` parses the DAGs folder as Airflow's DAG processor does, without running anything, and checks:

- every DAG imports, and every pipeline config has its DAG
- **every DAG follows the platform's rules:** a description and tags, no catchup, and on every task a real owner (alerts go to the owner's webhook), retries and the failure alert. A hand-written DAG gets them from `lib/orchestration/defaults.py`.
- **every pipeline DAG is built as expected:** an interval timetable, every task ordered on the previous day's run, the config's owner, the load as the only first task and the publish as the only last one, after every other task, and no dbt unit tests

They need no running service: Cosmos's cache of `dbt ls` results, kept in Airflow's metadata database, is turned off for the tests service.

## dbt project integrity tests

`tests/dbt/test_dbt_project.py` runs `dbt parse`, which needs no warehouse, and checks the manifest it writes: each model's final configuration, once the project-level, folder-level and model settings are combined. Its rules are ones the platform relies on, which would otherwise only fail at run time, in a pipeline:

| Rule | Relied on by |
|---|---|
| Every model takes its build lock before its build and releases it after, both outside the model's transaction | The build locks (see [One build of a model at a time](data-modelling.md#one-build-of-a-model-at-a-time)): released at the commit, a lock would no longer cover dbt dropping its backup table |
| Every core model has an enforced contract | dbt creating core tables from their declared types, and the publish copying that structure to live |
| Every core model has a primary key | The publish, which syncs live tables on it |
| Every column's classification is `internal` or `restricted` | The publish's column grants |
| A retention is `{column: <one of the model's columns>, days: <positive integer>}` | The builder, and the expiry as the model is built |
| Every model and column is described | dbt's documentation, the catalogue consumers read |

Each failing rule lists the models, or columns, that break it. New project-wide rules belong here.

## dbt unit tests

A dbt unit test runs a model's SQL on given input rows, replacing every input with literal values, and compares the result with expected rows. No table is read or written, but the query runs on the warehouse, and dbt reads the inputs' column types from it.

`tests/dbt/test_dbt_unit_tests.py` runs them with `dbt test --select test_type:unit`, after creating what is missing for the column types: the raw tables, from the pipeline configs with the loader's DDL, and models never built, with `dbt run` (empty on an empty warehouse). Existing tables are never rebuilt, so running the tests on a local stack keeps its data.

- **Not in pipeline runs:** they check code, not data. Pipelines exclude them (`DBT_EXCLUDE_RESOURCE_TYPES=unit_test`, set in `lib/transformation/dbt.py`), as `dbt test --select <model>` would otherwise run the model's unit tests in every run.
- **Incremental models** have one test per branch: `overrides: macros: is_incremental: true|false`, with `{{ this }}` given as an input for the incremental branch.
- **Writing expected rows:** every row lists the same columns; numbers are written with the model's scale, as strings (`"30.00"`), and arrays as Postgres literals (`"{price_invalid}"`). Values are compared before a contract's cast, so a column written as `coalesce(x, 0)` is expected as `0`.
- dbt-postgres reports array columns as `ARRAY`, which dbt cannot cast unit test values to; `dbt/macros/get_columns_in_relation.sql` reports them with their element type (`text[]`) instead.
- `dbt run --empty` is not used to create missing models: it breaks models that alias a ref (`{{ ref('...') }} as stg`).

## Conventions

- Unit tests need no running service: databases, connections and HTTP calls are replaced with small fakes or pytest's `monkeypatch`.
- One test file per module, with the module's rules in its docstring.
- `tests/conftest.py` imports Airflow before any test imports Cosmos: if the first import of Airflow happened inside a Cosmos module, Airflow's initialisation would load Cosmos's plugin while that module is only partly initialised (a circular import). It would only show when a test file runs on its own.
- pytest settings are in `pyproject.toml`.
- **dbt test names** say what is checked, as `<model>_<what is checked>`, e.g. `fct_transactions_quantity_at_least_1` or `stg_customer_transactions_volume_within_range`. They appear in Airflow task IDs, alerts, `audit.dbt_run_results` and the failure tables in `audit`. Generic tests with arguments get a `name:` in the YAML: dbt's default name strings their arguments together (e.g. `volume_within_range_stg_customer_transactions_14__7__0_5`). Plain `not_null` and `unique` tests keep dbt's default names (`not_null_<model>_<column>`), which already say what they check; singular tests are named by their file.
