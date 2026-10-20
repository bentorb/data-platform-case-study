"""Shared test setup, loaded by pytest before any test module."""

# Initialise Airflow before tests import Cosmos. Airflow's initialisation loads Cosmos's
# plugin, and if the first import of Airflow happens inside a Cosmos module (e.g.
# cosmos.constants), the plugin imports that module while it is only partly
# initialised: a circular import.
import airflow  # noqa: F401
