"""Table names (lib/warehouse.py): parsed once, then quoted as identifiers."""

import pytest
from psycopg import sql

from lib.warehouse import TableName


def test_table_name():
    table = TableName.parse("raw.customer_transactions")
    assert (table.schema, table.name) == ("raw", "customer_transactions")
    assert str(table) == "raw.customer_transactions"
    assert table.slug == "raw_customer_transactions"
    assert table.identifier == sql.Identifier("raw", "customer_transactions")
    assert table.in_schema("customer_transactions_p20261020") == sql.Identifier(
        "raw", "customer_transactions_p20261020"
    )


@pytest.mark.parametrize("table", ["customer_transactions", "db.raw.customer_transactions"])
def test_a_name_without_exactly_one_schema_is_rejected(table):
    with pytest.raises(ValueError):
        TableName.parse(table)
