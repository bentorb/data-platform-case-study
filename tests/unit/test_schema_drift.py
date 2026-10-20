"""Schema drift checks (lib/ingestion/schema_drift.py): which differences between a file,
a table and the expected columns are tolerated, and which fail the load."""

import io

import pytest

from lib.ingestion.schema_drift import SchemaDriftError, check_header, check_table, read_header

EXPECTED = ["id", "amount"]
RESERVED = ["_source_file", "_load_id"]


def test_read_header_leaves_the_stream_at_the_data():
    stream = io.BytesIO(b" id , amount\n1,10\n")
    assert read_header(stream) == ["id", "amount"]
    assert stream.read() == b"1,10\n"


def test_read_header_drops_a_byte_order_mark():
    assert read_header(io.BytesIO("﻿id,amount\n".encode())) == ["id", "amount"]


def test_read_header_of_an_empty_file():
    with pytest.raises(SchemaDriftError, match="empty"):
        read_header(io.BytesIO(b""))


def test_columns_in_another_order():
    assert check_header(["amount", "id"], EXPECTED, RESERVED) == []


def test_extra_columns_are_returned():
    assert check_header(["id", "discount", "amount"], EXPECTED, RESERVED) == ["discount"]


@pytest.mark.parametrize(
    "header, message",
    [
        (["id"], "missing expected columns: amount"),
        (["id", "amount", "id"], "duplicate or empty"),
        (["id", "", "amount"], "duplicate or empty"),
        (["id", "amount", "_load_id"], "reserved for lineage columns: _load_id"),
    ],
)
def test_header_failures(header, message):
    with pytest.raises(SchemaDriftError, match=message):
        check_header(header, EXPECTED, RESERVED)


class FakeCursor:
    """Answers the information_schema query with the given column names."""

    def __init__(self, columns):
        self.columns = columns

    def execute(self, query, params):
        pass

    def fetchall(self):
        return [(column,) for column in self.columns]


def test_a_table_that_does_not_exist_yet_passes():
    check_table(FakeCursor([]), "raw", "orders", EXPECTED)


def test_a_table_with_the_expected_columns_passes():
    check_table(FakeCursor(["amount", "id"]), "raw", "orders", EXPECTED)


def test_a_table_with_other_columns_fails():
    with pytest.raises(SchemaDriftError, match=r"missing: amount; not expected: price"):
        check_table(FakeCursor(["id", "price"]), "raw", "orders", EXPECTED)
