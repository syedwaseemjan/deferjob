from __future__ import annotations

import pytest

from deferjob.schema import check_table, install_sql


def test_install_sql_uses_the_table_name() -> None:
    ddl = install_sql("close_later")
    assert "CREATE TABLE IF NOT EXISTS close_later" in ddl
    assert "CREATE INDEX IF NOT EXISTS close_later_due" in ddl
    assert "jsonb_build_object()" in ddl


def test_table_name_must_be_a_simple_identifier() -> None:
    with pytest.raises(ValueError):
        check_table("defer; drop table")
    with pytest.raises(ValueError):
        check_table("jobs-1")
