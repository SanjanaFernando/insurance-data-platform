import pytest

from pipeline import lake, snowflake_ops


class FakeCursor:
    def __init__(self, log, rows):
        self.log, self.rows = log, rows

    def execute(self, sql):
        self.log.append(sql)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, rows=((2970,),)):
        self.log, self.rows = [], rows

    def cursor(self):
        return FakeCursor(self.log, self.rows)


def write_csv(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def test_copy_file_lands_file_in_raw_zone(tmp_path):
    write_csv(tmp_path / "src" / "a.csv", ["id", "1"])
    out = lake.copy_file(str(tmp_path / "src"), str(tmp_path / "raw"), "a.csv")
    assert (tmp_path / "raw" / "a.csv").read_text() == "id\n1\n"
    assert out.endswith("a.csv")


def test_copy_file_fails_for_missing_source(tmp_path):
    with pytest.raises(FileNotFoundError):
        lake.copy_file(str(tmp_path), str(tmp_path / "raw"), "nope.csv")


def test_validate_files_rejects_header_only_and_empty(tmp_path):
    good = tmp_path / "good.csv"
    write_csv(good, ["id", "1"])
    header_only = tmp_path / "h.csv"
    write_csv(header_only, ["id"])
    empty = tmp_path / "e.csv"
    empty.write_text("")
    lake.validate_files([good])
    with pytest.raises(ValueError, match="no data rows"):
        lake.validate_files([good, header_only])
    with pytest.raises(ValueError, match="empty"):
        lake.validate_files([empty])


def test_upload_silver_clears_stage_then_puts_files(tmp_path):
    write_csv(tmp_path / "claims" / "part-0001.csv", ["claim_id", "1"])
    conn = FakeConn()
    assert snowflake_ops.upload_silver(conn, str(tmp_path)) == 1
    assert conn.log[0].startswith("REMOVE @")
    assert conn.log[1].startswith("PUT 'file://") and "AUTO_COMPRESS = FALSE" in conn.log[1]


def test_copy_into_raw_truncates_then_loads():
    conn = FakeConn()
    snowflake_ops.copy_into_raw(conn)
    assert conn.log[0].startswith("TRUNCATE TABLE") and conn.log[1].startswith("COPY INTO")
    assert "FORCE = TRUE" in conn.log[1]


def test_reconcile_passes_when_counts_match(tmp_path):
    write_csv(tmp_path / "claims" / "part-0001.csv", ["claim_id"] + [str(i) for i in range(2970)])
    assert snowflake_ops.reconcile(FakeConn(), str(tmp_path)) == {"silver_claims": 2970, "fact_claims": 2970}


def test_reconcile_fails_when_counts_differ(tmp_path):
    write_csv(tmp_path / "claims" / "part-0001.csv", ["claim_id", "1", "2"])
    with pytest.raises(ValueError, match="reconciliation failed"):
        snowflake_ops.reconcile(FakeConn(), str(tmp_path))
