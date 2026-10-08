import pyarrow as pa
import pyarrow.parquet as pq

from src.size_report import report, size_of


def test_size_label_parsing():
    assert size_of("size=16x") == 16
    assert size_of("size=0.25x") == 0.25


def test_report_counts_rows_files_bytes(tmp_path):
    for label, n_files, rows in [("1", 2, 10), ("2", 4, 10)]:
        d = tmp_path / f"size={label}x"
        d.mkdir()
        for i in range(n_files):
            pq.write_table(pa.table({"x": list(range(rows))}), d / f"part-{i}.parquet")
    r = report(tmp_path)
    assert [(x["size"], x["rows"], x["files"]) for x in r] == [(1.0, 20, 2), (2.0, 40, 4)]
    assert r[1]["rows_vs_1x"] == 2.0
    assert all(x["bytes"] > 0 for x in r)
