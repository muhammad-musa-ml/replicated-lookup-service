import pytest

import common


def test_summarize_reports_median_and_spread():
    s = common.summarize([5, 1, 3, 2, 4])
    assert (s["median"], s["min"], s["max"], s["n"]) == (3, 1, 5, 5)
    assert s["q1"] == 2 and s["q3"] == 4


def test_summarize_small_samples_fall_back_to_min_max():
    s = common.summarize([2.0, 4.0])
    assert (s["q1"], s["q3"], s["median"]) == (2.0, 4.0, 3.0)


def test_csv_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "RESULTS", tmp_path)
    common.write_csv("x", [{"a": 1, "b": "t"}, {"a": 2, "b": "u"}])
    assert common.read_csv("x") == [{"a": "1", "b": "t"}, {"a": "2", "b": "u"}]


def test_missing_tools_do_not_crash_metadata(monkeypatch):
    monkeypatch.setattr(common.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError()))
    assert common.sh(["nope"]).returncode == 127
    assert common.docker_info() == {}


def test_write_meta_records_command_and_commit(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "RESULTS", tmp_path)
    monkeypatch.setattr(common, "machine_info", lambda: {"cpus": 1})
    common.write_meta("exp", ["run.py", "--reps", "5"], {"parameters": {"reps": 5}})
    import json

    meta = json.loads((tmp_path / "exp.meta.json").read_text())
    assert meta["command"] == "python run.py --reps 5" and meta["parameters"] == {"reps": 5}
    assert meta["commit"]


def test_summarize_script_writes_a_table_from_the_csvs(tmp_path, monkeypatch):
    import summarize

    monkeypatch.setattr(common, "RESULTS", tmp_path)
    monkeypatch.setattr(summarize, "RESULTS", tmp_path)
    monkeypatch.setattr(summarize, "read_csv", common.read_csv)
    rows = [{"impl": "go", "rep": r, "seed": r, "threads": 4, "requests": 100, "hit_rate": 0, "errors": 0,
             "p50_ms": 5 + r, "p90_ms": 7, "p99_ms": 9, "max_ms": 12, "rps": 400} for r in range(5)]
    common.write_csv("impls", rows)
    summarize.main()
    text = (tmp_path / "summary.md").read_text()
    assert "| go | 7.00 (5.00-9.00) |" in text
