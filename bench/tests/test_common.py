
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


def test_upstream_counts_reads_the_cache_tier_metrics():
    import run_failover

    text = (
        '# HELP x\nrls_upstream_errors_total{code="UNAVAILABLE",replica="dataset-go-1"} 3.0\n'
        'rls_upstream_errors_total{code="DEADLINE_EXCEEDED",replica="dataset-go-1"} 2.0\n'
        'rls_upstream_errors_total{code="UNAVAILABLE",replica="dataset-go-2"} 9.0\n'
        'rls_upstream_answers_total{replica="dataset-go-1"} 100.0\n'
        'rls_upstream_answers_total{replica="dataset-go-2"} 250.0\n'
    )
    assert run_failover.upstream_counts(text, "dataset-go-1") == {
        "victim_errors_seen": 5, "answered_by_victim": 100, "answered_by_other": 250}


def test_redact_removes_repo_and_home_paths_in_both_slash_styles():
    bs = chr(92)
    root, home = common.ROOT, common.Path.home()
    text = [f"{root}{bs}.venv{bs}Scripts{bs}python.exe", f"{root.as_posix()}/bench/run.py".upper(),
            f"{home}{bs}AppData{bs}x"]
    out = common.redact({"a": text, "n": 3, "nested": {"p": f"{home.as_posix()}/uv/python"}})
    assert out["a"][0] == f"<repo>{bs}.venv{bs}Scripts{bs}python.exe"
    assert out["a"][1].startswith("<repo>")
    assert out["a"][2] == f"<home>{bs}AppData{bs}x"
    assert out["nested"]["p"] == "<home>/uv/python"
    assert out["n"] == 3


def test_redact_argv_handles_both_flag_styles():
    flags = {"--ft-python": "<ft>", "--standard-python": "<std>"}
    argv = ["run.py", "--ft-python", "/some/where/py", "--reps", "5", "--standard-python=/usr/bin/python"]
    assert common.redact_argv(argv, flags) == [
        "run.py", "--ft-python", "<ft>", "--reps", "5", "--standard-python=<std>"]


def test_write_meta_never_records_local_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "RESULTS", tmp_path)
    monkeypatch.setattr(common, "machine_info", lambda: {"cpus": 1})
    exe = str(common.ROOT / ".venv" / "Scripts" / "python.exe")
    home_exe = str(common.Path.home() / "AppData" / "uv" / "python.exe")
    common.write_meta("exp", ["bench/run.py", "--python", exe], {"parameters": {"python": home_exe}})
    text = (tmp_path / "exp.meta.json").read_text()
    # json escapes backslashes, so check the parts a path is made of as well as the whole
    for needle in (str(common.ROOT), str(common.Path.home()), common.Path.home().name):
        assert needle not in text and needle.replace(chr(92), chr(92) * 2) not in text
    assert "<repo>" in text and "<home>" in text
