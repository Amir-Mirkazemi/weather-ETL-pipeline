import sqlite3

import pytest
import requests

import main


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def good_payload(temp=20.5, humidity=55, observed="2026-11-08T12:00"):
    return {
        "current": {
            "time": observed,
            "temperature_2m": temp,
            "relative_humidity_2m": humidity,
        }
    }


# --- validation -----------------------------------------------------------
def test_valid_reading_normal():
    assert main.is_valid_reading(22.5, 60) is True


def test_rejects_impossible_temp():
    assert main.is_valid_reading(500, 50) is False


def test_rejects_impossible_humidity():
    assert main.is_valid_reading(20, 150) is False


def test_boundaries():
    assert main.is_valid_reading(-90, 0) is True
    assert main.is_valid_reading(60, 100) is True


# --- location rotation ----------------------------------------------------
def test_todays_location_is_in_locations_list():
    assert main.get_todays_location() in main.LOCATIONS


# --- retry logic ----------------------------------------------------------
def test_retry_succeeds_after_transient_failures(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, headers, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ConnectionError("boom")
        if calls["n"] == 2:
            return FakeResponse(503)
        return FakeResponse(200, good_payload())

    monkeypatch.setattr(main.requests, "get", fake_get)
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    resp = main.fetch_with_retry("http://x", {}, retries=3, delay=0)
    assert resp is not None and resp.status_code == 200
    assert calls["n"] == 3


def test_retry_returns_none_when_all_attempts_fail(monkeypatch):
    monkeypatch.setattr(main.requests, "get", lambda *a, **k: FakeResponse(500))
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    assert main.fetch_with_retry("http://x", {}, retries=3, delay=0) is None


# --- database behaviour ---------------------------------------------------
def test_save_reading_is_idempotent(tmp_path):
    conn = sqlite3.connect(tmp_path / "w.db")
    main.init_db(conn)
    entry = ("2026-11-08 12:01:00", 20.5, 55, "Tokyo, Japan", "2026-11-08T12:00")
    assert main.save_reading(conn, entry) is True
    assert main.save_reading(conn, entry) is False  # same observation: skipped
    assert conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0] == 1


def test_init_db_migrates_old_schema_without_losing_rows(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.db")
    conn.execute(
        "CREATE TABLE weather (timestamp TEXT, temp_c REAL, humidity INTEGER, city TEXT)"
    )
    conn.execute("INSERT INTO weather VALUES ('2026-09-29 03:29:45', 12.0, 70, 'London, UK')")
    conn.commit()
    main.init_db(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(weather)")}
    assert "observed_at" in cols
    assert conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0] == 1


# --- end-to-end behaviour of run_pipeline --------------------------------
def test_pipeline_loads_valid_reading(monkeypatch, tmp_path):
    db = tmp_path / "w.db"
    monkeypatch.setattr(main, "fetch_with_retry", lambda *a, **k: FakeResponse(200, good_payload()))
    main.run_pipeline(str(db))
    assert sqlite3.connect(db).execute("SELECT COUNT(*) FROM weather").fetchone()[0] == 1


def test_pipeline_exits_nonzero_when_api_is_down(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "fetch_with_retry", lambda *a, **k: None)
    with pytest.raises(SystemExit) as exc:
        main.run_pipeline(str(tmp_path / "w.db"))
    assert exc.value.code == 1


def test_pipeline_rejects_implausible_reading_and_does_not_insert(monkeypatch, tmp_path):
    db = tmp_path / "w.db"
    monkeypatch.setattr(
        main, "fetch_with_retry", lambda *a, **k: FakeResponse(200, good_payload(temp=500))
    )
    with pytest.raises(SystemExit) as exc:
        main.run_pipeline(str(db))
    assert exc.value.code == 1
    assert not db.exists()  # nothing written when the gate rejects


def test_pipeline_exits_nonzero_on_malformed_payload(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "fetch_with_retry", lambda *a, **k: FakeResponse(200, {"oops": 1}))
    with pytest.raises(SystemExit) as exc:
        main.run_pipeline(str(tmp_path / "w.db"))
    assert exc.value.code == 1
