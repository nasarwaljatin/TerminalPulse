import numpy as np
from fastapi.testclient import TestClient

from app.features import FEATURES, generate_dataset
from app.model import train


def test_isolation_forest_meets_recall_target():
    model = train(n_train=6000, n_val=2500, n_test=4000, seed=11)
    m = model.metrics
    assert m["recall"] >= 0.88, m
    assert m["precision"] >= 0.6, m
    assert m["falsePositiveRate"] <= 0.05, m


def test_generator_is_deterministic_and_labeled():
    X1, y1, _ = generate_dataset(300, seed=3)
    X2, y2, _ = generate_dataset(300, seed=3)
    assert np.allclose(np.nan_to_num(X1), np.nan_to_num(X2))
    assert (y1 == y2).all()
    assert X1.shape == (300, len(FEATURES))
    assert 0 < y1.mean() < 0.3


def test_score_endpoint_flags_obvious_fault():
    from app.main import app

    client = TestClient(app)
    healthy = {"txn_per_min": 3.5, "decline_rate": 0.05, "error_rate": 0.005, "avg_latency_ms": 480,
               "hb_missing_ratio": 0.02, "avg_battery": 75, "avg_signal_dbm": -70, "avg_temperature_c": 38,
               "avg_queue_depth": 0.4, "reboots_per_hour": 0}
    overheating = {**healthy, "avg_temperature_c": 74, "reboots_per_hour": 4, "avg_latency_ms": 1300}
    res = client.post("/score", json={"terminals": [
        {"terminalId": "T1", "features": healthy},
        {"terminalId": "T2", "features": overheating},
        {"terminalId": "T3", "features": {**healthy, "avg_latency_ms": None}},
    ]})
    assert res.status_code == 200
    by_id = {r["terminalId"]: r for r in res.json()["results"]}
    assert not by_id["T1"]["flagged"]
    assert by_id["T2"]["flagged"]
    assert any("temperature" in r for r in by_id["T2"]["reasons"])
    assert not by_id["T3"]["flagged"]
