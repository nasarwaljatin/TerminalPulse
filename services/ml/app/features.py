"""Feature definitions shared by training and live scoring.

Live vectors are computed by the gateway (services/gateway/src/jobs.ts: computeFeatures) over a
sliding window of persisted transactions and heartbeats; the synthetic generator below produces
the same features by simulating that window from shared/fault-profiles.json.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np

FEATURES = [
    "txn_per_min",
    "decline_rate",
    "error_rate",
    "avg_latency_ms",
    "hb_missing_ratio",
    "avg_battery",
    "avg_signal_dbm",
    "avg_temperature_c",
    "avg_queue_depth",
    "reboots_per_hour",
]

# Human-readable labels used when explaining why a terminal was flagged.
LABELS = {
    "txn_per_min": ("transaction rate", "/min"),
    "decline_rate": ("decline rate", "%"),
    "error_rate": ("error rate", "%"),
    "avg_latency_ms": ("auth latency", " ms"),
    "hb_missing_ratio": ("missed heartbeats", "%"),
    "avg_battery": ("battery", "%"),
    "avg_signal_dbm": ("signal", " dBm"),
    "avg_temperature_c": ("temperature", " C"),
    "avg_queue_depth": ("offline queue", " items"),
    "reboots_per_hour": ("reboots", "/h"),
}
PERCENT_FEATURES = {"decline_rate", "error_rate", "hb_missing_ratio"}


def load_profiles() -> dict:
    here = Path(__file__).resolve().parents
    candidates = [
        os.environ.get("PROFILES_PATH"),
        # Repo checkout: services/ml/app/features.py -> <repo>/shared. In a container the file is
        # /app/app/features.py, which has no 4th parent, so look it up safely.
        here[3] / "shared" / "fault-profiles.json" if len(here) > 3 else None,
        here[1] / "fault-profiles.json",
        Path.cwd() / "shared" / "fault-profiles.json",
    ]
    for c in candidates:
        if c and Path(c).exists():
            return json.loads(Path(c).read_text(encoding="utf-8"))
    raise FileNotFoundError("fault-profiles.json not found; set PROFILES_PATH")


MIN_SPAN_S = 300  # gateway MIN_HISTORY_S: terminals are scored only after 5 minutes of history


def generate_dataset(n_terminals: int, seed: int = 7, fault_fraction: float | None = None):
    """Simulates one scoring window per terminal. Returns (X, y, profile_names).

    The window span varies between MIN_SPAN_S and the full feature window, like live scoring,
    where recently seen terminals have shorter histories (and noisier rates).

    Mirrors services/simulator/src/fleet.ts: each terminal draws its parameters uniformly from
    its profile's ranges, then transactions/heartbeats in the window are sampled around them.
    """
    cfg = load_profiles()
    rng = np.random.default_rng(seed)
    full_window_s = cfg["featureWindowS"]
    hb_interval = cfg["heartbeatIntervalS"]
    frac = cfg["faultFraction"] if fault_fraction is None else fault_fraction
    fault_names = list(cfg["faultWeights"].keys())
    fault_w = np.array([cfg["faultWeights"][k] for k in fault_names], dtype=float)
    fault_w /= fault_w.sum()

    rows, labels, names = [], [], []
    for _ in range(n_terminals):
        if rng.random() < frac:
            name = fault_names[rng.choice(len(fault_names), p=fault_w)]
        else:
            name = "healthy"
        spec = cfg["profiles"][name]
        window_s = rng.uniform(min(MIN_SPAN_S, full_window_s), full_window_s)
        p = {k: rng.uniform(v[0], v[1]) for k, v in spec.items()}

        # Reboots take the terminal offline for 6-14 s each and drop heartbeats meanwhile.
        reboots = rng.poisson(p["rebootsPerHour"] * window_s / 3600)
        downtime = float(sum(rng.uniform(6, 14) for _ in range(reboots)))
        up_s = max(window_s - downtime, 0.0)

        n_txn = rng.poisson(p["txnPerMin"] * up_s / 60)
        if n_txn:
            r = rng.random(n_txn)
            error_rate = float(np.mean(r < p["errorP"]))
            decline_rate = float(np.mean((r >= p["errorP"]) & (r < p["errorP"] + p["declineP"])))
            sigma = 0.25
            mu = np.log(p["latencyMs"]) - sigma**2 / 2
            avg_latency = float(np.mean(np.exp(mu + sigma * rng.standard_normal(n_txn))))
        else:
            error_rate, decline_rate, avg_latency = 0.0, 0.0, np.nan

        slots = int(up_s // hb_interval)
        hb_count = rng.binomial(slots, 1 - p["hbDropP"]) if slots else 0
        hb_missing = min(1.0, max(0.0, 1 - hb_count / (window_s / hb_interval)))
        k = max(hb_count, 1)
        battery = float(np.clip(p["battery"] + rng.normal(0, 0.8, k).cumsum().mean() * 0.3, 0, 100))
        signal = float(np.mean(np.clip(p["signalDbm"] + rng.normal(0, 2.5, k), -130, -30)))
        temp = float(np.mean(p["temperatureC"] + rng.normal(0, 1.2, k)))
        queue = float(np.mean(np.maximum(0, np.round(p["queueDepth"] + rng.normal(0, 0.8, k)))))

        rows.append([
            n_txn / (window_s / 60),
            decline_rate,
            error_rate,
            avg_latency,
            hb_missing,
            battery,
            signal,
            temp,
            queue,
            reboots / (window_s / 3600),
        ])
        labels.append(0 if name == "healthy" else 1)
        names.append(name)
    return np.array(rows, dtype=float), np.array(labels), np.array(names)
