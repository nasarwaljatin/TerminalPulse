"""Isolation Forest anomaly model for payment terminals."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from .features import FEATURES, LABELS, PERCENT_FEATURES, generate_dataset

ARTIFACT_DIR = Path(__file__).resolve().parents[1] / "artifacts"
MODEL_PATH = ARTIFACT_DIR / "model.joblib"
TARGET_RECALL = 0.9


@dataclass
class TerminalModel:
    pipeline: Pipeline
    threshold: float
    medians: np.ndarray
    scales: np.ndarray
    version: str
    metrics: dict = field(default_factory=dict)

    def scores(self, X: np.ndarray) -> np.ndarray:
        """Higher = more anomalous (negated sklearn score_samples)."""
        return -self.pipeline.score_samples(X)

    def predict(self, X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        s = self.scores(X)
        return s, s >= self.threshold

    def reasons(self, x: np.ndarray, top: int = 3) -> list[str]:
        """Explains a flag by the features furthest from the fleet's typical values (robust z-score)."""
        filled = np.where(np.isnan(x), self.medians, x)
        z = (filled - self.medians) / self.scales
        order = np.argsort(-np.abs(z))
        out = []
        for rank, i in enumerate(order[:top]):
            if abs(z[i]) < 2.5 and rank > 0:
                break
            name = FEATURES[i]
            label, unit = LABELS[name]
            val, typical = filled[i], self.medians[i]
            if name in PERCENT_FEATURES:
                val, typical = val * 100, typical * 100
            direction = "high" if z[i] > 0 else "low"
            out.append(f"{label} {direction}: {val:.1f}{unit} (fleet typical {typical:.1f}{unit})")
        return out


def build_pipeline(seed: int) -> Pipeline:
    return Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", RobustScaler()),
            ("forest", IsolationForest(n_estimators=300, max_samples=512, contamination="auto", random_state=seed)),
        ]
    )


def calibrate_threshold(scores: np.ndarray, y: np.ndarray, target_recall: float) -> float:
    """Highest threshold (i.e. best precision) whose recall on the labeled validation set meets the target."""
    faulty = np.sort(scores[y == 1])
    if len(faulty) == 0:
        return float(np.quantile(scores, 0.92))
    k = int(np.floor((1 - target_recall) * len(faulty)))
    return float(faulty[k])


def train(n_train: int = 8000, n_val: int = 3000, n_test: int = 5000, seed: int = 7, verbose: bool = False) -> TerminalModel:
    t0 = time.time()
    X_train, _y_train, _ = generate_dataset(n_train, seed=seed)  # labels unused: unsupervised fit
    X_val, y_val, _ = generate_dataset(n_val, seed=seed + 1)
    X_test, y_test, names_test = generate_dataset(n_test, seed=seed + 2)

    pipe = build_pipeline(seed)
    pipe.fit(X_train)

    medians = np.nanmedian(X_train, axis=0)
    mad = np.nanmedian(np.abs(X_train - medians), axis=0) * 1.4826
    floor = np.array([0.5, 0.01, 0.005, 25, 0.02, 3, 2, 1, 0.5, 0.1])  # avoid exploding z on near-constant features
    scales = np.maximum(mad, floor)

    model = TerminalModel(pipe, 0.0, medians, scales, version=f"iforest-{int(time.time())}")
    model.threshold = calibrate_threshold(model.scores(X_val), y_val, TARGET_RECALL)

    _, pred = model.predict(X_test)
    per_fault = {}
    for name in sorted(set(names_test) - {"healthy"}):
        mask = names_test == name
        per_fault[name] = round(float(pred[mask].mean()), 3)
    model.metrics = {
        "recall": round(float(recall_score(y_test, pred)), 3),
        "precision": round(float(precision_score(y_test, pred)), 3),
        "falsePositiveRate": round(float(pred[y_test == 0].mean()), 3),
        "testTerminals": int(n_test),
        "testFaulty": int(y_test.sum()),
        "recallByFault": per_fault,
        "threshold": round(model.threshold, 4),
        "trainSeconds": round(time.time() - t0, 1),
    }
    if verbose:
        print(json.dumps(model.metrics, indent=2))
    return model


def save(model: TerminalModel, path: Path = MODEL_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    (path.parent / "metrics.json").write_text(json.dumps(model.metrics, indent=2))


def load_or_train(path: Path = MODEL_PATH) -> TerminalModel:
    if path.exists():
        return joblib.load(path)
    model = train()
    save(model, path)
    return model
