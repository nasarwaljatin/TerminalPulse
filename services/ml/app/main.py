"""FastAPI scoring service: POST /score with per-terminal feature vectors."""

from __future__ import annotations

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel, Field

from .features import FEATURES
from .model import load_or_train

app = FastAPI(title="TerminalPulse anomaly service", version="1.0.0")
model = load_or_train()


class TerminalFeatures(BaseModel):
    terminalId: str
    features: dict[str, float | None]


class ScoreRequest(BaseModel):
    terminals: list[TerminalFeatures] = Field(default_factory=list, max_length=20000)


class ScoreResult(BaseModel):
    terminalId: str
    score: float
    flagged: bool
    reasons: list[str]


class ScoreResponse(BaseModel):
    modelVersion: str
    threshold: float
    results: list[ScoreResult]


@app.get("/healthz")
def healthz():
    return {"ok": True, "modelVersion": model.version}


@app.get("/model")
def model_info():
    return {"modelVersion": model.version, "features": FEATURES, "threshold": model.threshold, "evaluation": model.metrics}


@app.post("/score", response_model=ScoreResponse)
def score(req: ScoreRequest) -> ScoreResponse:
    if not req.terminals:
        return ScoreResponse(modelVersion=model.version, threshold=model.threshold, results=[])
    X = np.array(
        [[np.nan if t.features.get(f) is None else float(t.features[f]) for f in FEATURES] for t in req.terminals],
        dtype=float,
    )
    scores, flagged = model.predict(X)
    results = [
        ScoreResult(
            terminalId=t.terminalId,
            score=round(float(s), 4),
            flagged=bool(f),
            reasons=model.reasons(X[i]) if f else [],
        )
        for i, (t, s, f) in enumerate(zip(req.terminals, scores, flagged))
    ]
    return ScoreResponse(modelVersion=model.version, threshold=model.threshold, results=results)
