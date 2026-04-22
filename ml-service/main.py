"""
AnalyticCore ML Prediction Service
A lightweight FastAPI microservice for training and serving ML models.
"""
import os
import io
import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import (
    accuracy_score, f1_score, mean_squared_error, r2_score, mean_absolute_error,
)
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# ── Config ────────────────────────────────────────────────────────────────
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.environ.get("MODELS_DIR", os.path.join(APP_ROOT, "models_store"))
os.makedirs(MODELS_DIR, exist_ok=True)

ALLOWED_ALGORITHMS = {
    "random_forest_classifier": RandomForestClassifier,
    "random_forest_regressor": RandomForestRegressor,
    "logistic_regression": LogisticRegression,
    "linear_regression": LinearRegression,
    "decision_tree_classifier": DecisionTreeClassifier,
    "decision_tree_regressor": DecisionTreeRegressor,
}
CLASSIFIERS = {"random_forest_classifier", "logistic_regression", "decision_tree_classifier"}

# ── App ───────────────────────────────────────────────────────────────────
app = FastAPI(title="AnalyticCore ML Service", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Helpers ───────────────────────────────────────────────────────────────
def _read_tabular(data: bytes, filename: str = "") -> pd.DataFrame:
    """Parse CSV or Excel bytes into a DataFrame."""
    name = (filename or "").lower()
    try:
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(data))
        return pd.read_csv(io.BytesIO(data))
    except Exception:
        # fallback: try the other format
        try:
            return pd.read_excel(io.BytesIO(data))
        except Exception as exc:
            raise HTTPException(400, f"Could not parse file as CSV or Excel: {exc}")


def _infer_problem_type(y: pd.Series, forced: Optional[str]) -> str:
    if forced in ("classification", "regression"):
        return forced
    if y.dtype == "object" or y.dtype.name == "category":
        return "classification"
    nunique = y.nunique(dropna=True)
    if nunique <= 10 and y.dtype.kind in ("i", "u", "b"):
        return "classification"
    return "regression"


def _build_pipeline(algorithm: str, X: pd.DataFrame) -> Pipeline:
    if algorithm not in ALLOWED_ALGORITHMS:
        raise HTTPException(400, f"Unsupported algorithm '{algorithm}'.")

    numeric_cols = X.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = [c for c in X.columns if c not in numeric_cols]

    transformers = []
    if numeric_cols:
        transformers.append(("num", StandardScaler(with_mean=False), numeric_cols))
    if categorical_cols:
        transformers.append((
            "cat",
            OneHotEncoder(handle_unknown="ignore", sparse_output=False),
            categorical_cols,
        ))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")

    model_cls = ALLOWED_ALGORITHMS[algorithm]
    if algorithm in ("random_forest_classifier", "random_forest_regressor"):
        model = model_cls(n_estimators=100, random_state=42, n_jobs=-1)
    elif algorithm == "logistic_regression":
        model = model_cls(max_iter=1000, n_jobs=-1)
    else:
        model = model_cls(random_state=42) if "random_state" in model_cls().get_params() else model_cls()

    return Pipeline([("preprocessor", preprocessor), ("model", model)])


def _compute_metrics(problem_type: str, y_true, y_pred) -> Dict[str, float]:
    if problem_type == "classification":
        try:
            return {
                "accuracy": float(accuracy_score(y_true, y_pred)),
                "f1_weighted": float(f1_score(y_true, y_pred, average="weighted", zero_division=0)),
            }
        except Exception:
            return {"accuracy": float(accuracy_score(y_true, y_pred))}
    # regression
    return {
        "r2": float(r2_score(y_true, y_pred)),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
    }


def _model_path(model_id: str) -> str:
    return os.path.join(MODELS_DIR, f"{model_id}.joblib")


def _meta_path(model_id: str) -> str:
    return os.path.join(MODELS_DIR, f"{model_id}.json")


# ── Models ────────────────────────────────────────────────────────────────
class TrainResponse(BaseModel):
    model_id: str
    algorithm: str
    problem_type: str
    target_column: str
    feature_columns: List[str]
    metrics: Dict[str, float]
    created_at: str
    sample_size: int


class PredictResponse(BaseModel):
    model_id: str
    row_count: int
    predictions: List[Any]
    probabilities: Optional[List[List[float]]] = None


# ── Routes ────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "models_dir": MODELS_DIR, "timestamp": datetime.utcnow().isoformat()}


@app.get("/algorithms")
def list_algorithms():
    return {
        "algorithms": [
            {"id": k, "type": "classification" if k in CLASSIFIERS else "regression"}
            for k in ALLOWED_ALGORITHMS
        ]
    }


@app.post("/train", response_model=TrainResponse)
async def train(
    file: UploadFile = File(...),
    target_column: str = Form(...),
    algorithm: str = Form("random_forest_classifier"),
    feature_columns: Optional[str] = Form(None),  # JSON array string
    problem_type: Optional[str] = Form(None),  # 'classification' | 'regression'
    test_size: float = Form(0.2),
    model_id: Optional[str] = Form(None),
):
    raw = await file.read()
    df = _read_tabular(raw, file.filename or "")
    if df.empty:
        raise HTTPException(400, "Uploaded file contains no rows.")
    if target_column not in df.columns:
        raise HTTPException(400, f"Target column '{target_column}' not found in data.")

    df = df.dropna(subset=[target_column])
    if df.empty:
        raise HTTPException(400, "No rows remain after dropping NaN target values.")

    # Feature selection
    if feature_columns:
        try:
            feats = json.loads(feature_columns)
            assert isinstance(feats, list) and all(isinstance(c, str) for c in feats)
        except Exception:
            raise HTTPException(400, "feature_columns must be a JSON array of strings.")
        missing = [c for c in feats if c not in df.columns]
        if missing:
            raise HTTPException(400, f"Feature columns not in data: {missing}")
    else:
        feats = [c for c in df.columns if c != target_column]

    X = df[feats].copy()
    y = df[target_column].copy()

    # Fill simple NaNs so sklearn doesn't fail
    for c in X.columns:
        if X[c].dtype.kind in ("i", "u", "f"):
            X[c] = X[c].fillna(X[c].median())
        else:
            X[c] = X[c].fillna("__missing__").astype(str)

    ptype = _infer_problem_type(y, problem_type)

    # Auto-correct algorithm/problem-type mismatch
    # (e.g. user picked a classifier but target is numeric => switch to the regressor counterpart)
    _counterpart = {
        "random_forest_classifier": "random_forest_regressor",
        "random_forest_regressor": "random_forest_classifier",
        "logistic_regression": "linear_regression",
        "linear_regression": "logistic_regression",
        "decision_tree_classifier": "decision_tree_regressor",
        "decision_tree_regressor": "decision_tree_classifier",
    }
    algo_is_classifier = algorithm in CLASSIFIERS
    if (ptype == "classification") != algo_is_classifier:
        algorithm = _counterpart.get(algorithm, algorithm)

    pipeline = _build_pipeline(algorithm, X)

    # Stratify for classification if possible
    stratify = y if (ptype == "classification" and y.nunique() > 1 and (y.value_counts().min() >= 2)) else None
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=stratify
        )
    except ValueError:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    metrics = _compute_metrics(ptype, y_test, y_pred)

    mid = model_id or str(uuid.uuid4())
    joblib.dump(pipeline, _model_path(mid))

    meta = {
        "model_id": mid,
        "algorithm": algorithm,
        "problem_type": ptype,
        "target_column": target_column,
        "feature_columns": feats,
        "metrics": metrics,
        "sample_size": int(len(df)),
        "created_at": datetime.utcnow().isoformat(),
    }
    with open(_meta_path(mid), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return TrainResponse(**meta)


@app.post("/predict", response_model=PredictResponse)
async def predict(
    file: UploadFile = File(...),
    model_id: str = Form(...),
    include_probabilities: bool = Form(False),
):
    mp = _model_path(model_id)
    if not os.path.exists(mp):
        raise HTTPException(404, f"Model '{model_id}' not found.")
    pipeline = joblib.load(mp)

    raw = await file.read()
    df = _read_tabular(raw, file.filename or "")
    if df.empty:
        raise HTTPException(400, "Uploaded file contains no rows.")

    # Load metadata to get expected feature columns
    meta_p = _meta_path(model_id)
    if os.path.exists(meta_p):
        with open(meta_p, "r", encoding="utf-8") as f:
            meta = json.load(f)
        feats = meta.get("feature_columns", list(df.columns))
        missing = [c for c in feats if c not in df.columns]
        if missing:
            raise HTTPException(
                400,
                f"Prediction file missing required columns: {missing}. "
                f"File contains: {list(df.columns)}. "
                f"Model expects: {feats}.",
            )
        X = df[feats].copy()
    else:
        X = df.copy()

    # Fill NaNs similar to training
    for c in X.columns:
        if X[c].dtype.kind in ("i", "u", "f"):
            X[c] = X[c].fillna(X[c].median() if X[c].notna().any() else 0)
        else:
            X[c] = X[c].fillna("__missing__").astype(str)

    preds = pipeline.predict(X).tolist()
    probs = None
    if include_probabilities and hasattr(pipeline.named_steps.get("model"), "predict_proba"):
        try:
            probs = pipeline.predict_proba(X).tolist()
        except Exception:
            probs = None

    return PredictResponse(
        model_id=model_id,
        row_count=len(preds),
        predictions=preds,
        probabilities=probs,
    )


@app.get("/models/{model_id}")
def get_model_info(model_id: str):
    meta_p = _meta_path(model_id)
    if not os.path.exists(meta_p):
        raise HTTPException(404, f"Model '{model_id}' not found.")
    with open(meta_p, "r", encoding="utf-8") as f:
        return json.load(f)


@app.delete("/models/{model_id}")
def delete_model(model_id: str):
    removed = False
    for p in (_model_path(model_id), _meta_path(model_id)):
        if os.path.exists(p):
            os.remove(p)
            removed = True
    if not removed:
        raise HTTPException(404, f"Model '{model_id}' not found.")
    return {"ok": True, "model_id": model_id}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
