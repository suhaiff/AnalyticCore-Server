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
from pydantic import BaseModel, ConfigDict

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, VotingRegressor
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


def _smart_preprocess(X: pd.DataFrame) -> pd.DataFrame:
    """
    Smart preprocessing that handles common data issues:
    1. Converts numeric-looking string columns to actual numbers
    2. Converts date-like columns to useful numeric features (year, month, day, etc.)
    3. Drops high-cardinality categorical columns that would produce useless one-hot features
    """
    X = X.copy()
    cols_to_drop = []
    cols_to_add = {}

    for col in X.columns:
        # 1. Try to convert string columns to numeric
        if X[col].dtype == "object":
            try:
                converted = pd.to_numeric(X[col], errors="coerce")
                # If at least 80% of values converted successfully, treat as numeric
                if converted.notna().mean() >= 0.8:
                    X[col] = converted
                    continue
            except Exception:
                pass

        # 2. Try to detect and convert date-like columns
        if X[col].dtype == "object":
            col_lower = col.lower().strip()
            # Check column name for date hints
            date_hints = ["date", "time", "timestamp", "datetime", "period", "day", "month", "year"]
            is_date_name = any(hint in col_lower for hint in date_hints)

            # Also try parsing values to see if they look like dates
            is_date_values = False
            if not is_date_name:
                try:
                    sample = X[col].dropna().head(5)
                    if len(sample) > 0:
                        parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
                        if parsed.notna().mean() >= 0.8:
                            is_date_values = True
                except Exception:
                    pass

            if is_date_name or is_date_values:
                try:
                    dt = pd.to_datetime(X[col], errors="coerce", infer_datetime_format=True)
                    if dt.notna().mean() >= 0.5:
                        # Extract useful numeric features from date
                        cols_to_add[f"{col}_year"] = dt.dt.year.fillna(0).astype(float)
                        cols_to_add[f"{col}_month"] = dt.dt.month.fillna(0).astype(float)
                        cols_to_add[f"{col}_day"] = dt.dt.day.fillna(0).astype(float)
                        cols_to_add[f"{col}_dayofweek"] = dt.dt.dayofweek.fillna(0).astype(float)
                        cols_to_add[f"{col}_dayofyear"] = dt.dt.dayofyear.fillna(0).astype(float)
                        # Convert to ordinal (days since epoch) for continuous value
                        epoch = pd.Timestamp("1970-01-01")
                        cols_to_add[f"{col}_ordinal"] = (dt - epoch).dt.days.fillna(0).astype(float)
                        cols_to_drop.append(col)
                        continue
                except Exception:
                    pass

        # 3. Drop high-cardinality categorical columns (would produce useless one-hot features)
        if X[col].dtype == "object":
            n_unique = X[col].nunique(dropna=True)
            n_rows = len(X)
            # If more than 50% of values are unique, it's essentially an ID/text column
            if n_rows > 0 and n_unique / n_rows > 0.5 and n_unique > 20:
                cols_to_drop.append(col)

    # Apply changes
    if cols_to_drop:
        X = X.drop(columns=cols_to_drop, errors="ignore")
    for new_col, values in cols_to_add.items():
        X[new_col] = values

    return X


def _infer_problem_type(y: pd.Series, forced: Optional[str]) -> str:
    # Hard cap: classification on a continuous numeric target with many unique
    # values creates unusably-large models and almost never reflects user intent.
    # Override "classification" to "regression" when the target is numeric
    # with high cardinality, regardless of what the caller requested.
    if y.dtype.kind in ("i", "u", "f") and y.nunique(dropna=True) > 50:
        return "regression"
    if forced in ("classification", "regression"):
        return forced
    if y.dtype == "object" or y.dtype.name == "category":
        return "classification"
    nunique = y.nunique(dropna=True)
    if nunique <= 10 and y.dtype.kind in ("i", "u", "b"):
        return "classification"
    return "regression"


def _build_pipeline(algorithm: str, X: pd.DataFrame, problem_type: str = "regression") -> Pipeline:
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

    # For tree-based regression, blend with LinearRegression via VotingRegressor.
    # Tree models cannot extrapolate beyond their training data range — when
    # prediction inputs exceed training values, all rows land in the same leaf
    # node and produce identical predictions.  A LinearRegression component
    # provides the extrapolation capability while the tree captures non-linear
    # patterns within range.
    _tree_regressors = {"random_forest_regressor", "decision_tree_regressor"}
    if problem_type == "regression" and algorithm in _tree_regressors:
        lr = LinearRegression()
        ensemble = VotingRegressor(
            estimators=[("tree", model), ("lr", lr)],
            weights=[0.6, 0.4],
        )
        return Pipeline([("preprocessor", preprocessor), ("model", ensemble)])

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
    model_config = ConfigDict(protected_namespaces=())
    model_id: str
    algorithm: str
    problem_type: str
    target_column: str
    feature_columns: List[str]
    metrics: Dict[str, float]
    created_at: str
    sample_size: int
    updated_at: Optional[str] = None
    name: Optional[str] = None


class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    model_id: str
    row_count: int
    predictions: List[Any]
    probabilities: Optional[List[List[float]]] = None
    input_data: List[Dict[str, Any]]


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
    name: Optional[str] = Form(None),
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

    # Save original (raw) feature columns before preprocessing
    raw_feats = list(feats)

    # Smart preprocessing: handle dates, numeric coercion, high-cardinality drops
    X = _smart_preprocess(X)
    # Update feature list to reflect preprocessed columns
    feats = list(X.columns)

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

    pipeline = _build_pipeline(algorithm, X, problem_type=ptype)

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

    # Check if this is a retrain (model_id already exists) — read before overwriting
    existing_meta = None
    if model_id and os.path.exists(_meta_path(mid)):
        try:
            with open(_meta_path(mid), "r", encoding="utf-8") as f:
                existing_meta = json.load(f)
        except Exception:
            existing_meta = None

    joblib.dump(pipeline, _model_path(mid))

    now_iso = datetime.utcnow().isoformat()

    meta = {
        "model_id": mid,
        "algorithm": algorithm,
        "problem_type": ptype,
        "target_column": target_column,
        "feature_columns": feats,
        "raw_feature_columns": raw_feats,
        "metrics": metrics,
        "sample_size": int(len(df)) + (existing_meta.get("sample_size", 0) if existing_meta else 0),
        "created_at": existing_meta.get("created_at", now_iso) if existing_meta else now_iso,
        "updated_at": now_iso,
    }
    # Preserve the model name
    if name:
        meta["name"] = name
    elif existing_meta and existing_meta.get("name"):
        meta["name"] = existing_meta["name"]

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

        # raw_feature_columns = original columns before preprocessing
        # feature_columns = preprocessed columns the model actually uses
        raw_feats = meta.get("raw_feature_columns", meta.get("feature_columns", list(df.columns)))
        processed_feats = meta.get("feature_columns", raw_feats)

        # Check that the raw columns exist in the prediction file
        available_raw = [c for c in raw_feats if c in df.columns]
        if not available_raw:
            raise HTTPException(
                400,
                f"Prediction file missing required columns. "
                f"File contains: {list(df.columns)}. "
                f"Model expects: {raw_feats}.",
            )

        X = df[available_raw].copy()

        # Apply same smart preprocessing as training
        X = _smart_preprocess(X)

        # Ensure all expected processed columns exist (add missing ones as 0)
        for col in processed_feats:
            if col not in X.columns:
                X[col] = 0.0

        # Keep only the columns the model expects, in the right order
        X = X[[c for c in processed_feats if c in X.columns]]
    else:
        X = df.copy()
        X = _smart_preprocess(X)

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

    # Replace NaNs with empty string for JSON serialization
    input_data = df.fillna("").to_dict(orient="records")

    return PredictResponse(
        model_id=model_id,
        row_count=len(preds),
        predictions=preds,
        probabilities=probs,
        input_data=input_data,
    )


@app.get("/models")
def list_models():
    """List all available models for optional userId filtering."""
    try:
        models = []
        for filename in os.listdir(MODELS_DIR):
            if filename.endswith(".json"):
                model_id = filename[:-5]  # Remove .json extension
                meta_p = _meta_path(model_id)
                try:
                    with open(meta_p, "r", encoding="utf-8") as f:
                        model_data = json.load(f)
                        models.append(model_data)
                except Exception as e:
                    print(f"Error reading model {model_id}: {e}")
        return models
    except Exception as e:
        raise HTTPException(500, f"Failed to list models: {str(e)}")


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
