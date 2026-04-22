# AnalyticCore ML Service

A lightweight Python FastAPI microservice that powers the prediction
modelling feature of AnalyticCore. It trains scikit-learn models from
uploaded CSV/Excel data and serves predictions for new records.

The main Node.js backend (`../server`) proxies user-facing HTTP requests
to this service over the network.

## Requirements

- Python 3.9+
- pip

## Setup (Windows / PowerShell)

```powershell
cd ml-service
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Running locally

```powershell
# from inside the virtual environment
python main.py
# → listens on http://localhost:8001
```

Or with uvicorn directly for hot-reload:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

## Environment variables

| Variable     | Default                  | Description                          |
|--------------|--------------------------|--------------------------------------|
| `PORT`       | `8001`                   | Port the service listens on          |
| `MODELS_DIR` | `./models_store`         | Where trained `.joblib` files are kept |

## Endpoints

| Method | Path                 | Description                                 |
|--------|----------------------|---------------------------------------------|
| GET    | `/health`            | Service heartbeat                           |
| GET    | `/algorithms`        | List supported algorithms                   |
| POST   | `/train`             | Train a new model from uploaded file        |
| POST   | `/predict`           | Run predictions on uploaded file            |
| GET    | `/models/{id}`       | Fetch metadata for a trained model          |
| DELETE | `/models/{id}`       | Delete a trained model                      |

All requests accept `multipart/form-data` for file uploads.

## Supported algorithms

- `random_forest_classifier`, `random_forest_regressor`
- `logistic_regression`, `linear_regression`
- `decision_tree_classifier`, `decision_tree_regressor`

## Integration with Node backend

The Node.js server (`../server/index.js`) calls this service using
`ML_SERVICE_URL` (defaults to `http://localhost:8001`). Set this in
`../server/.env` when deploying:

```
ML_SERVICE_URL=http://your-ml-host:8001
```
