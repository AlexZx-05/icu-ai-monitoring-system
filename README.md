# ICU Command Center

![ICU Command Center Dashboard](docs/assets/icu-command-center.png)

Advanced ICU monitoring platform with:

- realtime risk triage dashboard
- live clinical alerts
- patient drill-down with trends
- backend notification engine (email alerts with cooldown/dedup)
- ML risk scoring pipeline

## Tech Stack

- Backend: FastAPI (`backend/app/main.py`)
- Frontend: HTML/CSS/JS (`frontend/`)
- ML Pipeline: Python + pandas + scikit-learn (`ml/`)

## What This Project Does

1. Ingests ICU vitals/lab streams.
2. Computes risk probability and risk tier per patient.
3. Surfaces critical/high-risk patients in a triage board.
4. Streams alert feed and patient detail timeline.
5. Sends automatic email alerts based on configurable thresholds.

## Run

```bash
cd backend
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open:

- `http://127.0.0.1:8000/`

## Dataset Migration

MIMIC-IV + eICU migration guide:

- [Migration Plan](docs/mimic4_eicu_migration.md)

## Screenshot Path

Put your screenshot at:

- `docs/assets/icu-command-center.png`

So GitHub will render it at the top of this README.
