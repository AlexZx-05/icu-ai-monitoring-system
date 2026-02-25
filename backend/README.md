# ICU Intelligence Backend

## Run

1. `cd backend`
2. `python -m venv .venv`
3. `.venv\Scripts\activate`
4. `pip install -r requirements.txt`
5. `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`

## API

- `GET /api/health`
- `GET /api/summary`
- `GET /api/patients?risk=critical|high|medium|low&search=&limit=150`
- `GET /api/patients/{subject_id}`
- `GET /api/alerts/live`
- `GET /api/notifications/status`
- `POST /api/reload`
- `WS /ws/alerts`

## Email Alerting

Backend now sends emails automatically from the running FastAPI service.

Environment variables (loaded from project `.env`):

- `EMAIL_USER` (SMTP sender)
- `EMAIL_PASS` (SMTP app password)
- `EMAIL_TO` (comma-separated recipients)
- `ENABLE_EMAIL_ALERTS=true|false` (default `true`)
- `ALERT_MINIMUM_TIER=critical|high|medium|low` (default `critical`)
- `ALERT_MINIMUM_PROBABILITY=0.85` (default `0.85`)
- `ALERT_COOLDOWN_MINUTES=30` (default `30`)
- `ALERT_SCAN_INTERVAL_SECONDS=20` (default `20`)
