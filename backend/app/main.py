from __future__ import annotations

import asyncio
import os
import smtplib
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
DATA_ROOT = PROJECT_ROOT / "data" / "mimic-iii-clinical-database-demo-1.4"
FULL_DATA_PATH = DATA_ROOT / "full_medical_data_clean.csv"
ALERTS_PATH = DATA_ROOT / "patient_alerts.csv"
MODEL_PATH = DATA_ROOT / "risk_model_v2.pkl"
FRONTEND_ROOT = PROJECT_ROOT / "frontend"

DEFAULT_COLUMNS = [
    "subject_id",
    "charttime",
    "heart_rate",
    "bp_mean",
    "spo2",
    "temp",
    "creatinine",
    "lactate",
    "wbc",
]

SAFE_BOUNDS = {
    "heart_rate": (35.0, 190.0),
    "bp_mean": (40.0, 135.0),
    "spo2": (72.0, 100.0),
    "temp": (34.0, 41.0),
    "creatinine": (0.3, 8.0),
    "lactate": (0.3, 9.5),
    "wbc": (1.0, 35.0),
}


def clean_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df))
    series = pd.to_numeric(df[col], errors="coerce")
    lo, hi = SAFE_BOUNDS.get(col, (-np.inf, np.inf))
    return series.clip(lower=lo, upper=hi)


def normalize_risk(probability: float) -> str:
    if probability >= 0.86:
        return "critical"
    if probability >= 0.7:
        return "high"
    if probability >= 0.4:
        return "medium"
    return "low"


@dataclass
class Snapshot:
    last_refreshed: str
    summary: dict[str, Any]
    rows: list[dict[str, Any]]
    by_id: dict[int, dict[str, Any]]
    timeline: dict[int, list[dict[str, Any]]]
    alerts: list[dict[str, Any]]


@dataclass
class NotificationEvent:
    timestamp: str
    subject_id: int
    risk_tier: str
    risk_probability: float
    status: str
    message: str


class ICURepository:
    def __init__(self) -> None:
        self.model = self._load_model()
        self.snapshot: Snapshot | None = None

    @staticmethod
    def _load_model() -> Any | None:
        if MODEL_PATH.exists():
            try:
                return joblib.load(MODEL_PATH)
            except Exception:
                return None
        return None

    @staticmethod
    def _load_frame(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Expected file was not found: {path}")
        return pd.read_csv(path)

    def _feature_frame(self, g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("charttime")
        last = g.tail(3)
        row = {
            "hr_avg": float(last["heart_rate"].mean()),
            "bp_avg": float(last["bp_mean"].mean()),
            "spo2_avg": float(last["spo2"].mean()),
            "temp_avg": float(last["temp"].mean()),
            "hr_trend": float(last["heart_rate"].iloc[-1] - last["heart_rate"].iloc[-2]),
            "creatinine": float(last["creatinine"].iloc[-1]),
            "lactate": float(last["lactate"].iloc[-1]),
            "wbc": float(last["wbc"].iloc[-1]),
            "spo2_missing": int(pd.isna(last["spo2"].iloc[-1])),
            "temp_missing": int(pd.isna(last["temp"].iloc[-1])),
        }
        return pd.DataFrame([row])

    def _hybrid_risk(self, features: pd.DataFrame, latest: pd.Series) -> float:
        ml_prob = None
        if self.model is not None:
            try:
                ml_prob = float(self.model.predict_proba(features)[0][1])
            except Exception:
                ml_prob = None

        rule_penalty = 0.0
        hr = latest["heart_rate"]
        bp = latest["bp_mean"]
        spo2 = latest["spo2"]
        lac = latest["lactate"]

        if pd.notna(hr):
            if hr > 120:
                rule_penalty += 0.2
            elif hr < 50:
                rule_penalty += 0.15
        if pd.notna(bp):
            if bp < 60:
                rule_penalty += 0.25
            elif bp > 110:
                rule_penalty += 0.1
        if pd.notna(spo2) and spo2 < 90:
            rule_penalty += 0.2
        if pd.notna(lac) and lac > 2.2:
            rule_penalty += 0.15

        rule_prob = min(0.98, max(0.02, 0.12 + rule_penalty))
        if ml_prob is None:
            return rule_prob
        return float(np.clip((0.68 * ml_prob) + (0.32 * rule_prob), 0.01, 0.99))

    def build_snapshot(self) -> Snapshot:
        df = self._load_frame(FULL_DATA_PATH)
        for col in DEFAULT_COLUMNS:
            if col not in df.columns:
                df[col] = np.nan

        df["charttime"] = pd.to_datetime(df["charttime"], errors="coerce")
        df = df.dropna(subset=["subject_id", "charttime"])
        df["subject_id"] = pd.to_numeric(df["subject_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["subject_id"])
        df["subject_id"] = df["subject_id"].astype(int)

        for col in ["heart_rate", "bp_mean", "spo2", "temp", "creatinine", "lactate", "wbc"]:
            df[col] = clean_numeric(df, col)

        timeline: dict[int, list[dict[str, Any]]] = {}
        rows: list[dict[str, Any]] = []

        grouped = df.sort_values(["subject_id", "charttime"]).groupby("subject_id")
        for subject_id, g in grouped:
            if len(g) < 3:
                continue

            latest = g.iloc[-1]
            features = self._feature_frame(g)
            risk_prob = self._hybrid_risk(features, latest)
            risk_tier = normalize_risk(risk_prob)
            trend = float(features["hr_trend"].iloc[0])

            reasons = []
            if pd.notna(latest["heart_rate"]) and latest["heart_rate"] > 120:
                reasons.append("tachycardia")
            if pd.notna(latest["bp_mean"]) and latest["bp_mean"] < 60:
                reasons.append("hypotension")
            if pd.notna(latest["spo2"]) and latest["spo2"] < 90:
                reasons.append("hypoxemia")
            if pd.notna(latest["lactate"]) and latest["lactate"] > 2.2:
                reasons.append("elevated lactate")
            if not reasons:
                reasons.append("monitoring")

            row = {
                "subject_id": subject_id,
                "updated_at": latest["charttime"].isoformat(),
                "risk_probability": round(risk_prob, 4),
                "risk_tier": risk_tier,
                "risk_reasons": reasons,
                "heart_rate": None if pd.isna(latest["heart_rate"]) else round(float(latest["heart_rate"]), 1),
                "bp_mean": None if pd.isna(latest["bp_mean"]) else round(float(latest["bp_mean"]), 1),
                "spo2": None if pd.isna(latest["spo2"]) else round(float(latest["spo2"]), 1),
                "temp": None if pd.isna(latest["temp"]) else round(float(latest["temp"]), 1),
                "creatinine": None if pd.isna(latest["creatinine"]) else round(float(latest["creatinine"]), 2),
                "lactate": None if pd.isna(latest["lactate"]) else round(float(latest["lactate"]), 2),
                "wbc": None if pd.isna(latest["wbc"]) else round(float(latest["wbc"]), 2),
                "heart_rate_trend": round(trend, 2),
            }
            rows.append(row)

            # Keep small timeline payload for efficient frontend rendering.
            tail = g.tail(12)
            timeline[subject_id] = [
                {
                    "charttime": t.isoformat(),
                    "heart_rate": None if pd.isna(hr) else round(float(hr), 1),
                    "bp_mean": None if pd.isna(bp) else round(float(bp), 1),
                    "spo2": None if pd.isna(spo2) else round(float(spo2), 1),
                    "temp": None if pd.isna(temp) else round(float(temp), 1),
                }
                for t, hr, bp, spo2, temp in zip(
                    tail["charttime"],
                    tail["heart_rate"],
                    tail["bp_mean"],
                    tail["spo2"],
                    tail["temp"],
                )
            ]

        rows.sort(key=lambda x: x["risk_probability"], reverse=True)
        by_id = {r["subject_id"]: r for r in rows}

        critical = sum(1 for r in rows if r["risk_tier"] == "critical")
        high = sum(1 for r in rows if r["risk_tier"] == "high")
        medium = sum(1 for r in rows if r["risk_tier"] == "medium")
        low = sum(1 for r in rows if r["risk_tier"] == "low")
        avg_risk = round(float(np.mean([r["risk_probability"] for r in rows])) if rows else 0.0, 4)

        alerts = []
        if ALERTS_PATH.exists():
            try:
                alerts_df = pd.read_csv(ALERTS_PATH)
                for _, row in alerts_df.tail(60).iterrows():
                    sid = int(row.get("subject_id")) if pd.notna(row.get("subject_id")) else None
                    if sid is None:
                        continue
                    alerts.append(
                        {
                            "subject_id": sid,
                            "charttime": str(row.get("charttime", "")),
                            "alert": str(row.get("alert", "Clinical alert")).strip() or "Clinical alert",
                            "risk_tier": by_id.get(sid, {}).get("risk_tier", "medium"),
                            "alert_heart_rate": None
                            if pd.isna(row.get("heart_rate"))
                            else float(row.get("heart_rate")),
                            "alert_bp_mean": None
                            if pd.isna(row.get("bp_mean"))
                            else float(row.get("bp_mean")),
                        }
                    )
            except Exception:
                alerts = []

        summary = {
            "patients_monitored": len(rows),
            "critical_count": critical,
            "high_count": high,
            "medium_count": medium,
            "low_count": low,
            "average_risk": avg_risk,
        }

        return Snapshot(
            last_refreshed=datetime.now(UTC).isoformat(),
            summary=summary,
            rows=rows,
            by_id=by_id,
            timeline=timeline,
            alerts=alerts,
        )

    def get_snapshot(self, force: bool = False) -> Snapshot:
        if force or self.snapshot is None:
            self.snapshot = self.build_snapshot()
        return self.snapshot


class NotificationEngine:
    def __init__(self) -> None:
        self.enabled = os.getenv("ENABLE_EMAIL_ALERTS", "true").lower() == "true"
        self.sender = os.getenv("EMAIL_USER")
        self.password = os.getenv("EMAIL_PASS")
        self.recipients = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]
        self.cooldown_minutes = int(os.getenv("ALERT_COOLDOWN_MINUTES", "30"))
        self.minimum_tier = os.getenv("ALERT_MINIMUM_TIER", "critical").lower()
        self.minimum_prob = float(os.getenv("ALERT_MINIMUM_PROBABILITY", "0.85"))
        self.last_sent: dict[int, datetime] = {}
        self.history: deque[NotificationEvent] = deque(maxlen=300)

    def _tier_order(self, tier: str) -> int:
        return {"low": 0, "medium": 1, "high": 2, "critical": 3}.get(tier, 0)

    def _eligible(self, row: dict[str, Any]) -> bool:
        if self._tier_order(row["risk_tier"]) < self._tier_order(self.minimum_tier):
            return False
        return float(row["risk_probability"]) >= self.minimum_prob

    def _cooldown_over(self, subject_id: int, now: datetime) -> bool:
        last = self.last_sent.get(subject_id)
        if last is None:
            return True
        mins = (now - last).total_seconds() / 60
        return mins >= self.cooldown_minutes

    def _record(self, event: NotificationEvent) -> None:
        self.history.appendleft(event)

    def _build_message(self, row: dict[str, Any]) -> MIMEMultipart:
        risk_pct = round(float(row["risk_probability"]) * 100, 1)
        msg = MIMEMultipart()
        msg["From"] = self.sender or ""
        msg["To"] = ", ".join(self.recipients)
        msg["Subject"] = f"ICU ALERT: Patient #{row['subject_id']} {row['risk_tier'].upper()} risk ({risk_pct}%)"

        body = f"""ICU Command Center Alert

Patient ID: {row['subject_id']}
Risk Tier: {row['risk_tier'].upper()}
Risk Probability: {risk_pct}%
Updated At: {row['updated_at']}

Vitals
- Heart Rate: {row.get('heart_rate')}
- MAP: {row.get('bp_mean')}
- SpO2: {row.get('spo2')}
- Temp: {row.get('temp')}
- Lactate: {row.get('lactate')}

Reasons: {", ".join(row.get("risk_reasons", []))}

This alert was generated automatically by ICU Intelligence.
"""
        msg.attach(MIMEText(body, "plain"))
        return msg

    def _send_email(self, row: dict[str, Any], now: datetime) -> NotificationEvent:
        if not self.enabled:
            return NotificationEvent(
                timestamp=now.isoformat(),
                subject_id=row["subject_id"],
                risk_tier=row["risk_tier"],
                risk_probability=float(row["risk_probability"]),
                status="skipped",
                message="Email alerts disabled by config",
            )

        if not self.sender or not self.password or not self.recipients:
            return NotificationEvent(
                timestamp=now.isoformat(),
                subject_id=row["subject_id"],
                risk_tier=row["risk_tier"],
                risk_probability=float(row["risk_probability"]),
                status="error",
                message="Missing EMAIL_USER/EMAIL_PASS/EMAIL_TO configuration",
            )

        msg = self._build_message(row)
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
            server.starttls()
            server.login(self.sender, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())
            server.quit()
            self.last_sent[row["subject_id"]] = now
            return NotificationEvent(
                timestamp=now.isoformat(),
                subject_id=row["subject_id"],
                risk_tier=row["risk_tier"],
                risk_probability=float(row["risk_probability"]),
                status="sent",
                message=f"Email sent to {', '.join(self.recipients)}",
            )
        except Exception as exc:
            return NotificationEvent(
                timestamp=now.isoformat(),
                subject_id=row["subject_id"],
                risk_tier=row["risk_tier"],
                risk_probability=float(row["risk_probability"]),
                status="error",
                message=f"Email failed: {exc}",
            )

    async def process_snapshot(self, snap: Snapshot) -> None:
        now = datetime.now(UTC)
        for row in snap.rows:
            if not self._eligible(row):
                continue
            if not self._cooldown_over(row["subject_id"], now):
                continue
            event = await asyncio.to_thread(self._send_email, row, now)
            self._record(event)

    def summary(self) -> dict[str, Any]:
        recent = list(self.history)[:20]
        sent_count = sum(1 for e in self.history if e.status == "sent")
        err_count = sum(1 for e in self.history if e.status == "error")
        return {
            "enabled": self.enabled,
            "minimum_tier": self.minimum_tier,
            "minimum_probability": self.minimum_prob,
            "cooldown_minutes": self.cooldown_minutes,
            "sent_count": sent_count,
            "error_count": err_count,
            "recent": [e.__dict__ for e in recent],
        }


repo = ICURepository()
notifier = NotificationEngine()
app = FastAPI(title="ICU Intelligence API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_ROOT.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_ROOT)), name="assets")


async def monitor_and_notify() -> None:
    interval_seconds = int(os.getenv("ALERT_SCAN_INTERVAL_SECONDS", "20"))
    while True:
        try:
            snap = repo.get_snapshot(force=True)
            await notifier.process_snapshot(snap)
        except Exception:
            # Keep monitor running even if one cycle fails.
            pass
        await asyncio.sleep(interval_seconds)


@app.on_event("startup")
async def startup_monitor() -> None:
    app.state.monitor_task = asyncio.create_task(monitor_and_notify())


@app.on_event("shutdown")
async def shutdown_monitor() -> None:
    task = getattr(app.state, "monitor_task", None)
    if task:
        task.cancel()


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "time": datetime.now(UTC).isoformat()}


@app.get("/api/summary")
def summary() -> dict[str, Any]:
    snap = repo.get_snapshot()
    return {"last_refreshed": snap.last_refreshed, "summary": snap.summary}


@app.get("/api/patients")
def list_patients(
    risk: str | None = Query(default=None, pattern="^(critical|high|medium|low)$"),
    search: str | None = None,
    limit: int = Query(default=120, ge=1, le=500),
) -> dict[str, Any]:
    snap = repo.get_snapshot()
    rows = snap.rows

    if risk:
        rows = [r for r in rows if r["risk_tier"] == risk]

    if search:
        token = search.strip().lower()
        rows = [
            r
            for r in rows
            if token in str(r["subject_id"]).lower()
            or any(token in reason.lower() for reason in r["risk_reasons"])
        ]

    return {"count": len(rows), "items": rows[:limit]}


@app.get("/api/patients/{subject_id}")
def patient_detail(subject_id: int) -> dict[str, Any]:
    snap = repo.get_snapshot()
    patient = snap.by_id.get(subject_id)
    if patient is None:
        raise HTTPException(status_code=404, detail="Patient not found")

    return {
        "patient": patient,
        "timeline": snap.timeline.get(subject_id, []),
    }


@app.get("/api/alerts/live")
def live_alerts(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    snap = repo.get_snapshot()
    alerts = snap.alerts[-limit:]
    alerts.reverse()
    return {"count": len(alerts), "items": alerts}


@app.post("/api/reload")
def reload_data() -> dict[str, Any]:
    snap = repo.get_snapshot(force=True)
    return {"status": "reloaded", "last_refreshed": snap.last_refreshed}


@app.get("/api/notifications/status")
def notifications_status() -> dict[str, Any]:
    return notifier.summary()


@app.websocket("/ws/alerts")
async def alerts_ws(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            snap = repo.get_snapshot()
            payload = {
                "timestamp": datetime.now(UTC).isoformat(),
                "summary": snap.summary,
                "top_alerts": snap.rows[:10],
            }
            await ws.send_json(payload)
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        return


@app.get("/")
def home() -> FileResponse:
    index_file = FRONTEND_ROOT / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Frontend not found")
    return FileResponse(index_file)


@app.get("/{file_path:path}")
def frontend_files(file_path: str) -> FileResponse:
    if file_path.startswith("api/") or file_path.startswith("ws/"):
        raise HTTPException(status_code=404, detail="Not found")

    target = FRONTEND_ROOT / file_path
    if target.exists() and target.is_file():
        return FileResponse(target)

    # SPA fallback for deep links.
    index_file = FRONTEND_ROOT / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not found")
