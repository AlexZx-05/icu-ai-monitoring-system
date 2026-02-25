import os
from pathlib import Path

import pandas as pd


BASE_PATH = Path("data/mimic-iii-clinical-database-demo-1.4")
OUTPUT_PATH = BASE_PATH / "icu_detailed_data.csv"


def _safe_read(name: str) -> pd.DataFrame:
    path = BASE_PATH / name
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def build_diagnosis_summary() -> pd.DataFrame:
    diagnoses = _safe_read("DIAGNOSES_ICD.csv")
    icd_map = _safe_read("D_ICD_DIAGNOSES.csv")[["icd9_code", "short_title"]]

    diagnoses["seq_num"] = pd.to_numeric(diagnoses["seq_num"], errors="coerce")
    diagnoses = diagnoses.sort_values(["hadm_id", "seq_num"])
    diagnoses = diagnoses.merge(icd_map, on="icd9_code", how="left")

    def agg_codes(series: pd.Series, n: int = 3) -> str:
        values = [str(v) for v in series.dropna().tolist()[:n]]
        return " | ".join(values)

    diag_summary = (
        diagnoses.groupby("hadm_id", as_index=False)
        .agg(
            primary_icd9_code=("icd9_code", lambda s: agg_codes(s, 1)),
            primary_icd9_title=("short_title", lambda s: agg_codes(s, 1)),
            top3_icd9_codes=("icd9_code", lambda s: agg_codes(s, 3)),
            top3_icd9_titles=("short_title", lambda s: agg_codes(s, 3)),
            diagnosis_count=("icd9_code", "count"),
        )
    )
    return diag_summary


def build_icu_stay_summary() -> pd.DataFrame:
    icu = _safe_read("ICUSTAYS.csv")
    icu["los"] = pd.to_numeric(icu["los"], errors="coerce")
    icu["intime"] = pd.to_datetime(icu["intime"], errors="coerce")
    icu = icu.sort_values(["hadm_id", "intime"])

    summary = (
        icu.groupby("hadm_id", as_index=False)
        .agg(
            icu_stay_count=("icustay_id", "nunique"),
            icu_total_los_days=("los", "sum"),
            icu_max_los_days=("los", "max"),
            first_careunit=("first_careunit", "first"),
            last_careunit=("last_careunit", "last"),
            dbsource=("dbsource", "first"),
        )
    )
    return summary


def build_alert_summary() -> pd.DataFrame:
    alerts_file = BASE_PATH / "patient_alerts.csv"
    if not alerts_file.exists():
        return pd.DataFrame(columns=["hadm_id", "alert_event_count", "has_shock_alert", "has_tachy_alert"])

    alerts = pd.read_csv(alerts_file)
    alerts["alert"] = alerts["alert"].fillna("").astype(str)
    alerts["has_shock_alert"] = alerts["alert"].str.contains("Shock Risk", case=False)
    alerts["has_tachy_alert"] = alerts["alert"].str.contains("High Heart Rate", case=False)

    summary = (
        alerts.groupby("hadm_id", as_index=False)
        .agg(
            alert_event_count=("alert", "count"),
            has_shock_alert=("has_shock_alert", "max"),
            has_tachy_alert=("has_tachy_alert", "max"),
        )
    )
    return summary


def main() -> None:
    full = _safe_read("full_medical_data_clean.csv")
    patients = _safe_read("PATIENTS.csv")[["subject_id", "gender", "dob", "expire_flag"]]
    admissions = _safe_read("ADMISSIONS.csv")[
        [
            "subject_id",
            "hadm_id",
            "admittime",
            "dischtime",
            "deathtime",
            "admission_type",
            "admission_location",
            "discharge_location",
            "insurance",
            "language",
            "marital_status",
            "ethnicity",
            "diagnosis",
            "hospital_expire_flag",
        ]
    ]

    diag_summary = build_diagnosis_summary()
    icu_summary = build_icu_stay_summary()
    alert_summary = build_alert_summary()

    full["charttime"] = pd.to_datetime(full["charttime"], errors="coerce")
    patients["dob"] = pd.to_datetime(patients["dob"], errors="coerce")
    admissions["admittime"] = pd.to_datetime(admissions["admittime"], errors="coerce")
    admissions["dischtime"] = pd.to_datetime(admissions["dischtime"], errors="coerce")
    admissions["deathtime"] = pd.to_datetime(admissions["deathtime"], errors="coerce")

    df = full.merge(patients, on="subject_id", how="left")
    df = df.merge(admissions, on=["subject_id", "hadm_id"], how="left")
    df = df.merge(icu_summary, on="hadm_id", how="left")
    df = df.merge(diag_summary, on="hadm_id", how="left")
    df = df.merge(alert_summary, on="hadm_id", how="left")

    # Avoid datetime overflow by computing age from calendar parts.
    df["age_at_admit"] = (
        df["admittime"].dt.year
        - df["dob"].dt.year
        - (
            (df["admittime"].dt.month < df["dob"].dt.month)
            | (
                (df["admittime"].dt.month == df["dob"].dt.month)
                & (df["admittime"].dt.day < df["dob"].dt.day)
            )
        ).astype(int)
    ).clip(lower=0, upper=120)
    df["hospital_los_days"] = (df["dischtime"] - df["admittime"]).dt.total_seconds() / 86400
    df["days_from_admit"] = (df["charttime"] - df["admittime"]).dt.total_seconds() / 86400
    df["died_within_30d"] = (
        ((df["deathtime"] - df["admittime"]).dt.total_seconds() / 86400) <= 30
    ).fillna(False)
    df["has_multiple_icu_stays"] = (pd.to_numeric(df["icu_stay_count"], errors="coerce").fillna(0) > 1)

    for col in ["alert_event_count", "diagnosis_count", "icu_stay_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["has_shock_alert", "has_tachy_alert", "died_within_30d", "has_multiple_icu_stays"]:
        df[col] = df[col].astype("boolean").fillna(False).astype(bool)

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved detailed ICU dataset: {OUTPUT_PATH}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")


if __name__ == "__main__":
    main()
