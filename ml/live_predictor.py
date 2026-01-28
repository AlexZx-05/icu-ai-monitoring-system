import pandas as pd
import os
import joblib
from send_alert_email import send_alert


BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

MODEL_PATH = os.path.join(BASE_PATH, "risk_model_v2.pkl")
DATA_PATH = os.path.join(BASE_PATH, "full_medical_data_clean.csv")


# Load model
model = joblib.load(MODEL_PATH)

# Load data
df = pd.read_csv(DATA_PATH)
df["charttime"] = pd.to_datetime(df["charttime"])

# Sort
df = df.sort_values(["subject_id", "charttime"])


# ----------------------------
# Build latest features
# ----------------------------
def build_latest_features(patient_df):

    patient_df = patient_df.sort_values("charttime")

    # Last 3 records
    last = patient_df.tail(3)

    if len(last) < 3:
        return None

    features = {}

    # Rolling averages
    features["hr_avg"] = last["heart_rate"].mean()
    features["bp_avg"] = last["bp_mean"].mean()
    features["spo2_avg"] = last["spo2"].mean()
    features["temp_avg"] = last["temp"].mean()

    # Trend
    features["hr_trend"] = (
        last["heart_rate"].iloc[-1] - last["heart_rate"].iloc[-2]
    )

    # Labs (latest)
    features["creatinine"] = last["creatinine"].iloc[-1]
    features["lactate"] = last["lactate"].iloc[-1]
    features["wbc"] = last["wbc"].iloc[-1]

    # Missing flags
    features["spo2_missing"] = last["spo2_missing"].iloc[-1]
    features["temp_missing"] = last["temp_missing"].iloc[-1]

    return pd.DataFrame([features])


# ----------------------------
# Predict for all patients
# ----------------------------
print("🔍 Running Live ICU Monitor...\n")


for pid in df["subject_id"].unique():

    patient_data = df[df["subject_id"] == pid]

    features = build_latest_features(patient_data)

    if features is None:
        continue

    # Predict probability
    prob = model.predict_proba(features)[0][1]

    status = "STABLE"

    if prob >= 0.7:
        status = "⚠️ HIGH RISK"

    if prob >= 0.85:
        status = "🚨 CRITICAL"

        # Use averages for alert info
        hr = features["hr_avg"].values[0]
        bp = features["bp_avg"].values[0]

        send_alert(pid, prob * 100, hr, bp)


    print(f"Patient {pid}")
    print(f"Risk Score: {prob*100:.1f}%")
    print(f"Status: {status}")
    print("-" * 40)
