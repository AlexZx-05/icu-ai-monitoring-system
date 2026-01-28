import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load cleaned full data
df = pd.read_csv(
    os.path.join(BASE_PATH, "full_medical_data_clean.csv")
)

df["charttime"] = pd.to_datetime(df["charttime"])

# Sort
df = df.sort_values(["subject_id", "charttime"])

# -----------------------------
# Missing flags (IMPORTANT)
# -----------------------------
df["spo2_missing"] = df["spo2"].isna().astype(int)
df["temp_missing"] = df["temp"].isna().astype(int)

# Fill vitals with median (after flag)
df["spo2"] = df["spo2"].fillna(df["spo2"].median())
df["temp"] = df["temp"].fillna(df["temp"].median())

# -----------------------------
# Rolling features (3 hours)
# -----------------------------
group = df.groupby("subject_id")

df["hr_avg"] = group["heart_rate"].rolling(3).mean().reset_index(0, drop=True)
df["bp_avg"] = group["bp_mean"].rolling(3).mean().reset_index(0, drop=True)
df["spo2_avg"] = group["spo2"].rolling(3).mean().reset_index(0, drop=True)
df["temp_avg"] = group["temp"].rolling(3).mean().reset_index(0, drop=True)

df["hr_trend"] = group["heart_rate"].diff()

# -----------------------------
# Create target (future risk)
# -----------------------------
df["future_risk"] = 0

for pid in df["subject_id"].unique():

    patient = df[df["subject_id"] == pid]

    for i in range(len(patient)):

        t = patient.iloc[i]["charttime"]

        future = patient[
            (patient["charttime"] > t) &
            (patient["charttime"] <= t + pd.Timedelta(hours=6))
        ]

        if (
            (future["heart_rate"] > 120).any() or
            (future["bp_mean"] < 60).any() or
            (future["spo2"] < 90).any() or
            (future["lactate"] > 2).any()
        ):
            df.loc[patient.index[i], "future_risk"] = 1


# -----------------------------
# Remove incomplete rows
# -----------------------------
df = df.dropna()

# -----------------------------
# Final ML dataset
# -----------------------------
ml_data = df[[
    "hr_avg",
    "bp_avg",
    "spo2_avg",
    "temp_avg",
    "hr_trend",

    "creatinine",
    "lactate",
    "wbc",

    "spo2_missing",
    "temp_missing",

    "future_risk"
]]

print("ML Dataset Shape:", ml_data.shape)

# Save
OUTPUT = os.path.join(BASE_PATH, "ml_training_data_v2.csv")
ml_data.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
