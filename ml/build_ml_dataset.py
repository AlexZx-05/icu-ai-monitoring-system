import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load alert-ready data
df = pd.read_csv(os.path.join(BASE_PATH, "alert_ready_data.csv"))

# Convert time
df["charttime"] = pd.to_datetime(df["charttime"])

# Sort
df = df.sort_values(["subject_id", "charttime"])

# Rolling features (3-hour window)
df["hr_avg"] = df.groupby("subject_id")["heart_rate"].rolling(3).mean().reset_index(0, drop=True)
df["bp_avg"] = df.groupby("subject_id")["bp_mean"].rolling(3).mean().reset_index(0, drop=True)

df["hr_trend"] = df.groupby("subject_id")["heart_rate"].diff()

# Create risk label (future danger in next 6 hours)
df["future_risk"] = 0

for pid in df["subject_id"].unique():

    patient = df[df["subject_id"] == pid]

    for i in range(len(patient)):

        current_time = patient.iloc[i]["charttime"]

        future = patient[
            (patient["charttime"] > current_time) &
            (patient["charttime"] <= current_time + pd.Timedelta(hours=6))
        ]

        if ((future["heart_rate"] > 120).any() or
            (future["bp_mean"] < 60).any()):

            df.loc[patient.index[i], "future_risk"] = 1


# Clean NaNs
df = df.dropna()

# Select ML columns
ml_data = df[[
    "hr_avg",
    "bp_avg",
    "hr_trend",
    "future_risk"
]]

print("ML Dataset Shape:", ml_data.shape)

# Save
OUTPUT = os.path.join(BASE_PATH, "ml_training_data.csv")
ml_data.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
