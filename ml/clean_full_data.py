import pandas as pd
import os
import numpy as np

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

df = pd.read_csv(os.path.join(BASE_PATH, "full_medical_data.csv"))

df["charttime"] = pd.to_datetime(df["charttime"])

# Sort
df = df.sort_values(["subject_id", "charttime"])

cols = ["spo2", "temp", "creatinine", "lactate", "wbc"]

# Missing flags
for c in cols:
    df[c + "_missing"] = df[c].isna().astype(int)

# Per-patient fill
df[cols] = df.groupby("subject_id")[cols].ffill()
df[cols] = df.groupby("subject_id")[cols].bfill()

# Safe defaults
DEFAULTS = {
    "spo2": 97.0,
    "temp": 36.8,
    "creatinine": df["creatinine"].median(),
    "lactate": df["lactate"].median(),
    "wbc": df["wbc"].median()
}

# Final fill
for c in cols:
    df[c] = df[c].fillna(DEFAULTS[c])

# Check
print("Missing after cleaning:")
print(df[cols].isna().sum())

# Save
OUTPUT = os.path.join(BASE_PATH, "full_medical_data_clean.csv")
df.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
