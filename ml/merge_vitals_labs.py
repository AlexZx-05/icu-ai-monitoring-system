import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

vitals = pd.read_csv(os.path.join(BASE_PATH, "alert_ready_data.csv"))
labs = pd.read_csv(os.path.join(BASE_PATH, "lab_ready_data.csv"))

# FIX: Make IDs same type (int)
vitals["subject_id"] = vitals["subject_id"].astype(int)
vitals["hadm_id"] = vitals["hadm_id"].astype(int)

labs["subject_id"] = labs["subject_id"].astype(int)
labs["hadm_id"] = labs["hadm_id"].astype(int)

# Convert time
vitals["charttime"] = pd.to_datetime(vitals["charttime"])
labs["charttime"] = pd.to_datetime(labs["charttime"])

# Sort (required for merge_asof)
vitals = vitals.sort_values("charttime")
labs = labs.sort_values("charttime")

# Merge by nearest time (within 6 hours)
merged = pd.merge_asof(
    vitals,
    labs,
    on="charttime",
    by=["subject_id", "hadm_id"],
    direction="nearest",
    tolerance=pd.Timedelta("6h")
)

print(merged.head())

# Save
OUTPUT = os.path.join(BASE_PATH, "full_medical_data.csv")
merged.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
