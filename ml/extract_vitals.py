import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load chartevents
chartevents = pd.read_csv(
    os.path.join(BASE_PATH, "CHARTEVENTS.csv"),
    low_memory=False
)

# Important itemids (from your output)
VITAL_ITEMIDS = [
    211,   # Heart Rate
    51,    # Arterial BP Systolic
    455,   # NBP Systolic
    456,   # NBP Mean
    424,   # Temp
    427,   # Temp
    219    # Respiration
]

# Filter vitals
vitals = chartevents[chartevents["itemid"].isin(VITAL_ITEMIDS)]

# Keep useful columns
vitals = vitals[
    ["subject_id", "hadm_id", "charttime", "itemid", "valuenum", "valueuom"]
]

# Remove missing values
vitals = vitals.dropna(subset=["valuenum"])

print("Clean vitals shape:", vitals.shape)

# Save file
OUTPUT_PATH = os.path.join(BASE_PATH, "clean_vitals.csv")
vitals.to_csv(OUTPUT_PATH, index=False)

print("Saved to:", OUTPUT_PATH)
