import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

df = pd.read_csv(os.path.join(BASE_PATH, "clean_vitals.csv"))

df["charttime"] = pd.to_datetime(df["charttime"])

# Separate each vital
hr = df[df["itemid"] == 211][["subject_id","hadm_id","charttime","valuenum"]]
bp = df[df["itemid"] == 456][["subject_id","hadm_id","charttime","valuenum"]]
spo2 = df[df["itemid"] == 646][["subject_id","hadm_id","charttime","valuenum"]]
temp = df[df["itemid"].isin([676,677])][["subject_id","hadm_id","charttime","valuenum"]]

# Rename columns
hr = hr.rename(columns={"valuenum":"heart_rate"})
bp = bp.rename(columns={"valuenum":"bp_mean"})
spo2 = spo2.rename(columns={"valuenum":"spo2"})
temp = temp.rename(columns={"valuenum":"temp"})

# Sort (required for merge_asof)
for d in [hr, bp, spo2, temp]:
    d.sort_values("charttime", inplace=True)

# -----------------------
# Merge HR + BP (2 hours)
# -----------------------
base = pd.merge_asof(
    hr,
    bp,
    on="charttime",
    by=["subject_id","hadm_id"],
    direction="nearest",
    tolerance=pd.Timedelta("2h")
)

# -----------------------
# Merge SpO2 (2 hours)
# -----------------------
base = pd.merge_asof(
    base,
    spo2,
    on="charttime",
    by=["subject_id","hadm_id"],
    direction="nearest",
    tolerance=pd.Timedelta("2h")
)

# -----------------------
# Merge Temp (2 hours)
# -----------------------
base = pd.merge_asof(
    base,
    temp,
    on="charttime",
    by=["subject_id","hadm_id"],
    direction="nearest",
    tolerance=pd.Timedelta("2h")
)

# -----------------------
# Forward + Backward Fill
# -----------------------
base = base.sort_values(["subject_id", "charttime"])

cols = ["spo2", "temp"]

base[cols] = base.groupby("subject_id")[cols].ffill()
base[cols] = base.groupby("subject_id")[cols].bfill()

# Preview
print(base.head())

# Save
OUTPUT = os.path.join(BASE_PATH, "alert_ready_data.csv")
base.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
