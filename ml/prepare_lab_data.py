import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

df = pd.read_csv(os.path.join(BASE_PATH, "clean_labs.csv"))

df["charttime"] = pd.to_datetime(df["charttime"])

LAB_MAP = {
    50811: "wbc",
    50912: "creatinine",
    50813: "lactate"
}

df["lab"] = df["itemid"].map(LAB_MAP)

# Pivot
lab_table = df.pivot_table(
    index=["subject_id", "hadm_id", "charttime"],
    columns="lab",
    values="valuenum"
).reset_index()

print(lab_table.head())

# Save
OUTPUT = os.path.join(BASE_PATH, "lab_ready_data.csv")
lab_table.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
