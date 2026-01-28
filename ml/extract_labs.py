import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# UPDATE THESE AFTER STEP 1
LAB_ITEMIDS = [
    50811,   # WBC
    50912,   # Creatinine
    50813    # Lactate
]

labs = pd.read_csv(
    os.path.join(BASE_PATH, "LABEVENTS.csv"),
    low_memory=False
)

# Filter labs
lab_data = labs[labs["itemid"].isin(LAB_ITEMIDS)]

# Keep useful columns
lab_data = lab_data[
    ["subject_id", "hadm_id", "charttime", "itemid", "valuenum"]
]

lab_data = lab_data.dropna(subset=["valuenum"])

print("Lab rows:", lab_data.shape)

# Save
OUTPUT = os.path.join(BASE_PATH, "clean_labs.csv")
lab_data.to_csv(OUTPUT, index=False)

print("Saved:", OUTPUT)
