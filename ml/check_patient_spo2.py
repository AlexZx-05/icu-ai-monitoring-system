import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

df = pd.read_csv(os.path.join(BASE_PATH, "clean_vitals.csv"))

result = df[
    (df["subject_id"] == 10006) &
    (df["itemid"].isin([646, 676, 677]))
]

print(result.head(20))
print("Total rows:", len(result))
