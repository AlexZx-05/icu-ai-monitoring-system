import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

labs = pd.read_csv(os.path.join(BASE_PATH, "D_LABITEMS.csv"))

keywords = ["white", "wbc", "creatinine", "lactate"]

matches = labs[
    labs["label"].str.lower().str.contains("|".join(keywords), na=False)
]

print(matches[["itemid", "label"]])
