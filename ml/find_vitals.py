import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load item dictionary
items = pd.read_csv(os.path.join(BASE_PATH, "D_ITEMS.csv"))

# Search keywords
keywords = ["heart", "rate", "spo2", "oxygen", "bp", "blood", "pressure", "temp"]

# Find matching items
matches = items[
    items["label"].str.lower().str.contains("|".join(keywords), na=False)
]

print(matches[["itemid", "label", "category"]].head(50))
