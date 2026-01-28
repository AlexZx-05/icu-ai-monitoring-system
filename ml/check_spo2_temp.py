import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

chartevents = pd.read_csv(
    os.path.join(BASE_PATH, "CHARTEVENTS.csv"),
    low_memory=False
)

# Check counts
ids = [646, 676, 677]

for i in ids:
    count = (chartevents["itemid"] == i).sum()
    print(f"ItemID {i} count:", count)
