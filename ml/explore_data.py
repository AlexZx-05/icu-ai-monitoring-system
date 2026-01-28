import pandas as pd
import os

# Base path to dataset folder
BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load main files
patients = pd.read_csv(os.path.join(BASE_PATH, "PATIENTS.csv"))
admissions = pd.read_csv(os.path.join(BASE_PATH, "ADMISSIONS.csv"))
icustays = pd.read_csv(os.path.join(BASE_PATH, "ICUSTAYS.csv"))
chartevents = pd.read_csv(
    os.path.join(BASE_PATH, "CHARTEVENTS.csv"),
    low_memory=False
)

# Print basic info
print("Patients:", patients.shape)
print("Admissions:", admissions.shape)
print("ICU Stays:", icustays.shape)
print("Chart Events:", chartevents.shape)

# Show first rows
print("\nPatients Data:")
print(patients.head())

print("\nICU Stays Data:")
print(icustays.head())

print("\nChart Events Data:")
print(chartevents.head())
