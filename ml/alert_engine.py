import pandas as pd
import os

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load prepared data
df = pd.read_csv(os.path.join(BASE_PATH, "alert_ready_data.csv"))

# Convert time
df["charttime"] = pd.to_datetime(df["charttime"])

# Alert rules
def check_alert(row):
    alerts = []

    hr = row["heart_rate"]
    bp = row["bp_mean"]

    if hr > 120:
        alerts.append("High Heart Rate")

    if hr < 50:
        alerts.append("Low Heart Rate")

    if bp < 60:
        alerts.append("Low Blood Pressure (Shock Risk)")

    if bp > 110:
        alerts.append("High Blood Pressure")

    return ", ".join(alerts)

# Apply rules
df["alert"] = df.apply(check_alert, axis=1)

# Filter only danger rows
alerts_df = df[df["alert"] != ""]

print("ALERTS FOUND:")
print(alerts_df.head(10))

# Save alerts
OUTPUT = os.path.join(BASE_PATH, "patient_alerts.csv")
alerts_df.to_csv(OUTPUT, index=False)

print("Saved alerts to:", OUTPUT)
