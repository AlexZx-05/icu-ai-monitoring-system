import pandas as pd
import os
import joblib

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score


BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load training data
df = pd.read_csv(os.path.join(BASE_PATH, "ml_training_data.csv"))

X = df[["hr_avg", "bp_avg", "hr_trend"]]
y = df["future_risk"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=8,
    random_state=42
)

model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)

acc = accuracy_score(y_test, y_pred)

print("Model Accuracy:", acc)
print("\nClassification Report:")
print(classification_report(y_test, y_pred))


# Save model
MODEL_PATH = os.path.join(BASE_PATH, "risk_model.pkl")
joblib.dump(model, MODEL_PATH)

print("Model saved to:", MODEL_PATH)
