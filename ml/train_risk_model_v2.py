import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import joblib

BASE_PATH = r"C:\Users\LENOVO\Desktop\icu_project\data\mimic-iii-clinical-database-demo-1.4"

# Load dataset
df = pd.read_csv(os.path.join(BASE_PATH, "ml_training_data_v2.csv"))

# Features & Target
X = df.drop("future_risk", axis=1)
y = df["future_risk"]

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train model
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    random_state=42,
    class_weight="balanced"
)

model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Evaluate
acc = accuracy_score(y_test, y_pred)

print("Model Accuracy:", acc)
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Save
MODEL_PATH = os.path.join(BASE_PATH, "risk_model_v2.pkl")
joblib.dump(model, MODEL_PATH)

print("Saved model to:", MODEL_PATH)
