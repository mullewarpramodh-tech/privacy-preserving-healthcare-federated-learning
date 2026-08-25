import pandas as pd
import numpy as np

# Generate 5,000 rows of synthetic patient data
print("Generating synthetic hospital dataset...")
np.random.seed(42)
n_samples = 5000

ages = np.random.randint(25, 85, n_samples)
blood_pressures = np.random.normal(120, 15, n_samples).astype(int) # Mean 120, SD 15
cholesterols = np.random.normal(200, 30, n_samples).astype(int)    # Mean 200, SD 30

# Logic for Diagnosis: Higher age, BP, and Cholesterol increase the probability of disease
# Normalize values roughly to calculate a risk score
risk_scores = (ages * 0.4) + (blood_pressures * 0.3) + (cholesterols * 0.3)
threshold = np.percentile(risk_scores, 75) # Top 25% get diagnosis 1 (positive)

diagnoses = (risk_scores > threshold).astype(int)

df = pd.DataFrame({
    'Age': ages,
    'BloodPressure': blood_pressures,
    'Cholesterol': cholesterols,
    'Diagnosis': diagnoses
})

df.to_csv('mock_patient_data.csv', index=False)
print(f"Successfully generated a massive dataset with {n_samples} patient records!")
