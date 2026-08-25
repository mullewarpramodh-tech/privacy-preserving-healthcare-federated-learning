import os
import json
import pandas as pd
import tensorflow as tf
from kafka import KafkaConsumer, KafkaProducer

# 1. Setup Kafka Connections to the Java Spring Boot Broker
KAFKA_BROKER = '127.0.0.1:9092'

print("🏥 Starting Hospital Federated Learning Client...")

# Producer to send trained weights back to Admin
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    api_version=(3, 7, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consumer to listen for global model broadcasts from Admin
consumer = KafkaConsumer(
    'global-model-updates',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='hospital-1-group',
    api_version=(3, 7, 0),
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def build_model():
    """Builds the exact Neural Network architecture described in the project PDF"""
    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(16, activation='relu', input_shape=(3,)),
        tf.keras.layers.Dense(8, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

def load_local_data():
    """Loads the hospital's private, encrypted patient data (mock data for now)"""
    df = pd.read_csv('mock_patient_data.csv')
    X = df[['Age', 'BloodPressure', 'Cholesterol']].values
    y = df['Diagnosis'].values
    return X, y

def train_local_model():
    """Trains the model locally so private data NEVER leaves the hospital"""
    print("\n📊 Loading private patient data...")
    X, y = load_local_data()
    
    # 80/20 Train-Test Split (Replicating the original PDF logic)
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    print(f"📈 Dataset split: {len(X_train)} training rows, {len(X_test)} validation rows")
    
    print("🧠 Initializing Deep Learning Model...")
    model = build_model()
    
    print("🚀 Training model locally for 5 epochs...")
    # Add validation_data so you can see the real accuracy progression
    model.fit(X_train, y_train, epochs=5, validation_data=(X_test, y_test), verbose=1)
    
    # Evaluate final accuracy on the unseen test set
    accuracy = model.evaluate(X_test, y_test, verbose=0)[1]
    
    print(f"\n✅ Local Training Complete! True Validation Accuracy: {accuracy*100:.2f}%")
    return accuracy

print("🎧 Listening for Global AI Model from Admin Node...")
for message in consumer:
    global_model_event = message.value
    print(f"\n🔔 KAFKA ALERT: Received Global Model Version {global_model_event.get('version', '1')} from Admin!")
    
    # Trigger local training
    local_accuracy = train_local_model()
    
    # Send the "weights" (accuracy metrics for this prototype) back to Kafka
    update_payload = {
        "hospitalId": 1,
        "hospitalName": "Delhi City Hospital",
        "localAccuracy": local_accuracy,
        "status": "SUCCESS"
    }
    
    print("📤 Publishing localized weights back to Kafka topic 'hospital-local-weights'...")
    producer.send('hospital-local-weights', update_payload)
    producer.flush()
    print("🎉 Update sent! Waiting for next global model iteration...\n")
