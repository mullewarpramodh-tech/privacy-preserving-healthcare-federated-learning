import os
import json
import numpy as np
import pandas as pd
import tensorflow as tf
from kafka import KafkaConsumer, KafkaProducer

# 1. Setup Kafka Connections to the Java Spring Boot Broker
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9092')
HOSPITAL_ID = int(os.getenv('HOSPITAL_ID', '1'))
HOSPITAL_NAME = os.getenv('HOSPITAL_NAME', 'Delhi City Hospital')

print(f"🏥 Starting Edge Node: '{HOSPITAL_NAME}' (ID: {HOSPITAL_ID})...")

# Producer to send local trained weights back to Spring Boot Admin
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    api_version=(3, 7, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consumer to listen for global model broadcasts from Spring Boot Admin
consumer = KafkaConsumer(
    'global-model-updates',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id=f'hospital-{HOSPITAL_ID}-group',
    api_version=(3, 7, 0),
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def build_model():
    """Builds the deep neural network architecture for clinical risk classification."""
    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(16, activation='relu', input_shape=(3,)),
        tf.keras.layers.Dense(8, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

def load_local_data():
    """Loads the hospital's private electronic health records (EHR)."""
    csv_file = 'mock_patient_data.csv'
    if not os.path.exists(csv_file):
        print("⚠️ 'mock_patient_data.csv' not found. Generating synthetic dataset...")
        import generate_data
    
    df = pd.read_csv(csv_file)
    X = df[['Age', 'BloodPressure', 'Cholesterol']].values
    y = df['Diagnosis'].values
    return X, y

def train_local_model(initial_weights=None):
    """
    Trains the model locally so private patient data NEVER leaves the hospital firewall.
    Initializes weights with incoming global model if provided.
    """
    print("\n📊 Loading private patient records...")
    X, y = load_local_data()
    
    # 80/20 Train-Validation Split
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    print(f"📈 Local Dataset: {len(X_train)} training records, {len(X_test)} validation records")
    model = build_model()

    # Load initial global model weights if received from orchestrator
    if initial_weights is not None:
        try:
            np_weights = [np.array(w, dtype=np.float32) for w in initial_weights]
            model.set_weights(np_weights)
            print("🔄 Initialized local model with latest Global Model weights from Admin!")
        except Exception as e:
            print(f"⚠️ Warning: Could not apply incoming weights ({e}); training with initial weights.")

    print("🚀 Training local neural network for 5 epochs on private EHR...")
    model.fit(X_train, y_train, epochs=5, validation_data=(X_test, y_test), verbose=1)
    
    # Evaluate performance on local unseen test partition
    eval_res = model.evaluate(X_test, y_test, verbose=0)
    loss = float(eval_res[0])
    accuracy = float(eval_res[1])
    
    print(f"✅ Local Training Complete! Validation Accuracy: {accuracy*100:.2f}% | Loss: {loss:.4f}")

    # Extract model weights and serialize tensors into JSON-compatible Python lists
    raw_weights = model.get_weights()
    serialized_weights = [w.tolist() for w in raw_weights]

    return accuracy, loss, serialized_weights, len(X_train)

print(f"🎧 Edge Node '{HOSPITAL_NAME}' listening for Global Model events on Kafka topic 'global-model-updates'...\n")

for message in consumer:
    global_model_event = message.value
    round_version = global_model_event.get('version', 1)
    incoming_weights = global_model_event.get('weights', None)

    print(f"\n🔔 [Kafka Event] Received Global Model Round #{round_version} from Admin!")
    
    # Run local training loop
    local_accuracy, local_loss, trained_weights, sample_count = train_local_model(initial_weights=incoming_weights)
    
    # Package payload according to Federated Averaging (FedAvg) protocol
    update_payload = {
        "hospitalId": HOSPITAL_ID,
        "hospitalName": HOSPITAL_NAME,
        "roundId": round_version,
        "sampleCount": sample_count,
        "localAccuracy": round(local_accuracy, 4),
        "localLoss": round(local_loss, 4),
        "weights": trained_weights,
        "status": "SUCCESS"
    }
    
    print(f"📤 Publishing local model weights (Samples: {sample_count}) to Kafka topic 'hospital-local-weights'...")
    producer.send('hospital-local-weights', update_payload)
    producer.flush()
    print("🎉 Update published successfully! Awaiting next Federated round...\n")
