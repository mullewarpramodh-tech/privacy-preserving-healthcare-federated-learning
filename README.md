# Privacy-Preserving Healthcare Federated Learning Network

![Java](https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.3-brightgreen?style=for-the-badge&logo=springboot)
![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python)
![TensorFlow](https://img.shields.io/badge/TensorFlow-2.x-FF6F00?style=for-the-badge&logo=tensorflow)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Streaming-black?style=for-the-badge&logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon_Serverless-blue?style=for-the-badge&logo=postgresql)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

A decentralized, privacy-preserving machine learning architecture engineered to train deep learning models across distributed hospital data silos without centralizing sensitive electronic health records (EHR).

---

## Architecture & Data Flow

```mermaid
flowchart TD
    subgraph Hospital Node A (Local Edge)
        DataA[(Local Patient EHR)] --> LocalTrainA[TensorFlow Model Training]
        LocalTrainA --> WeightsA[Extracted Model Weights]
    end

    subgraph Hospital Node B (Local Edge)
        DataB[(Local Patient EHR)] --> LocalTrainB[TensorFlow Model Training]
        LocalTrainB --> WeightsB[Extracted Model Weights]
    end

    WeightsA -->|Kafka: hospital-local-weights| Admin[Spring Boot Admin Orchestrator]
    WeightsB -->|Kafka: hospital-local-weights| Admin

    subgraph Aggregation Engine
        Admin -->|Federated Averaging FedAvg| GlobalModel[Updated Global Model Weights]
        Admin --> DB[(Neon Cloud PostgreSQL)]
    end

    GlobalModel -->|Kafka Broadcast: global-model-updates| Hospital Node A
    GlobalModel -->|Kafka Broadcast: global-model-updates| Hospital Node B
```

---

## Key Highlights & Privacy Guarantees

* **Zero Raw Data Centralization:** Compliant with healthcare data governance (HIPAA, India DPDP Act). Raw patient records never leave the local hospital firewall.
* **Polyglot Microservices:** Combines high-performance Java Spring Boot (Admin Orchestrator) for participant registration, JWT authentication, and Kafka stream management with Python/TensorFlow (Hospital Edge Nodes) for localized neural network computation.
* **Federated Averaging (FedAvg):** Aggregates local gradient updates to formulate global model weights:
  $$\theta_{\text{global}} \leftarrow \sum_{k=1}^{K} \frac{n_k}{n} \theta_k$$
* **Event-Driven Coordination:** Asynchronous model distribution and weight collection mediated by Apache Kafka topics (`global-model-updates` and `hospital-local-weights`).

---

## Technology Stack

* **Central Admin Node:** Java 21, Spring Boot 3.3, Spring Security, JWT, Neon Serverless PostgreSQL
* **Message Broker:** Apache Kafka
* **Edge ML Client:** Python 3.11, TensorFlow 2.x, Keras, Pandas, Kafka-Python
* **Build Tools:** Maven & Pip

---

## Repository Structure

```text
privacy-preserving-healthcare-federated-learning/
├── admin-service/               # Java / Spring Boot Central Orchestration Node
│   ├── src/main/java/com/federated/admin/
│   └── pom.xml
├── hospital-node/               # Python / TensorFlow Edge Client
│   ├── client.py                # Local training & Kafka listener
│   ├── mock_patient_data.csv    # Local training dataset
│   └── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## Quick Start & Execution

### 1. Start the Java Admin Orchestrator
```bash
cd admin-service
./mvnw spring-boot:run
```
*The service initializes on port 8080 and configures the Kafka topics.*

### 2. Start the Python Hospital ML Node
```bash
cd hospital-node
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python client.py
```
*The client connects to Kafka and begins listening for `global-model-updates`.*

### 3. Trigger Federated Training Pipeline
```bash
# Register a hospital
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "Metro Hospital", "email": "admin@metrohealth.org", "password": "securePass123"}'

# Login to retrieve JWT
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "admin@metrohealth.org", "password": "securePass123"}' | jq -r .token)

# Trigger training broadcast
curl -X POST http://localhost:8080/api/fl/broadcast \
  -H "Authorization: Bearer $TOKEN"
```

---

## API Reference Guide

| Endpoint | Method | Security | Description |
|---|---|---|---|
| `/api/auth/register` | `POST` | Public | Onboards a hospital node into PostgreSQL |
| `/api/auth/login` | `POST` | Public | Authenticates hospital and returns JWT |
| `/api/fl/broadcast` | `POST` | Bearer JWT | Dispatches training trigger into Kafka broker |

---

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
