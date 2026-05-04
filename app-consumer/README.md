# Smart Waste Management - Application Consumer

This Python script consumes real-time telemetry data from the Kafka broker deployed on AWS EKS.

## Connectivity Note

The application connects to Kafka via an AWS External Load Balancer (ELB). Ensure your network allows outbound traffic on the Kafka port (9094).

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration**:
   Credentials and broker details are stored in `.env`. 

## Running the Consumer

```bash
python kafka_consumer.py
```

## Running the Tests

Diagnostic and verification scripts are located in the `tests/` directory.

```bash
python tests/test_consumer.py
```
