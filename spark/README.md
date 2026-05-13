# Spark Batch ML Pipeline

This module handles the batch training of machine learning models for the Smart Waste Management System.

## Structure

- `job.py`: Main entry point for the Spark job.
- `src/`: Core logic package.
  - `config.py`: Configuration and environment variables.
  - `data.py`: Database interaction and JDBC loading.
  - `features.py`: Feature engineering logic for the three models.
  - `models.py`: Scikit-learn model training and hyperparameter tuning.
  - `tracking.py`: MLflow integration and logging utilities.

## Models Trained

1. **Waste Fill Time Model**: Predicts the remaining time until a bin is full.
2. **Zone Generation Model**: Forecasts waste generation trends per zone.
3. **Route Score Model**: Evaluates the efficiency of collection routes.

## Deployment

The job is dockerized and intended to be run via Airflow's `BashOperator`.

```bash
docker build -t my-spark-job .
docker run --network waste-network my-spark-job
```
