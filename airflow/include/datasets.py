# Datasets wiring batch completion to its consumers (ARCHITECTURE.md §9.1).
from airflow import Dataset

BATCH_GOLD = Dataset("lakehouse://gold/batch")
