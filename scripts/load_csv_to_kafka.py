# scripts/load_csv_to_kafka.py
import pandas as pd
from kafka import KafkaProducer
import json

# Chemin du fichier CSV
CSV_FILE_PATH = "/opt/airflow/data/DataCoSupplyChainDataset.csv"

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "logistics"

def load_csv_to_kafka():
    # Charger le fichier CSV
    print("Chargement du fichier CSV...")
    df = pd.read_csv(CSV_FILE_PATH, encoding="latin-1")
    
    # Sélectionner les colonnes pertinentes (comme dans /api/logistics)
    columns = [
        "Order Id",
        "order date (DateOrders)",
        "shipping date (DateOrders)",
        "Order Item Quantity",
        "Customer City",
        "Order City"
    ]
    df = df[columns]
    
    # Convertir en JSON
    data = df.to_dict(orient="records")
    print(f"{len(data)} enregistrements chargés depuis le CSV.")

    # Envoyer à Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print(f"Envoi des données à Kafka (topic: {KAFKA_TOPIC})...")
    for record in data:
        producer.send(KAFKA_TOPIC, value=record)
    producer.flush()
    producer.close()
    print("Données envoyées avec succès à Kafka.")

if __name__ == "__main__":
    load_csv_to_kafka()