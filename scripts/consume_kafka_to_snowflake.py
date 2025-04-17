# scripts/consume_kafka_to_snowflake.py
from kafka import KafkaConsumer
import snowflake.connector
import json
import os

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "logistics"

# Configuration Snowflake
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = "LOGISTICS_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"

# Connexion à Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logistics_data (
            order_id VARCHAR,
            order_date VARCHAR,
            shipping_date VARCHAR,
            order_item_quantity INT,
            customer_city VARCHAR,
            order_city VARCHAR
        )
    """)

def insert_data(cursor, data):
    for record in data:
        cursor.execute("""
            INSERT INTO logistics_data (order_id, order_date, shipping_date, order_item_quantity, customer_city, order_city)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            record.get("Order Id"),
            record.get("order date (DateOrders)"),
            record.get("shipping date (DateOrders)"),
            record.get("Order Item Quantity"),
            record.get("Customer City"),
            record.get("Order City")
        ))

def consume_kafka_to_snowflake():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="snowflake-consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    cursor = conn.cursor()
    try:
        # Créer la table si elle n'existe pas
        create_table_if_not_exists(cursor)

        # Consommer les messages
        print("Début de la consommation des messages Kafka...")
        for message in consumer:
            data = message.value
            print(f"Message reçu du topic {KAFKA_TOPIC}: {data}")

            # Insérer les données dans Snowflake
            insert_data(cursor, [data])  # Traiter un message à la fois
            conn.commit()
            print("Données insérées dans Snowflake (table: logistics_data).")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_kafka_to_snowflake()