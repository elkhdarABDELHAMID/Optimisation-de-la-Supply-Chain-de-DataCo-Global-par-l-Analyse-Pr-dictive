# api/logistics_api/main.py
from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/api/logistics")
async def get_logistics_data():
    # Charger le fichier CSV
    df = pd.read_csv("/opt/airflow/data/DataCoSupplyChainDataset.csv", encoding="latin-1")
    
    # Sélectionner les colonnes pertinentes
    columns = [
        "Order Id", 
        "order date (DateOrders)", 
        "shipping date (DateOrders)", 
        "Order Item Quantity", 
        "Customer City",  # Approximation pour l'emplacement de l'entrepôt
        "Order City"     # Approximation pour le point de livraison
    ]
    
    # Filtrer les colonnes
    df = df[columns]
    
    # Convertir en JSON
    data = df.to_dict(orient="records")
    return {"data": data}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}