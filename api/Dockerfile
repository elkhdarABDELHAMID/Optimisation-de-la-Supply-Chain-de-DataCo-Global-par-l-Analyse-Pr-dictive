# api/Dockerfile
FROM python:3.9-slim

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY ./logistics_api /app/logistics_api
COPY ./requirements.txt /app/requirements.txt

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Exposer le port
EXPOSE 8000

# Commande pour lancer l'API
CMD ["uvicorn", "logistics_api.main:app", "--host", "0.0.0.0", "--port", "8000"]