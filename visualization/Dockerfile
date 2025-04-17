# Utiliser une image de base Python
FROM python:3.9-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers requirements.txt
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Installer Streamlit
RUN pip install streamlit

# Copier le code de l'application
COPY streamlit/ .

# Exposer le port Streamlit
EXPOSE 8501

# Commande pour démarrer Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]