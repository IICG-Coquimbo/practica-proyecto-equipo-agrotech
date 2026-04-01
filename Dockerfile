<<<<<<< HEAD
FROM jupyter/pyspark-notebook:latest

USER root

# 1. Instalar dependencias base y configurar el repo de Google Chrome
RUN apt-get update && apt-get install -y wget gnupg2 curl && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub |
    apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable
    main" >> /etc/apt/sources.list.d/google-chrome.list
# 2. Instalar Google Chrome y librerias de soporte
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    libnss3 \
    libgbm1 \
    libasound2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Instalar librer as de Python (incluyendo el manager de drivers)
RUN pip install selenium pymongo webdriver-manager
=======
# Imagen base: trae Jupyter + Python + PySpark ya configurado
FROM jupyter/pyspark-notebook:latest

# Cambia al usuario administrador (root) para poder instalar programas
USER root

# 1. Actualiza repositorios e instala herramientas básicas, instala Google Chrome y librerías necesarias
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    ca-certificates && \
    mkdir -p /etc/apt/keyrings && \
    wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y \
    google-chrome-stable \
    libnss3 \
    libgbm1 \
    libasound2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# 2. Instala librerías de Python necesarias
RUN pip install selenium pymongo webdriver-manager

# Vuelve al usuario normal de Jupyter (buena práctica de seguridad)
USER jovyan
>>>>>>> 8fd6febbced5157e0ad155e84b9eabe5f03842d1
