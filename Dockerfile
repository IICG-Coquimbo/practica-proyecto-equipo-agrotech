FROM jupyter/pyspark-notebook:latest

USER root

# 1. Instalar dependencias base y configurar el repo de Google Chrome
RUN apt-get update && apt-get install -y wget gnupg2 curl && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list

# 2. Instalar Google Chrome y librerías de soporte
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    libnss3 \
    libgbm1 \
    libasound2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Instalar librerías de Python
RUN pip install selenium pymongo webdriver-manager

USER jovyan