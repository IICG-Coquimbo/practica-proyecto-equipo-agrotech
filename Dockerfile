# Imagen base
FROM jupyter/pyspark-notebook:latest

USER root

# Instalar Chrome + dependencias + entorno gráfico
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    ca-certificates \
    xvfb \
    fluxbox \
    x11vnc \
    net-tools \
    && mkdir -p /etc/apt/keyrings && \
    wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y \
    google-chrome-stable \
    libnss3 \
    libgbm1 \
    libasound2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Python libs
RUN pip install selenium pymongo webdriver-manager

# Script para iniciar VNC automáticamente
RUN echo '#!/bin/bash\n\
Xvfb :1 -screen 0 1024x768x16 &\n\
fluxbox &\n\
x11vnc -display :1 -nopw -listen 0.0.0.0 -xkb -forever &\n\
start-notebook.sh\n\
' > /start.sh && chmod +x /start.sh

USER jovyan

CMD ["/start.sh"]