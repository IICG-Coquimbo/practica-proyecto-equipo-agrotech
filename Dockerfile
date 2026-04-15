FROM jupyter/pyspark-notebook:latest

USER root

# Instala entorno gráfico + Chrome + dependencias
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    ca-certificates \
    xvfb \
    fluxbox \
    x11vnc \
    supervisor \
    python3-websockify \
    novnc \
    libnss3 \
    libgbm1 \
    libasound2 \
    sed \
    && mkdir -p /etc/apt/keyrings \
    && wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Librerías Python
RUN pip install selenium pymongo webdriver-manager pandas

# Variables entorno gráfico
ENV DISPLAY=:99
ENV SCREEN_WIDTH=1366
ENV SCREEN_HEIGHT=768
ENV SCREEN_DEPTH=24

# Script para iniciar entorno visual
RUN echo '#!/bin/bash\n\
Xvfb :99 -screen 0 ${SCREEN_WIDTH}x${SCREEN_HEIGHT}x${SCREEN_DEPTH} &\n\
fluxbox &\n\
x11vnc -display :99 -nopw -listen 0.0.0.0 -xkb -forever &\n\
websockify --web=/usr/share/novnc/ 6080 localhost:5900 &\n\
start-notebook.sh\n\
' > /start.sh && chmod +x /start.sh

# Puertos
EXPOSE 8888 6080 4040

USER jovyan

CMD bash -c "Xvfb :99 -screen 0 1366x768x24 & fluxbox & x11vnc -display :99 -nopw -listen 0.0.0.0 -xkb -forever & websockify --web=/usr/share/novnc/ 6080 localhost:5900 & start-notebook.sh"