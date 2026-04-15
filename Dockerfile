

# Imagen base: trae Jupyter + Python + PySpark ya configurado
FROM jupyter/pyspark-notebook:latest

USER root

# 1. Actualiza repositorios e instala herramientas básicas + Google Chrome
# Instala entorno visual, supervisor y Chrome
>>>>>>> 485ee9a1aaeac23febbe254e844320d427e7b398
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

# Instala librerías Python para scraping y MongoDB
RUN pip install selenium pymongo webdriver-manager pandas

# Variables del entorno gráfico
ENV DISPLAY=:99
ENV SCREEN_WIDTH=1366
ENV SCREEN_HEIGHT=768
ENV SCREEN_DEPTH=24

# Copia archivos de inicio
COPY start-vnc.sh /usr/local/bin/start-vnc.sh
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Convierte saltos de línea Windows a Linux y da permisos
RUN sed -i 's/\r$//' /usr/local/bin/start-vnc.sh && chmod +x /usr/local/bin/start-vnc.sh

# Puertos del contenedor
EXPOSE 8888 5900 6080 4040

# Inicia supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
=======
# 1. Instalar dependencias base y configurar el repo de Google Chrome
RUN apt-get update && apt-get install -y wget gnupg2 curl && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list

# 2. Instalar Google Chrome y librerías de soporte
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    libnss3 \
    libgbm1 \
    libasound2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Instala librerías de Python necesarias
RUN pip install selenium pymongo webdriver-manager

# Vuelve al usuario normal de Jupyter (buena práctica de seguridad)
USER jovyan
=======
    libasound2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 3. Instalar librerías de Python
RUN pip install selenium pymongo webdriver-manager

# Vuelve al usuario normal de Jupyter (buena pr�ctica de seguridad)
USER jovyan

