# Imagen base con Jupyter + PySpark
FROM jupyter/pyspark-notebook:latest

USER root

# Instala entorno visual, supervisor y Chrome
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

# Descarga el conector MongoDB-Spark compatible con Spark 3.5
RUN wget -P /usr/local/spark/jars/ \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar && \
    wget -P /usr/local/spark/jars/ \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar && \
    wget -P /usr/local/spark/jars/ \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar && \
    wget -P /usr/local/spark/jars/ \
    https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar

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

# Verifica que noVNC esté en la ruta correcta
RUN ln -sf /usr/share/novnc/vnc.html /usr/share/novnc/index.html \
    && which websockify || echo "websockify no encontrado"


# Inicia supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

RUN ln -sf /usr/bin/python3 /usr/bin/python