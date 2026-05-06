FROM jupyter/pyspark-notebook:latest

USER root

# 1. Herramientas de Red, SSL y Entorno Gráfico (Xvfb para el entorno VNC)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    curl \
    xvfb \
    fluxbox \
    x11vnc \
    supervisor \
    websockify \
    novnc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Instalamos Brave Browser (o Chromium) para el scraping
RUN curl -fsSLo /usr/share/keyrings/brave-browser-archive-keyring.gpg https://brave-browser-apt-release.s3.brave.com/brave-browser-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/brave-browser-archive-keyring.gpg] https://brave-browser-apt-release.s3.brave.com/ stable main" | tee /etc/apt/sources.list.d/brave-browser-release.list \
    && apt-get update && apt-get install -y brave-browser

# 3. Librerías de Python para el curso (Scraping + Atlas + Spark)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "pymongo[srv]" dnspython certifi selenium webdriver-manager pandas

# 4. Conectores Spark-MongoDB
RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar -P /usr/local/spark/jars/ \
    && wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar -P /usr/local/spark/jars/

# 5. Copiamos los archivos de configuración
COPY start-vnc.sh /usr/local/bin/start-vnc.sh
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# 6. Permisos de ejecución
RUN chmod +x /usr/local/bin/start-vnc.sh

# 7. Variables de entorno
ENV DISPLAY=:99

# 8. Supervisor lanza Jupyter y el entorno gráfico al mismo tiempo
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
