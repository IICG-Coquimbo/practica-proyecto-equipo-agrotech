#!/bin/bash
# Script de inicio mejorado - maneja errores sin entrar en bucle

# NO usar set -e para evitar que supervisord entre en bucle de reinitentos
# set -e

# Define la pantalla virtual y directorio runtime
export DISPLAY=:99
export XDG_RUNTIME_DIR=/tmp/runtime-root

# Crea el directorio runtime
mkdir -p /tmp/runtime-root
chmod 700 /tmp/runtime-root

# Limpia procesos y locks previos
echo "Limpiando procesos previos..."
pkill Xvfb 2>/dev/null || true
pkill fluxbox 2>/dev/null || true
pkill x11vnc 2>/dev/null || true
pkill websockify 2>/dev/null || true
rm -f /tmp/.X99-lock
rm -f /tmp/.X11-unix/X99
sleep 2

echo "Iniciando Xvfb..."
Xvfb :99 -screen 0 1366x768x24 &
sleep 3

echo "Iniciando fluxbox..."
fluxbox &
sleep 1

echo "Iniciando x11vnc..."
x11vnc -display :99 -forever -shared -nopw -listen 0.0.0.0 -rfbport 5900 &
sleep 1

echo "Iniciando noVNC..."
websockify --web=/usr/share/novnc/ 6080 localhost:5900 2>/dev/null &
sleep 1

echo "Iniciando Jupyter..."
# Cambiar al directorio de trabajo
cd /home/jovyan/work 2>/dev/null || cd /home/jovyan

# Iniciar Jupyter con configuración explícita
exec start-notebook.sh \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --ServerApp.token='' \
    --ServerApp.password='' \
    --ServerApp.allow_origin='*' \
    --ServerApp.disable_check_xsrf=True