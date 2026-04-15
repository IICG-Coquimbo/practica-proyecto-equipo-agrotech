#!/bin/bash
set -e

export DISPLAY=:99
export XDG_RUNTIME_DIR=/tmp/runtime-root

mkdir -p /tmp/runtime-root
chmod 700 /tmp/runtime-root

# Limpieza de procesos anteriores (por si supervisor reinicia el script)
pkill -f Xvfb || true
pkill -f x11vnc || true
pkill -f websockify || true
pkill -f fluxbox || true
rm -f /tmp/.X99-lock

sleep 1

echo "Iniciando Xvfb..."
Xvfb :99 -screen 0 1366x768x24 &

# Espera activa hasta que Xvfb esté listo
for i in $(seq 1 20); do
    xdpyinfo -display :99 >/dev/null 2>&1 && break
    echo "Esperando Xvfb... ($i)"
    sleep 0.5
done

echo "Iniciando fluxbox..."
fluxbox &

echo "Iniciando x11vnc..."
x11vnc -display :99 -forever -shared -nopw -listen 0.0.0.0 -rfbport 5900 &
sleep 2

echo "Iniciando noVNC..."
websockify --web=/usr/share/novnc/ 6080 localhost:5900 &

echo "Iniciando Jupyter..."
exec su jovyan -c "PATH=/opt/conda/bin:$PATH start-notebook.py --ip=0.0.0.0 --port=8888 --no-browser"