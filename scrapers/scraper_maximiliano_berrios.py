import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

def ejecutar_extraccion():
    """
    Scraper adaptado a ENAP (Diésel)
    Formato: _id, integrante, etiqueta, mes, valor, fecha_captura
    """

    # --- CONFIGURACIÓN ---
    NOMBRE_INTEGRANTE = "Maximiliano Berrios"
    ETIQUETA = "Diésel ENAP"

    URL = "https://www.enap.cl/tabla-de-precios-de-paridad-historico"

    MESES_MAP = {
        "01": "enero", "02": "febrero", "03": "marzo",
        "04": "abril", "05": "mayo", "06": "junio",
        "07": "julio", "08": "agosto", "09": "septiembre",
        "10": "octubre", "11": "noviembre", "12": "diciembre"
    }

    datos_finales = []

    # --- REQUEST ---
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(URL, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    filas = soup.select("table tbody tr")

    print(f"Filas encontradas: {len(filas)}")

    for fila in filas:
        try:
            columnas = fila.find_all("td")

            if len(columnas) >= 5:
                fecha_raw = columnas[0].get_text(strip=True)
                diesel_raw = columnas[4].get_text(strip=True)

                if diesel_raw != "":
                    # --- LIMPIAR PRECIO ---
                    valor = diesel_raw.replace(".", "").replace(",", "").strip()
                    precio = float(valor) if valor.isdigit() else 0.0

                    # --- PROCESAR FECHA ---
                    partes = fecha_raw.split("-")
                    if len(partes) == 3:
                        dia, mes_num, año = partes
                        mes = MESES_MAP.get(mes_num, mes_num)
                    else:
                        mes = "desconocido"

                    # --- FORMATO FINAL ---
                    datos_finales.append({
                        "integrante": NOMBRE_INTEGRANTE,
                        "etiqueta": ETIQUETA,
                        "mes": mes,
                        "valor": precio,
                        "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
        except:
            continue

    return datos_finales
<<<<<<< HEAD
        datos = ejecutar_extraccion()
        len(datos)
        df = pd.DataFrame(datos)
        df
=======

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
