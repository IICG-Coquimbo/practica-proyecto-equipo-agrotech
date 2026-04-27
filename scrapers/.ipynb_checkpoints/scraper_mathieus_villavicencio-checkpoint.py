import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
import re
import os
from collections import OrderedDict

def ejecutar_extraccion():
    """
    Función integrada para extraer precios de productos (Tomate/Papa) y 
    commodities (Potasio/Fosfato/Urea) según el formato AgroTech.
    """
    # --- CONFIGURACIÓN GENERAL ---
    NOMBRE_INTEGRANTE = "Sebastián Castillo"
    NOMBRE_GRUPO = "AgroTech"
    
    # Archivos Excel (ODEPA)
    ARCHIVOS_EXCEL = [
        "tomate-precio-consumidor_semanal_202501-202552.xlsx",
        "papa-precio-consumidor_semanal_202502-202552.xlsx"
    ]
    
    # URLs para Web Scraping (IndexMundi)
    URLS_WEB = {
        "Cloruro de potasio": "https://www.indexmundi.com/es/precios-de-mercado/?mercancia=cloruro-de-potasio&meses=240&moneda=clp",
        "Fosfato diamónico": "https://www.indexmundi.com/es/precios-de-mercado/?mercancia=fosfato-diamonico&meses=360&moneda=clp",
        "Urea": "https://www.indexmundi.com/es/precios-de-mercado/?mercancia=urea&meses=240&moneda=clp"
    }

    # Diccionarios de traducción de meses
    MESES_NUM = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
                 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
    
    MESES_ABR = {"ene.": "enero", "feb.": "febrero", "mar.": "marzo", "abr.": "abril", "may.": "mayo", "jun.": "junio",
                 "jul.": "julio", "ago.": "agosto", "sep.": "septiembre", "oct.": "octubre", "nov.": "noviembre", "dic.": "diciembre"}

    datos_finales = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # --- PARTE 1: PROCESAMIENTO EXCEL (Tomate y Papa) ---
    for nombre_archivo in ARCHIVOS_EXCEL:
        try:
            if os.path.exists(nombre_archivo):
                df = pd.read_excel(nombre_archivo, skiprows=4)
                df_step = df[['Fecha inicio', 'Producto', 'Precio promedio']].dropna().copy()
                df_step['Fecha inicio'] = pd.to_datetime(df_step['Fecha inicio'], dayfirst=True)
                
                for _, fila in df_step.iterrows():
                    precio = float(str(fila["Precio promedio"]).replace('.', '').replace(',', '.'))
                    datos_finales.append({
                        "integrante": NOMBRE_INTEGRANTE,
                        "producto": fila["Producto"],
                        "mes": MESES_NUM[fila["Fecha inicio"].month],
                        "valor": precio,
                        "fecha_captura": time.strftime("%Y-%m-%d")
                    })
        except Exception as e:
            print(f"❌ Error en Excel {nombre_archivo}: {e}")

    # --- PARTE 2: PROCESAMIENTO WEB (Potasio, Fosfato, Urea) ---
    for producto, url in URLS_WEB.items():
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            filas = soup.find_all('tr')
            
            for fila in filas:
                celdas = fila.find_all(['td', 'th'])
                if len(celdas) >= 2:
                    texto_fecha = celdas[0].get_text(strip=True).lower()
                    match = re.search(r'([a-z]{3}\.?)\s+(\d{4})', texto_fecha)
                    
                    if match:
                        abr_mes = match.group(1) if match.group(1).endswith('.') else match.group(1) + "."
                        anio = int(match.group(2))
                        
                        if anio >= 2020:
                            precio_raw = celdas[1].get_text(strip=True)
                            if precio_raw and any(char.isdigit() for char in precio_raw):
                                precio = float(precio_raw.replace('.', '').replace(',', '.'))
                                
                                datos_finales.append({
                                    "integrante": NOMBRE_INTEGRANTE,
                                    "producto": producto,
                                    "mes": MESES_ABR.get(abr_mes, abr_mes),
                                    "valor": precio,
                                    "fecha_captura": time.strftime("%Y-%m-%d")
                                })
        except Exception as e:
            print(f"❌ Error en Scraping {producto}: {e}")

    return datos_finales