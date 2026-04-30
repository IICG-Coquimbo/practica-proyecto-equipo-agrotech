import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
import re
import os

def ejecutar_extraccion():
    """
    Función integral que extrae datos de ODEPA (Excel) e IndexMundi (Web)
    en una sola estructura de datos.
    """
    # --- CONFIGURACIÓN GENERAL ---
    NOMBRE_INTEGRANTE = "Sebastián Castillo"
    fecha_hoy = time.strftime("%Y-%m-%d")
    
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        BASE_DIR = os.getcwd()

    # Configuración ODEPA
    ARCHIVOS_EXCEL = [
        "tomate-papa-precio-consumidor_semanal_202402-202452.xlsx",
        "tomate-papa-precio-consumidor_semanal_202502-202552.xlsx"
    ]
    
    # Configuración IndexMundi (Solo Potasio y Urea)
    URLS_WEB = {
        "Cloruro de potasio": "https://www.indexmundi.com/es/precios-de-mercado/?mercancia=cloruro-de-potasio&meses=240&moneda=clp",
        "Urea": "https://www.indexmundi.com/es/precios-de-mercado/?mercancia=urea&meses=240&moneda=clp"
    }

    # Diccionarios de conversión
    MESES_NUM = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
                 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
    
    MESES_ABR = {"ene.": "enero", "feb.": "febrero", "mar.": "marzo", "abr.": "abril", "may.": "mayo", "jun.": "junio",
                 "jul.": "julio", "ago.": "agosto", "sep.": "septiembre", "oct.": "octubre", "nov.": "noviembre", "dic.": "diciembre"}

    datos_finales = []

    # --- PARTE 1: ODEPA (EXCEL) ---
    for nombre_archivo in ARCHIVOS_EXCEL:
        ruta_completa = os.path.join(BASE_DIR, nombre_archivo)
        if os.path.exists(ruta_completa):
            try:
                df = pd.read_excel(ruta_completa, skiprows=4)
                df_step = df[['Fecha inicio', 'Producto', 'Sector', 'Variedad', 'Calidad', 'Tipo punto monitoreo', 'Precio promedio']].dropna(subset=['Precio promedio']).copy()
                df_step['Fecha inicio'] = pd.to_datetime(df_step['Fecha inicio'], dayfirst=True)
                
                for _, fila in df_step.iterrows():
                    precio_val = fila["Precio promedio"]
                    # Mantenemos float para ODEPA por precisión en precios minoristas
                    precio = float(precio_val) if isinstance(precio_val, (int, float)) else float(str(precio_val).replace('.', '').replace(',', '.'))
                    f = fila["Fecha inicio"]
                    
                    datos_finales.append({
                        "integrante": NOMBRE_INTEGRANTE,
                        "producto": fila["Producto"],
                        "comuna": fila["Sector"],
                        "variedad": fila["Variedad"],
                        "calidad": fila["Calidad"],
                        "lugar_monitoreo": fila["Tipo punto monitoreo"],
                        "fecha": f.strftime("%Y-%m-%d"),
                        "mes": MESES_NUM[f.month],
                        "año": f.year,
                        "precio": precio,
                        "fecha_captura": fecha_hoy
                    })
                print(f"✅ ODEPA: {nombre_archivo} procesado.")
            except Exception as e: print(f"❌ Error ODEPA {nombre_archivo}: {e}")

    # --- PARTE 2: INDEXMUNDI (WEB) ---
    headers = {"User-Agent": "Mozilla/5.0"}
    for fertilizante, url in URLS_WEB.items():
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            filas = soup.find_all('tr')
            
            for fila in filas:
                celdas = fila.find_all(['td', 'th'])
                if len(celdas) >= 3:
                    texto_fecha = celdas[0].get_text(strip=True).lower()
                    match = re.search(r'([a-z]{3}\.?)\s+(\d{4})', texto_fecha)
                    
                    if match:
                        abr_mes = match.group(1) if match.group(1).endswith('.') else match.group(1) + "."
                        anio = int(match.group(2))
                        
                        if anio in [2024, 2025]:
                            p_raw = celdas[1].get_text(strip=True)
                            # Convertimos a entero para IndexMundi
                            precio = int(round(float(p_raw.replace('.', '').replace(',', '.'))))
                            
                            t_raw = celdas[2].get_text(strip=True).replace('%', '')
                            tasa = float(t_raw.replace(',', '.'))
                            
                            nombre_mes = MESES_ABR.get(abr_mes, abr_mes)
                            
                            datos_finales.append({
                                "integrante": NOMBRE_INTEGRANTE,
                                "fertilizante": fertilizante,
                                "precio": precio,
                                "mes": nombre_mes,
                                "año": anio,
                                "tasa_cambio": tasa,
                                "fecha_captura": fecha_hoy
                            })
            print(f"✅ IndexMundi: {fertilizante} scrapeado.")
        except Exception as e: print(f"❌ Error IndexMundi {fertilizante}: {e}")

    return datos_finales

# --- BLOQUE DE VISUALIZACIÓN ---
if __name__ == "__main__":
    resultados = ejecutar_extraccion()
    df_total = pd.DataFrame(resultados)

    # Configuración para consola
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    print("\n" + "="*80)
    print("MUESTRA DE REGISTROS (ODEPA)")
    print("="*80)
    print(df_total[df_total['producto'].notna()].head(5).to_string(index=False))

    print("\n" + "="*80)
    print("MUESTRA DE REGISTROS (INDEXMUNDI - PRECIOS ENTEROS)")
    print("="*80)
    print(df_total[df_total['fertilizante'].notna()].head(10).to_string(index=False))
    print("="*80)