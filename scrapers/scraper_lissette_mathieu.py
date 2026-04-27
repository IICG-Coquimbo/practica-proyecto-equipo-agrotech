import pandas as pd
import time
import re
import os
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def ejecutar_extraccion():
    NOMBRE_INTEGRANTE = "Lissette Mathieu" 
    NOMBRE_GRUPO = "AgroTech"
<<<<<<< HEAD
    
=======

    # FIX: Ruta absoluta a la carpeta del scraper
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
    ARCHIVOS_EXCEL = [
        "precio-consumidor_semanal_202501-202616.xlsx",
        "precio-consumidor_semanal_202501-202616-supermercado.xlsx"
    ]
    
    MESES_NUM = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }

    datos_finales = []
    print(f"Iniciando extracción para el grupo {NOMBRE_GRUPO}...")

    for nombre_archivo in ARCHIVOS_EXCEL:
<<<<<<< HEAD
        if os.path.exists(nombre_archivo):
            try:
                df = pd.read_excel(nombre_archivo, skiprows=4)
=======
        ruta_completa = os.path.join(BASE_DIR, nombre_archivo)  # FIX

        if os.path.exists(ruta_completa):  # FIX
            try:
                df = pd.read_excel(ruta_completa, skiprows=4)  # FIX
>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
                
                columnas_interes = ['Fecha término', 'Producto', 'Precio promedio']
                
                if not all(col in df.columns for col in columnas_interes):
                    print(f"❌ Columnas encontradas: {df.columns.tolist()}")
                    continue

                df_step = df[columnas_interes].dropna().copy()
                df_step['Fecha término'] = pd.to_datetime(df_step['Fecha término'], dayfirst=True)
                
                for _, fila in df_step.iterrows():
                    precio_str = str(fila["Precio promedio"]).replace('.', '').replace(',', '.')
                    precio_limpio = re.sub(r'[^\d.]', '', precio_str)
                    
                    if precio_limpio:
                        datos_finales.append({
                            "integrante": NOMBRE_INTEGRANTE,
                            "etiqueta": fila["Producto"],
                            "mes": MESES_NUM[fila["Fecha término"].month],
                            "valor": float(precio_limpio),
                            "fecha_captura": time.strftime("%Y-%m-%d")
                        })
                
                print(f"✅ Archivo procesado: {nombre_archivo}")
                
            except Exception as e:
                print(f"❌ Error al procesar {nombre_archivo}: {e}")
        else:
            print(f"⚠️ Archivo no encontrado: {nombre_archivo}")

<<<<<<< HEAD
    return datos_finales
=======
    return datos_finales
>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
