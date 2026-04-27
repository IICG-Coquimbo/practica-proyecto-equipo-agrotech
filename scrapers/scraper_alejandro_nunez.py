import pandas as pd
import time
import re
import os
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def ejecutar_extraccion():
    """
    Script AgroTech: Procesamiento de archivos ODEPA de Naranjas.
    """
    # --- CONFIGURACIÓN GENERAL ---
    NOMBRE_INTEGRANTE = "Alejandro Nuñez"
<<<<<<< HEAD
    
=======

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
    # Archivos de Naranjas (ODEPA)
    ARCHIVOS_EXCEL = [
        "naranja_super_precio-consumidor_semanal_202502-202552.xlsx",
        "naranjas_precio-consumidor_semanal_202501-202552.xlsx"
    ]
<<<<<<< HEAD
    
=======

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
    # Diccionario de traducción de meses
    MESES_NUM = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }

    datos_finales = []

    print(f"Iniciando extracción para integrante {NOMBRE_INTEGRANTE}...")

    # --- PROCESAMIENTO EXCEL (Naranjas) ---
    for nombre_archivo in ARCHIVOS_EXCEL:
<<<<<<< HEAD
        if os.path.exists(nombre_archivo):
            try:
                # ODEPA suele tener encabezados en la fila 4 (skiprows=4)
                df = pd.read_excel(nombre_archivo, skiprows=4)
                
                columnas_interes = ['Fecha término', 'Producto', 'Precio promedio']
                
=======
        # FIX: construimos la ruta completa uniendo BASE_DIR + nombre del archivo
        ruta_completa = os.path.join(BASE_DIR, nombre_archivo)

        if os.path.exists(ruta_completa):  # FIX: verificamos la ruta completa
            try:
                # ODEPA suele tener encabezados en la fila 4 (skiprows=4)
                df = pd.read_excel(ruta_completa, skiprows=4)  # FIX: leemos la ruta completa

                columnas_interes = ['Fecha término', 'Producto', 'Precio promedio']

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
                if not all(col in df.columns for col in columnas_interes):
                    print(f"❌ Error: El archivo {nombre_archivo} no tiene las columnas esperadas.")
                    continue

                df_step = df[columnas_interes].dropna().copy()

<<<<<<< HEAD
                # 🔧 Renombramos la columna 'Producto' a 'Etiqueta'
                df_step = df_step.rename(columns={"Producto": "Etiqueta"})

                df_step['Fecha término'] = pd.to_datetime(df_step['Fecha término'], dayfirst=True)
                
                for _, fila in df_step.iterrows():
                    precio_str = str(fila["Precio promedio"]).replace('.', '').replace(',', '.')
                    precio_limpio = re.sub(r'[^\d.]', '', precio_str)
                    
                    if precio_limpio:
                        precio = float(precio_limpio)
                        
                        datos_finales.append({
                            "integrante": NOMBRE_INTEGRANTE,
                            "etiqueta": fila["Etiqueta"],   # 👈 ahora usamos 'Etiqueta'
=======
                # Renombramos la columna 'Producto' a 'Etiqueta'
                df_step = df_step.rename(columns={"Producto": "Etiqueta"})

                df_step['Fecha término'] = pd.to_datetime(df_step['Fecha término'], dayfirst=True)

                for _, fila in df_step.iterrows():
                    precio_str = str(fila["Precio promedio"]).replace('.', '').replace(',', '.')
                    precio_limpio = re.sub(r'[^\d.]', '', precio_str)

                    if precio_limpio:
                        precio = float(precio_limpio)

                        datos_finales.append({
                            "integrante": NOMBRE_INTEGRANTE,
                            "etiqueta": fila["Etiqueta"],
>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
                            "mes": MESES_NUM[fila["Fecha término"].month],
                            "valor": precio,
                            "fecha_captura": time.strftime("%Y-%m-%d")
                        })
<<<<<<< HEAD
                
                print(f"✅ Procesado exitosamente: {nombre_archivo}")
                
=======

                print(f"✅ Procesado exitosamente: {nombre_archivo}")

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
            except Exception as e:
                print(f"❌ Error al procesar {nombre_archivo}: {e}")
        else:
            print(f"⚠️ Archivo no encontrado en la carpeta: {nombre_archivo}")

    return datos_finales

<<<<<<< HEAD
=======

>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
if __name__ == "__main__":
    lista_resultados = ejecutar_extraccion()

    if lista_resultados:
        df_resultado = pd.DataFrame(lista_resultados)
<<<<<<< HEAD
        
        print("\n" + "="*45)
        print("📊 REPORTE FINAL DE REGISTROS")
        print("="*45)
        
        # 🔧 Conteo ahora sobre 'etiqueta'
        conteo = df_resultado['etiqueta'].value_counts()
        print("Cantidad de registros por etiqueta:")
        print(conteo)
        
        print("-" * 45)
        print(f"TOTAL GENERAL: {len(df_resultado)} registros cargados correctamente.")
        print("-" * 45)
        
        print("\n🔍 Vista previa de los datos extraídos:")
        print(df_resultado.head(10))
    else:
        print("\n❌ No se extrajo ningún dato. Verifica que los archivos Excel tengan la columna 'Fecha término'.")
=======

        print("\n" + "="*45)
        print("📊 REPORTE FINAL DE REGISTROS")
        print("="*45)

        conteo = df_resultado['etiqueta'].value_counts()
        print("Cantidad de registros por etiqueta:")
        print(conteo)

        print("-" * 45)
        print(f"TOTAL GENERAL: {len(df_resultado)} registros cargados correctamente.")
        print("-" * 45)

        print("\n🔍 Vista previa de los datos extraídos:")
        print(df_resultado.head(10))
    else:
        print("\n❌ No se extrajo ningún dato. Verifica que los archivos Excel tengan la columna 'Fecha término'.")
>>>>>>> 9f6c75c (Arreglo del Dockerfile y prueba del main.py)
