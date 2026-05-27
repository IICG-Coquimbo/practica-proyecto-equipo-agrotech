import os
import time
import certifi
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from pymongo import MongoClient

# --- 1. IMPORTACIÓN DE TUS SCRAPERS ---
# Asegúrate de que los archivos en /scrapers se llamen S1.py, S2.py, etc.
from scrapers.S1 import scraper_tiendanimal
from scrapers.S2 import scraper_kiwoko
from scrapers.S3 import scraper_zooplus
from scrapers.S4 import scraper_amazon_mascotas
from scrapers.S5 import scraper_miscota
from scrapers.S6 import scraper_bitiba
from scrapers.S7 import scraper_superzoo

# --- 2. LIMPIEZA DE PROCESOS ---
try:
    os.system("pkill -9 chrome")
    os.system("pkill -9 chromedriver")
    os.system("rm -rf /tmp/.com.google.Chrome.*")
    os.system("rm -rf /tmp/.org.chromium.Chromium.*")
    print("🧹 Limpieza de procesos y temporales completada.")
except:
    pass

# --- 3. CONFIGURACIÓN DEL DRIVER ---
options = Options()
options.binary_location = "/usr/bin/google-chrome"

options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--disable-software-rasterizer")
options.add_argument("--window-size=1920,1080")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

driver = None
datos_totales = []

try:
    # Iniciar Navegador
    driver = webdriver.Chrome(options=options)
    print("🚀 Navegador iniciado correctamente.")

    # --- 4. EJECUCIÓN COLECTIVA ---
    print("🔍 Iniciando extracción masiva de rubro Mascotas...")
    
    # Ejecutamos cada scraper y extendemos la lista maestra
    # Nota: Se pasa 'By' para que los scrapers lo usen internamente
    try:
        datos_totales.extend(scraper_tiendanimal(driver, By, paginas=15))
        print(f"✅ Tiendanimal completado.")
        
        #datos_totales.extend(scraper_kiwoko(driver, By, paginas=15))
        #print(f"✅ Kiwoko completado.")
        
          
        #datos_totales.extend(scraper_amazon_mascotas(driver, By, paginas=15))
        #print(f"✅ Amazon completado.")

        #datos_totales.extend(scraper_zooplus(driver, By, paginas=1))
        #print(f"✅ Zooplus completado.")
        
        #datos_totales.extend(scraper_bitiba(driver, By, paginas=10))
        #print(f"✅ Bitiba completado.")
        
        #datos_totales.extend(scraper_miscota(driver, By, paginas=10))
        #print(f"✅ Miscota completado.")

        #datos_totales.extend(scraper_superzoo(driver, By, paginas=10))
        #print(f"✅ Superzoo.")
        
    except Exception as e:
        print(f"⚠️ Error durante la extracción de alguna fuente: {e}")

    print(f"📊 Total capturado: {len(datos_totales)} registros.")

    # --- 5. CARGA EN MONGO ATLAS ---
    if datos_totales:
        uri = "mongodb+srv://profe_vannessa:Ejemplo123@cluster0.kthdyh1.mongodb.net/?retryWrites=true&w=majority"
        
        client = MongoClient(uri, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
        
        try:
            # Validar conexión
            client.server_info() 
            db = client["ProyectoSemana9"]
            coleccion = db["Alimento_Mascotas_Raw"]
            
            print("📤 Subiendo datos a MongoDB Atlas...")
            coleccion.insert_many(datos_totales)
            print("✅ ¡Éxito! Datos cargados en la nube.")
            
        except Exception as e:
            print(f"❌ Error de conexión o subida a MongoDB: {e}")
    else:
        print("Empty 📭: No se recolectaron datos para subir.")

except Exception as e:
    print(f"❌ Error crítico en el sistema: {e}")

finally:
    # El cierre del driver SIEMPRE debe ir al final de todo el proceso
    if driver:
        driver.quit()
        print("🔒 Navegador cerrado correctamente.")