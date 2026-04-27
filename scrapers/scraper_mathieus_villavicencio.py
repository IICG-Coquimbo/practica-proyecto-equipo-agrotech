import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def ejecutar_extraccion():

    NOMBRE_INTEGRANTE = "Mathieus Villavicencio"
    NOMBRE_GRUPO = "AgroTech"

    datos_finales = []

    fecha_actual = time.strftime("%Y-%m-%d %H:%M:%S")

    meses = [
        "Enero","Febrero","Marzo","Abril","Mayo","Junio",
        "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
    ]

    # LIMPIEZA
    os.system("pkill -9 chrome")
    os.system("pkill -9 chromedriver")
    os.system("pkill -9 brave")

    options = Options()
    options.binary_location = "/usr/bin/brave-browser"

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")

    driver = None

    try:
        driver = webdriver.Chrome(options=options)

        # =========================
        # INDICE UV
        # =========================
        url_uv = "https://climatologia.meteochile.gob.cl/application/anual/indiceUvbMaximoAnual/290004/2025"
        driver.get(url_uv)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )

        filas = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        for fila in filas:
            columnas = fila.find_elements(By.TAG_NAME, "td")

            if len(columnas) >= 13:
                try:
                    int(columnas[0].text.strip())
                except:
                    continue

                for i in range(1, 13):
                    valor = columnas[i].text.strip()

                    if valor:
                        try:
                            valor_float = float(valor.replace(",", "."))
                        except:
                            continue

                        datos_finales.append({
                            "integrante": NOMBRE_INTEGRANTE,
                            "etiqueta": "Indice UV",
                            "mes": meses[i - 1],
                            "valor": valor_float,
                            "fecha_captura": fecha_actual
                        })

        # =========================
        # HUMEDAD
        # =========================
        url_humedad = "https://climatologia.meteochile.gob.cl/application/anual/humedadAnual/290004/2025"
        driver.get(url_humedad)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )

        filas = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        for fila in filas:
            columnas = fila.find_elements(By.TAG_NAME, "td")

            if len(columnas) >= 13:
                try:
                    int(columnas[0].text.strip())
                except:
                    continue

                for i in range(1, 13):
                    valor = columnas[i].text.strip()

                    if valor:
                        try:
                            valor_float = float(valor.replace(",", "."))
                        except:
                            continue

                        datos_finales.append({
                            "integrante": NOMBRE_INTEGRANTE,
                            "etiqueta": "Humedad",
                            "mes": meses[i - 1],
                            "valor": valor_float,
                            "fecha_captura": fecha_actual
                        })

        print(f"✅ Extracción terminada: {len(datos_finales)} registros")

    except Exception as e:
        print("❌ Error en Selenium:", e)

    finally:
        if driver:
            driver.quit()

    return datos_finales