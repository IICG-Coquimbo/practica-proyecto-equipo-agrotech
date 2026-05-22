import time
import random
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scraper_miscota(driver, By, paginas=1):
    datos_tienda = []
    base_url = "https://www.miscota.es/perros/s_pienso_naturales16436"
    wait = WebDriverWait(driver, 25) # Aumentamos el tiempo de espera

    for p in range(1, paginas + 1):
        url = f"{base_url}?p={p}" if p > 1 else base_url
        print(f"--- [Miscota] Procesando Página {p} ---")
        
        try:
            driver.get(url)
            
            # TRUCO: Scroll suave para disparar la carga de imágenes y scripts de la página
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(random.uniform(2, 4)) 

            # Esperamos específicamente al contenedor que vimos en tu captura (imagen_2.png)
            # Usamos presence_of_all_elements_located para asegurar que hay datos
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "li.prod_box")))

            script_js = """
            let boxes = document.querySelectorAll('li.prod_box');
            return Array.from(boxes).map(box => {
                let marca = box.querySelector('.prod-name--brand')?.innerText.trim() || "S/M";
                let nombre = box.querySelector('.prod-name--title')?.innerText.trim() || "Sin nombre";
                
                // En Miscota el precio puede estar en diferentes spans según la oferta
                let precio_el = box.querySelector('.price, .prod_box--price, .current-price');
                let precio = precio_el?.innerText.replace(/[^0-9.,]/g, "").replace(",", ".") || "0.0";
                
                // Captura de Rating y Opiniones para tu análisis prescriptivo
                let rating = box.querySelector('[data-rating]')?.getAttribute('data-rating') || "0";
                let opiniones = box.querySelector('.reviews-count')?.innerText.replace(/[^0-9]/g, "") || "0";

                return {
                    sku_id: nombre,
                    marca: marca,
                    precio_raw: precio,
                    formato_raw: nombre, 
                    rating: rating,
                    opiniones: opiniones
                };
            });
            """
            
            items = driver.execute_script(script_js)

            if not items:
                print(f"   ⚠️ Página {p} cargó vacía, reintentando scroll...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                items = driver.execute_script(script_js)

            for item in items:
                datos_tienda.append({
                    **item,
                    "tienda": "Miscota",
                    "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S")
                })

        except Exception as e:
            # Si el error persiste, tomamos una captura para que puedas ver qué pasó
            driver.save_screenshot(f"error_miscota_p{p}.png")
            print(f"   ⚠️ Error en Miscota P{p}. Revisa 'error_miscota_p{p}.png'")
            continue
            
    return datos_tienda