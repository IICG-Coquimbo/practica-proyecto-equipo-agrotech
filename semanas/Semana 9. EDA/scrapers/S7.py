import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scraper_superzoo(driver, By, paginas=1):
    datos_tienda = []
    base_url = "https://www.superzoo.cl/perro/alimentos/alimentos-seco"
    wait = WebDriverWait(driver, 20)

    for p in range(1, paginas + 1):
        # Superzoo suele usar ?start= (en incrementos de 24 o 36) o ?page=
        url = f"{base_url}?page={p}" if p > 1 else base_url
        print(f"--- [Superzoo Chile] Procesando Página {p} ---")
        
        try:
            driver.get(url)
            # Esperamos al contenedor de producto
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-tile")))

            script_js = """
            let tiles = document.querySelectorAll('div.product-tile');
            return Array.from(tiles).map(tile => {
                let nombre = tile.querySelector('h2.text-base')?.innerText.trim() || null;
                
                // Extraemos el valor numérico del atributo 'content' para evitar el signo $
                let precio_el = tile.querySelector('span.sales span.value');
                let precio = precio_el?.getAttribute('content') || precio_el?.innerText.replace(/[^0-9]/g, "") || null;
                
                // Forzamos datos nulos intencionalmente si no hay estrellas, para tu EDA
                let rating = tile.querySelector('.tile-ratings .rating-stars')?.getAttribute('data-rating') || null;
                
                return {
                    sku_id: nombre,
                    marca: "Pendiente_Spark", // Lo limpiarás en el EDA
                    precio_raw: precio,
                    formato_raw: nombre, 
                    rating: rating,
                    opiniones: null, // Dato vacío para análisis de valores faltantes
                    moneda: "CLP"
                };
            });
            """
            
            items = driver.execute_script(script_js)

            for item in items:
                datos_tienda.append({
                    **item,
                    "tienda": "Superzoo_CL",
                    "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S")
                })

        except Exception as e:
            print(f"⚠️ Error en Superzoo P{p}: {e}")
            continue
            
    return datos_tienda