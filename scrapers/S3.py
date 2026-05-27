import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scraper_zooplus(driver, By, paginas=1):
    datos_totales = []
    base_url = "https://www.zooplus.es/shop/perros/pienso_perros/dieta_veterinaria"
    wait = WebDriverWait(driver, 25)

    for p in range(1, paginas + 1):
        try:
            driver.get(base_url)
            # Espera a que el contenedor que viste en el inspector aparezca
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="ProductCard_productCard"]')))

            # Usamos JS para capturar la estructura modular que viste
            script_js = """
            let items = document.querySelectorAll('div[class*="ProductCard_productCard"]');
            return Array.from(items).map(item => {
                let nombre = item.querySelector('span[class*="ProductCard_productTitle"]')?.innerText.trim() || "Sin nombre";
                let formato = item.querySelector('p[class*="ProductCard_productDesc"]')?.innerText.trim() || "";
                
                // El precio suele estar en un bloque de precio dedicado
                let precio = item.querySelector('[class*="priceBlock"], [class*="Price"]')?.innerText.replace(/[^0-9.,]/g, "").replace(",", ".") || "0.0";
                
                // Rating y Opiniones (basado en el bloque Rating que se ve abajo en tu imagen)
                let ratingBlock = item.querySelector('[class*="ProductCard_productRating"]');
                let rating = ratingBlock?.innerText.match(/(\d+[\.,]?\d*)/)?.[0] || "0";
                
                return {
                    sku_id: nombre,
                    precio_raw: precio,
                    marca: "Zooplus_Veterinary",
                    formato_raw: formato ? formato : nombre,
                    rating: rating,
                    opiniones: "Consultar Web" // Zooplus a veces oculta el conteo en el texto del rating
                };
            });
            """
            items_pagina = driver.execute_script(script_js)

            for item in items_pagina:
                datos_totales.append({
                    **item,
                    "tienda": "Zooplus",
                    "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                
        except Exception as e:
            print(f"⚠️ Error en Zooplus: {e}")
            break 
            
    return datos_totales