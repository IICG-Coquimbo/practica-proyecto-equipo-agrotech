import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scraper_bitiba(driver, By, paginas=1):
    datos_tienda = []
    # URL típica de dietas veterinarias en Bitiba
    base_url = "https://www.bitiba.es/shop/perros/pienso_perros/dieta_veterinaria"
    wait = WebDriverWait(driver, 25)

    for p in range(1, paginas + 1):
        url = f"{base_url}?p={p}" if p > 1 else base_url
        print(f"--- [Bitiba] Procesando Página {p} ---")
        
        try:
            driver.get(url)
            # Esperamos el contenedor modular que identificaste
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="ProductCard_productCard"]')))

            # Extracción por JS para capturar rating y peso por separado
            script_js = """
            let cards = document.querySelectorAll('div[class*="ProductCard_productCard"]');
            return Array.from(cards).map(card => {
                let nombre = card.querySelector('span[class*="ProductCard_productTitle"]')?.innerText.trim() || "Sin nombre";
                let formato = card.querySelector('p[class*="ProductCard_productDesc"]')?.innerText.trim() || "";
                
                // El precio suele estar en bloques con clases 'price' o 'Price'
                let precio_el = card.querySelector('[class*="price"], [class*="PriceBlock"]');
                let precio = precio_el?.innerText.replace(/[^0-9.,]/g, "").replace(",", ".") || "0.0";
                
                // Rating y Opiniones (siguiendo el estándar de la plataforma)
                let ratingEl = card.querySelector('[class*="Rating"], [data-zta="rating-link"]');
                let rating = ratingEl?.innerText.match(/(\d+[\.,]?\d*)/)?.[0] || "0";
                
                return {
                    sku_id: nombre,
                    precio_raw: precio,
                    marca: "Bitiba_Veterinary",
                    formato_raw: formato ? formato : nombre,
                    rating: rating,
                    opiniones: "Ver en tienda"
                };
            });
            """
            
            items = driver.execute_script(script_js)

            for item in items:
                datos_tienda.append({
                    **item,
                    "tienda": "Bitiba",
                    "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S")
                })

        except Exception as e:
            print(f"⚠️ Error en Bitiba página {p}: {e}")
            continue
            
    return datos_tienda