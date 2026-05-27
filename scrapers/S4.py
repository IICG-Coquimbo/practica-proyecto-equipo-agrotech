import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scraper_amazon_mascotas(driver, By, paginas=3):
    limite_paginas=paginas
    datos_recuperados = []
    NOMBRE_GRUPO = "Vannessa"
    url_busqueda = "https://www.amazon.es/s?k=comida+perros"
    wait = WebDriverWait(driver, 20)
    
    try:
        driver.get(url_busqueda)
        
        for nivel_pagina in range(limite_paginas):
            print(f"--- [Amazon] Procesando Página {nivel_pagina + 1} ---")

            # ESPERA EXPLÍCITA: En lugar de sleep fijo, esperamos el bloque de resultados
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']")))
            except:
                print(f"Timeout en página {nivel_pagina + 1}")
                continue

            # EXTRACCIÓN POR JAVASCRIPT: Más rápido y menos propenso a errores de "element not found"
            script_js = """
            let results = document.querySelectorAll("div[data-component-type='s-search-result']");
            return Array.from(results).map(b => {
                let nombre = b.querySelector("h2")?.innerText || "Sin nombre";
                let precioE = b.querySelector(".a-price-whole")?.innerText || "0";
                let precioD = b.querySelector(".a-price-fraction")?.innerText || "0";
                
                // Captura de Rating (Estrellas)
                // Amazon guarda esto en un span oculto o en el alt del icono
                let ratingText = b.querySelector(".a-icon-alt")?.innerText || "0";
                let ratingNum = ratingText.split(" ")[0].replace(",", "."); // Ej: "4.5 de 5" -> "4.5"

                // Captura de Total de Opiniones
                let reviews = b.querySelector('span[aria-label*="valoraciones"], .a-size-base.s-underline-text')?.innerText || "0";
                reviews = reviews.replace(/[^0-9]/g, ""); // Solo dejamos los números

                return {
                    sku_id: nombre,
                    precio_raw: `${precioE}.${precioD}`.replace(/[^0-9.]/g, ""),
                    rating: ratingNum,
                    opiniones: reviews,
                    formato_raw: nombre // El nombre contiene los kg para tu RegEx en Spark
                };
            });
            """
            
            items_pagina = driver.execute_script(script_js)

            for item in items_pagina:
                datos_recuperados.append({
                    **item,
                    "marca": "Amazon_Mascotas", # Se depura en Spark buscando marcas en el sku_id
                    "tienda": "Amazon",
                    "fecha_captura": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "grupo": NOMBRE_GRUPO
                })

            # NAVEGACIÓN A SIGUIENTE PÁGINA
            try:
                btn_sig = driver.find_element(By.CLASS_NAME, "s-pagination-next")
                if "s-pagination-disabled" in btn_sig.get_attribute("class"):
                    break
                driver.execute_script("arguments[0].click();", btn_sig)
                # Pausa aleatoria pequeña para no parecer bot entre páginas
                time.sleep(2)
            except:
                break

        return datos_recuperados

    except Exception as e:
        print(f"Error en el scraper de Amazon: {e}")
        return datos_recuperados