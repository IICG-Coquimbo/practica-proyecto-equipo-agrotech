import time
import datetime

def scraper_tiendanimal(driver, By, paginas=1):
    datos_tienda = []
    base_url = "https://www.tiendanimal.es/perros/dietas-veterinarias/"
    
    for p in range(1, paginas + 1):
        url = f"{base_url}?page={p}" if p > 1 else base_url
        try:
            driver.get(url)
            # Scroll para asegurar que los elementos carguen sus atributos (Lazy Load)
            driver.execute_script("window.scrollTo(0, 600);")
            time.sleep(4) 

            script_js = """
            let cards = document.querySelectorAll('.isk-product-card');

            return Array.from(cards).map(p => {
                // 1. EXTRACCIÓN DE MARCA Y NOMBRE (Basado en imagen.png)
                let headline = p.querySelector('.isk-product-card__headline');
                let marca_real = p.querySelector('.isk-product-card__headline-brand')?.innerText.trim() || "S/M";
                
                // Extraemos el texto del nombre ignorando el span de la marca
                let nombre_real = headline ? headline.childNodes[headline.childNodes.length - 1].textContent.trim() : "Sin nombre";

                // 2. RESCATE DE SKU (Si data-product-card-data falla, buscamos en data-itemid o href)
                let rawData = p.getAttribute('data-product-card-data');
                let jsonData = rawData ? JSON.parse(rawData) : {};
                
                // Prioridad de SKU: JSON > Atributo data-itemid > ID en la URL
                let sku_real = jsonData.id || p.getAttribute('data-itemid') || p.querySelector('a')?.href.split('-').pop().replace('.html', '') || "Sin ID";

                // 3. PRECIO, RATING Y OPINIONES
                let precio = p.querySelector('.isk-product-card__price')?.getAttribute('data-min-price') || 
                             p.querySelector('.price-value')?.innerText.trim() || "0.0";
                
                let opciones = p.querySelector('.isk-product-card__options, .isk-product-card__pum')?.innerText.trim() || "";
                let rating = p.querySelector('.isk-reviews, [data-rating]')?.getAttribute('data-rating') || "0";
                let opiniones = p.querySelector('.isk-product-card__reviews-total')?.innerText.replace(/[()]/g, "").trim() || "0";

                return {
                    sku_id: sku_real,
                    marca: marca_real,
                    precio_raw: precio,
                    formato_raw: nombre_real + " " + opciones,
                    rating: rating,
                    opiniones: opiniones,
                    moneda: "EUR"
                };
            });
            """
            
            items = driver.execute_script(script_js)
            
            for item in items:
                # Filtro de veracidad para no enviar basura al main
                if item['precio_raw'] != "0.0" and item['sku_id'] != "Sin ID":
                    datos_tienda.append({
                        **item, 
                        "tienda": "Tiendanimal", 
                        "fecha_captura": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            print(f"✅ Tiendanimal P{p}: {len(items)} detectados, {len(datos_tienda)} válidos.")

        except Exception as e:
            print(f"⚠️ Error Tiendanimal P{p}: {e}")
            continue
            
    return datos_tienda