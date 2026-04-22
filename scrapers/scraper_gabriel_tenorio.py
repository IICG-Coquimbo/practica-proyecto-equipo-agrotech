"""
Scraper de Datos Climáticos - Agromet Chile
============================================
Extrae temperatura promedio y precipitación de la estación Coquimbo.

Granularidad: 1 registro por día, con 2 filas por día (temperatura y precipitación)
Esquema estandarizado del equipo G9_Agrotech para MongoDB Atlas.

Integrante: Gabriel Tenorio
Universidad: UCN
"""

import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import warnings
import time

# Configuración para ignorar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Configuración del scraper
CONFIG = {
    "region_id": 5,
    "comuna_id": 4102,
    "estacion_id": 327
}
# Mapeo de número de mes a nombre en español
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
    except (IndexError, TypeError):
        return fecha_hora_str


def ejecutar_extraccion():
    """
    Ejecuta la extracción de datos climáticos desde Agromet.
    
    Returns:
        list: Lista de diccionarios con estructura estandarizada:
            {
                "integrante": "gabriel tenorio",
                "etiqueta": "temperatura_promedio" | "precipitacion_total",
                "mes": "Enero",
                "valor": 18.5,
                "fecha_captura": "2026-04-26"
            }
    """
    # Trimestres de 2025 (limitación de API: máximo 3 meses por petición)
    TRIMESTRES_2025 = [
        ("01/01/2025 00:00", "31/03/2025 23:59", "Q1"),
        ("01/04/2025 00:00", "30/06/2025 23:59", "Q2"),
        ("01/07/2025 00:00", "30/09/2025 23:59", "Q3"),
        ("01/10/2025 00:00", "31/12/2025 23:59", "Q4")
    ]

    print("Iniciando extracción - Agromet Coquimbo")
    
    todos_los_datos = []

    for idx, (fecha_inicio, fecha_fin, periodo) in enumerate(TRIMESTRES_2025, 1):
        print(f"Extrayendo trimestre {idx}/4: {periodo}")

        try:
            url = "https://www.agromet.cl/ext/aux/getDatosHistoricosEstacion_histo_estacion.php"

            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.agromet.cl/datos-historicos",
                "User-Agent": "Mozilla/5.0 (compatible; ScraperAgrotech/1.0)"
            }

            data = {
                "combo_reg_ia_id": str(CONFIG['region_id']),
                "combo_com_ia_id": str(CONFIG['comuna_id']),
                "combo_ema_ia_id": str(CONFIG['estacion_id']),
                "id_variable": "TEMP_MED",
                "fechaInicio": fecha_inicio,
                "fechaFin": fecha_fin
            }

            response = requests.post(url, headers=headers, data=data, verify=False)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            tabla = soup.find('table', class_='tabla_paginada')

            if not tabla:
                print(f"⚠️  No se encontró tabla para {periodo}")
                continue

            # Extraer headers y filas
            thead = tabla.find('thead')
            headers_row = thead.find_all('tr')[1]
            columnas = [th.get_text(strip=True) for th in headers_row.find_all('th')]

            tbody = tabla.find('tbody')
            filas = [[td.get_text(strip=True) for td in tr.find_all('td')] 
                     for tr in tbody.find_all('tr')]

            df = pd.DataFrame(filas, columns=columnas)

            # Limpiar valores numéricos
            for col in ['Temp. promedio aire', 'Precipitación horaria']:
                df[col] = (df[col]
                          .str.replace(',', '.')
                          .str.replace('--', '')
                          .pipe(lambda x: pd.to_numeric(x, errors='coerce')))

            todos_los_datos.append(df)
            print(f"✅ {len(df)} registros extraídos")
            
            # Rate limiting: esperar 1 segundo entre peticiones
            # Esto evita que Agromet bloquee el scraper por demasiadas peticiones
            if idx < len(TRIMESTRES_2025):
                time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión en {periodo}: {e}")
            print(f"ℹ️  NOTA: Si ves errores 429 o timeouts, Agromet podría estar bloqueando peticiones.")
            continue
        except Exception as e:
            print(f"❌ Error en {periodo}: {e}")
            continue

    if not todos_los_datos:
        print("❌ No se extrajeron datos")
        return []

    df_completo = pd.concat(todos_los_datos, ignore_index=True)
    print(f"Total registros crudos: {len(df_completo)}")

    # Derivar columnas de agrupación
    df_completo['fecha'] = df_completo['Fecha Hora'].str[:10]
    df_completo['mes'] = df_completo['Fecha Hora'].apply(mes_a_nombre)

    # Agrupación por día
    df_agrupado = (
        df_completo
        .groupby(['mes', 'fecha'], sort=False)
        .agg(
            temperatura_promedio=('Temp. promedio aire', 'mean'),
            precipitacion_total=('Precipitación horaria', 'sum')
        )
        .reset_index()
    )

    fecha_captura = datetime.now().strftime('%Y-%m-%d')

    # Construcción de datos con esquema estandarizado
    datos_finales = []

    for _, row in df_agrupado.iterrows():
        # Registro para temperatura
        datos_finales.append({
            "integrante": CONFIG['integrante'],
            "etiqueta": "temperatura_promedio",
            "mes": row['mes'],
            "valor": round(row['temperatura_promedio'], 2) if pd.notna(row['temperatura_promedio']) else None,
            "fecha_captura": fecha_captura
        })
        
        # Registro para precipitación
        datos_finales.append({
            "integrante": CONFIG['integrante'],
            "etiqueta": "precipitacion_total",
            "mes": row['mes'],
            "valor": round(row['precipitacion_total'], 2) if pd.notna(row['precipitacion_total']) else None,
            "fecha_captura": fecha_captura
        })

    print(f"✅ {len(datos_finales)} registros generados ({len(df_agrupado)} días × 2 variables)")
    print("Extracción completada")

    return datos_finales


if __name__ == "__main__":
    datos = ejecutar_extraccion()
    
    if datos:
        # Guardar en CSV para verificación local
        df_salida = pd.DataFrame(datos)
        archivo = f"agromet_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_salida.to_csv(archivo, index=False, encoding='utf-8-sig')
        print(f"💾 Datos guardados en: {archivo}")
        print(f"\nPrimeros registros:")
        print(df_salida.head(6))
