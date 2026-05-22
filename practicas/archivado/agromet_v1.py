"""
Scraper de Datos Climáticos - Agromet Chile
============================================
Extrae temperatura promedio y precipitación de la estación Coquimbo.

Granularidad de salida: promedio/suma por DÍA
-----------------------------------------------------------
Los datos crudos de Agromet son horarios (24 registros/día), lo que
genera ~8.760 filas anuales. Para reducir volumen y evitar sesgo por
sobrerrepresentación horaria, se decidió agrupar en 1 registro por día:

    - Temperatura : promedio de las 24 mediciones del día (°C)
    - Precipitación: suma de las 24 mediciones del día (mm acumulados)

Esto produce ~365 registros anuales, manteniendo el esquema de columnas
acordado con el equipo para la integración global en MongoDB Atlas:
    mes, temperatura_promedio, precipitacion_total, fecha_captura, integrante

Equipo: G9_Agrotech
Integrante: Gabriel Tenorio
Universidad: UCN
"""

import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import warnings

# Configuración para ignorar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Mapeo de número de mes a nombre en español
MESES_ES = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre"
}


def mes_a_nombre(fecha_hora_str):
    """
    Convierte la columna 'Fecha Hora' al nombre del mes en español.

    Formato de entrada esperado: "DD-MM-YYYY HH:MM"
    Ejemplo: "15-03-2025 14:00" → "Marzo"

    Returns:
        str: Nombre del mes en español, o la cadena original si no se puede parsear.
    """
    try:
        numero_mes = fecha_hora_str[3:5]
        return MESES_ES.get(numero_mes, fecha_hora_str)
    except (IndexError, TypeError):
        return fecha_hora_str


def ejecutar_extraccion():
    """
    Ejecuta la extracción de datos climáticos desde Agromet.
    Retorna un registro por día con temperatura promedio y precipitación acumulada.

    Returns:
        list: Lista de diccionarios con la estructura:
            {
                "mes": "Enero",
                "temperatura_promedio": 18.5,
                "precipitacion_total": 2.3,
                "fecha_captura": "2026-04-22 15:30:00",
                "integrante": "gabriel tenorio"
            }
    """

    # Configuración de extracción
    REGION_ID = 5
    COMUNA_ID = 4102
    ESTACION_ID = 327
    VARIABLE = "TEMP_MED"

    # Trimestres de 2025 (limitación de API: máximo 3 meses por petición)
    TRIMESTRES_2025 = [
        ("01/01/2025 00:00", "31/03/2025 23:59", "Q1_2025"),
        ("01/04/2025 00:00", "30/06/2025 23:59", "Q2_2025"),
        ("01/07/2025 00:00", "30/09/2025 23:59", "Q3_2025"),
        ("01/10/2025 00:00", "31/12/2025 23:59", "Q4_2025")
    ]

    print("=" * 60)
    print("🚀 Iniciando extracción de datos climáticos")
    print("=" * 60)
    print(f"📍 Estación: Coquimbo (ID: {ESTACION_ID})")
    print(f"👤 Integrante: Gabriel Tenorio")
    print(f"📅 Fecha de captura: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    todos_los_datos = []

    for idx, (fecha_inicio, fecha_fin, periodo) in enumerate(TRIMESTRES_2025, 1):
        print(f"\n📥 Extrayendo trimestre {idx}/4: {periodo}")
        print(f"   Período: {fecha_inicio} → {fecha_fin}")

        try:
            url = "https://www.agromet.cl/ext/aux/getDatosHistoricosEstacion_histo_estacion.php"

            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.agromet.cl/datos-historicos",
                "User-Agent": "Mozilla/5.0 (compatible; ScraperAgrotech/1.0)"
            }

            data = {
                "combo_reg_ia_id": str(REGION_ID),
                "combo_com_ia_id": str(COMUNA_ID),
                "combo_ema_ia_id": str(ESTACION_ID),
                "id_variable": VARIABLE,
                "fechaInicio": fecha_inicio,
                "fechaFin": fecha_fin
            }

            response = requests.post(url, headers=headers, data=data, verify=False)
            response.raise_for_status()

            # Parsear HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            tabla = soup.find('table', class_='tabla_paginada')

            if not tabla:
                print(f"   ⚠️  No se encontró tabla para {periodo}")
                continue

            # Extraer headers
            thead = tabla.find('thead')
            headers_row = thead.find_all('tr')[1]
            columnas = [th.get_text(strip=True) for th in headers_row.find_all('th')]

            # Extraer filas
            tbody = tabla.find('tbody')
            filas = []
            for tr in tbody.find_all('tr'):
                fila = [td.get_text(strip=True) for td in tr.find_all('td')]
                filas.append(fila)

            df = pd.DataFrame(filas, columns=columnas)

            # Limpiar valores numéricos
            df['Temp. promedio aire'] = (
                df['Temp. promedio aire']
                .str.replace(',', '.')
                .str.replace('--', '')
            )
            df['Precipitación horaria'] = (
                df['Precipitación horaria']
                .str.replace(',', '.')
                .str.replace('--', '')
            )

            df['Temp. promedio aire'] = pd.to_numeric(df['Temp. promedio aire'], errors='coerce')
            df['Precipitación horaria'] = pd.to_numeric(df['Precipitación horaria'], errors='coerce')

            todos_los_datos.append(df)
            print(f"   ✅ {len(df)} registros extraídos")

        except Exception as e:
            print(f"   ❌ Error en {periodo}: {e}")
            continue

    if not todos_los_datos:
        print("\n❌ No se extrajeron datos")
        return []

    df_completo = pd.concat(todos_los_datos, ignore_index=True)

    print(f"\n{'=' * 60}")
    print("📊 Procesando datos por día...")
    print(f"{'=' * 60}")
    print(f"✅ Total de registros crudos: {len(df_completo)}")

    # --- Derivar columnas de agrupación ---
    # Formato de 'Fecha Hora': "DD-MM-YYYY HH:MM"
    df_completo['fecha'] = df_completo['Fecha Hora'].str[:10]   # "DD-MM-YYYY" para agrupar
    df_completo['mes']   = df_completo['Fecha Hora'].apply(mes_a_nombre)

    # --- Agrupación por día ---
    # Temperatura: promedio de las 24 horas del día
    # Precipitación: suma de las 24 horas del día (mm acumulados)
    df_agrupado = (
        df_completo
        .groupby(['mes', 'fecha'], sort=False)
        .agg(
            temperatura_promedio=('Temp. promedio aire',   'mean'),
            precipitacion_total =('Precipitación horaria', 'sum')
        )
        .reset_index()
        .drop(columns=['fecha'])  # fecha solo se usó para agrupar, no va al esquema final
    )

    fecha_captura = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Construir lista final respetando el esquema acordado con el equipo
    datos_finales = []

    for _, row in df_agrupado.iterrows():
        datos_finales.append({
            "mes":                  row['mes'],
            "temperatura_promedio": round(row['temperatura_promedio'], 2) if pd.notna(row['temperatura_promedio']) else None,
            "precipitacion_total":  round(row['precipitacion_total'],  2) if pd.notna(row['precipitacion_total'])  else None,
            "fecha_captura":        fecha_captura,
            "integrante":           "gabriel tenorio"
        })

    print(f"✅ Registros diarios generados: {len(datos_finales)}")
    print(f"\n{'=' * 60}")
    print("🎉 Extracción completada exitosamente")
    print(f"{'=' * 60}")

    # Muestra de datos
    print("\n📋 Muestra de datos extraídos:")
    for i, dato in enumerate(datos_finales[:5], 1):
        print(f"\n{i}. Mes: {dato['mes']}")
        print(f"   Temperatura promedio : {dato['temperatura_promedio']}°C")
        print(f"   Precipitación total  : {dato['precipitacion_total']} mm")

    if len(datos_finales) > 5:
        print(f"\n... y {len(datos_finales) - 5} registros más")

    return datos_finales


# Punto de entrada para pruebas locales.
# Este bloque NO se ejecuta cuando main.py importa ejecutar_extraccion().
if __name__ == "__main__":
    datos = ejecutar_extraccion()

    if datos:
        df_salida = pd.DataFrame(datos)
        archivo_salida = f"agromet_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_salida.to_csv(archivo_salida, index=False, encoding='utf-8-sig')
        print(f"\n💾 Datos guardados en: {archivo_salida}")
