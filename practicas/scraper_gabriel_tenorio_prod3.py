"""
Scraper de Datos Climáticos - Agromet Chile
============================================
Extrae temperatura promedio y precipitación de estaciones en Coquimbo y La Serena.

Granularidad: 1 registro por día (temperatura y precipitación en el mismo registro)
Cobertura: Años 2024-2025 completos | 2 comunas (Coquimbo y La Serena)
Esquema estandarizado del equipo G9_Agrotech para MongoDB Atlas.

Integrante: Gabriel Tenorio
Universidad: UCN
"""

import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import warnings
import time

# Configuración para ignorar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Configuración de estaciones a extraer
# CAMBIO DE FORMATO: Si necesitas cambiar el formato de los nombres de comuna/estación,
# modifica los valores de 'comuna_nombre' y 'estacion_nombre' aquí.
# Ejemplos: "Coquimbo" vs "COQUIMBO", "Guayacán" vs "Guayacan" o "GUAYACAN"
ESTACIONES = [
    {
        "region_id": 5,
        "comuna_id": 4102,
        "comuna_nombre": "Coquimbo",      # ← CAMBIAR FORMATO AQUÍ si es necesario
        "estacion_id": 327,
        "estacion_nombre": "Coquimbo"     # ← CAMBIAR FORMATO AQUÍ si es necesario
    },
    {
        "region_id": 5,
        "comuna_id": 4101,
        "comuna_nombre": "La Serena",     # ← CAMBIAR FORMATO AQUÍ si es necesario
        "estacion_id": 323,
        "estacion_nombre": "Cerro Grande" # ← CAMBIAR FORMATO AQUÍ si es necesario
    }
]

# Integrante del equipo
INTEGRANTE = "gabriel tenorio"

# Mapeo de número de mes a nombre en español
MESES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
}


def mes_a_nombre(fecha_hora_str):
    """
    Convierte una fecha en formato 'DD/MM/YYYY HH:MM' o 'DD-MM-YYYY HH:MM' al nombre del mes.
    
    Args:
        fecha_hora_str (str): Fecha en formato 'DD/MM/YYYY HH:MM' o 'DD-MM-YYYY HH:MM'
    
    Returns:
        str: Nombre del mes en español (ej: "Enero")
    """
    try:
        # Detectar el separador (/ o -)
        if '/' in fecha_hora_str:
            mes_numero = fecha_hora_str.split('/')[1]
        elif '-' in fecha_hora_str:
            mes_numero = fecha_hora_str.split('-')[1]
        else:
            return fecha_hora_str
        return MESES.get(mes_numero, fecha_hora_str)
    except (IndexError, TypeError, AttributeError):
        return fecha_hora_str


def extraer_año(fecha_hora_str):
    """
    Extrae el año de una fecha en formato 'DD/MM/YYYY HH:MM' o 'DD-MM-YYYY HH:MM'.
    
    Args:
        fecha_hora_str (str): Fecha en formato 'DD/MM/YYYY HH:MM' o 'DD-MM-YYYY HH:MM'
    
    Returns:
        int: Año (ej: 2025)
    """
    try:
        # Detectar el separador (/ o -)
        if '/' in fecha_hora_str:
            año_str = fecha_hora_str.split('/')[2].split()[0]
        elif '-' in fecha_hora_str:
            año_str = fecha_hora_str.split('-')[2].split()[0]
        else:
            return None
        return int(año_str)
    except (IndexError, ValueError, TypeError, AttributeError):
        return None


def convertir_a_formato_chileno(fecha_str):
    """
    Convierte fecha de formato 'YYYY-MM-DD' a formato chileno 'DD-MM-YYYY'.
    
    Args:
        fecha_str (str): Fecha en formato 'YYYY-MM-DD' o 'DD/MM/YYYY'
    
    Returns:
        str: Fecha en formato chileno 'DD-MM-YYYY'
    """
    try:
        # Si viene en formato DD/MM/YYYY (del scraping)
        if '/' in fecha_str:
            partes = fecha_str.split('/')
            return f"{partes[0]}-{partes[1]}-{partes[2]}"
        # Si viene en formato YYYY-MM-DD (interno)
        elif '-' in fecha_str and len(fecha_str.split('-')[0]) == 4:
            partes = fecha_str.split('-')
            return f"{partes[2]}-{partes[1]}-{partes[0]}"
        else:
            return fecha_str
    except (IndexError, AttributeError):
        return fecha_str


def extraer_datos_estacion(estacion_config, trimestres):
    """
    Extrae datos de una estación específica para todos los trimestres.
    
    Args:
        estacion_config (dict): Configuración de la estación
        trimestres (list): Lista de trimestres a extraer
    
    Returns:
        pd.DataFrame: DataFrame con los datos crudos de la estación
    """
    comuna = estacion_config['comuna_nombre']
    estacion = estacion_config['estacion_nombre']
    
    print(f"\n{'='*60}")
    print(f"Extrayendo: {estacion} ({comuna})")
    print(f"{'='*60}")
    
    todos_los_datos = []
    total_trimestres = len(trimestres)

    for idx, (fecha_inicio, fecha_fin, periodo) in enumerate(trimestres, 1):
        print(f"Trimestre {idx}/{total_trimestres}: {periodo}")

        try:
            url = "https://www.agromet.cl/ext/aux/getDatosHistoricosEstacion_histo_estacion.php"

            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.agromet.cl/datos-historicos",
                "User-Agent": "Mozilla/5.0 (compatible; ScraperAgrotech/1.0)"
            }

            data = {
                "combo_reg_ia_id": str(estacion_config['region_id']),
                "combo_com_ia_id": str(estacion_config['comuna_id']),
                "combo_ema_ia_id": str(estacion_config['estacion_id']),
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

            # Agregar metadatos de la estación
            df['comuna'] = comuna
            df['estacion'] = estacion

            todos_los_datos.append(df)
            print(f"✅ {len(df)} registros extraídos")
            
            # Rate limiting: esperar 1 segundo entre peticiones
            # Esto evita que Agromet bloquee el scraper por demasiadas peticiones
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión en {periodo}: {e}")
            print(f"ℹ️  NOTA: Si ves errores 429 o timeouts, Agromet podría estar bloqueando peticiones.")
            continue
        except Exception as e:
            print(f"❌ Error en {periodo}: {e}")
            continue

    if not todos_los_datos:
        print(f"❌ No se extrajeron datos para {estacion} ({comuna})")
        return pd.DataFrame()

    df_estacion = pd.concat(todos_los_datos, ignore_index=True)
    print(f"✅ Total registros crudos de {estacion}: {len(df_estacion)}")
    
    return df_estacion


def ejecutar_extraccion():
    """
    Ejecuta la extracción de datos climáticos desde Agromet para todas las estaciones.
    
    Returns:
        list: Lista de diccionarios con estructura estandarizada
    """
    # Trimestres de 2024 y 2025 (limitación de API: máximo 3 meses por petición)
    TRIMESTRES = [
        # Año 2024
        ("01/01/2024 00:00", "31/03/2024 23:59", "2024-Q1"),
        ("01/04/2024 00:00", "30/06/2024 23:59", "2024-Q2"),
        ("01/07/2024 00:00", "30/09/2024 23:59", "2024-Q3"),
        ("01/10/2024 00:00", "31/12/2024 23:59", "2024-Q4"),
        # Año 2025
        ("01/01/2025 00:00", "31/03/2025 23:59", "2025-Q1"),
        ("01/04/2025 00:00", "30/06/2025 23:59", "2025-Q2"),
        ("01/07/2025 00:00", "30/09/2025 23:59", "2025-Q3"),
        ("01/10/2025 00:00", "31/12/2025 23:59", "2025-Q4")
    ]

    print("╔" + "="*58 + "╗")
    print("║ SCRAPER AGROMET CHILE - Datos Climáticos 2024-2025      ║")
    print("║ Estaciones: Coquimbo (Guayacán) + La Serena (C. Grande) ║")
    print("╚" + "="*58 + "╝")
    
    # Extraer datos de todas las estaciones
    dataframes_estaciones = []
    
    for estacion in ESTACIONES:
        df_estacion = extraer_datos_estacion(estacion, TRIMESTRES)
        if not df_estacion.empty:
            dataframes_estaciones.append(df_estacion)
    
    if not dataframes_estaciones:
        print("\n❌ No se extrajeron datos de ninguna estación")
        return []
    
    # Consolidar todos los datos
    df_completo = pd.concat(dataframes_estaciones, ignore_index=True)
    print(f"\n{'='*60}")
    print(f"Total registros crudos de todas las estaciones: {len(df_completo)}")
    print(f"{'='*60}")
    
    # DEBUG: Verificar columnas disponibles
    print(f"\n🔍 DEBUG - Columnas disponibles en el DataFrame:")
    print(df_completo.columns.tolist())
    print(f"\n🔍 DEBUG - Primeras 3 filas:")
    print(df_completo.head(3))

    # Verificar que existe la columna 'Fecha Hora'
    if 'Fecha Hora' not in df_completo.columns:
        print("\n❌ ERROR: No se encontró la columna 'Fecha Hora'")
        print(f"Columnas disponibles: {df_completo.columns.tolist()}")
        return []

    # Derivar columnas de agrupación
    df_completo['fecha'] = df_completo['Fecha Hora'].str[:10]  # DD/MM/YYYY
    df_completo['mes'] = df_completo['Fecha Hora'].apply(mes_a_nombre)
    df_completo['año'] = df_completo['Fecha Hora'].apply(extraer_año)
    
    # DEBUG: Verificar derivaciones
    print(f"\n🔍 DEBUG - Columnas después de derivar:")
    print(df_completo[['Fecha Hora', 'fecha', 'mes', 'año', 'comuna', 'estacion']].head(3))

    # Verificar que no hay valores nulos en las columnas clave
    print(f"\n🔍 DEBUG - Valores nulos:")
    print(df_completo[['fecha', 'mes', 'año', 'comuna', 'estacion']].isnull().sum())

    # Agrupación por día, comuna y estación
    df_agrupado = (
        df_completo
        .groupby(['comuna', 'estacion', 'año', 'mes', 'fecha'], sort=False, dropna=False)
        .agg(
            temperatura_promedio=('Temp. promedio aire', 'mean'),
            precipitacion_total=('Precipitación horaria', 'sum')
        )
        .reset_index()
    )
    
    print(f"\n🔍 DEBUG - Registros después de agrupar: {len(df_agrupado)}")
    print(f"🔍 DEBUG - Primeras 3 filas agrupadas:")
    print(df_agrupado.head(3))

    # Fecha de captura en formato chileno DD-MM-YYYY
    fecha_captura = datetime.now().strftime('%d-%m-%Y')

    # Construcción de datos con esquema estandarizado
    datos_finales = []

    for _, row in df_agrupado.iterrows():
        datos_finales.append({
            "integrante": INTEGRANTE,
            "mes": row['mes'],
            "año": int(row['año']) if pd.notna(row['año']) else None,
            "fecha": convertir_a_formato_chileno(row['fecha']),
            "fecha_captura": fecha_captura,
            "comuna": row['comuna'],
            "estacion": row['estacion'],
            "temperatura": round(row['temperatura_promedio'], 2) if pd.notna(row['temperatura_promedio']) else None,
            "precipitaciones": round(row['precipitacion_total'], 2) if pd.notna(row['precipitacion_total']) else None
        })

    print(f"\n✅ {len(datos_finales)} registros generados")
    print(f"   • {len(df_agrupado)} días únicos")
    print(f"   • {len(ESTACIONES)} estaciones")
    print(f"   • 2 años de cobertura (2024-2025)")
    print("\nExtracción completada exitosamente ✨")

    return datos_finales


if __name__ == "__main__":
    datos = ejecutar_extraccion()
    
    if datos:
        # Conectar a MongoDB
        try:
            client = MongoClient('mongodb://database:27017/')
            db = client['proyecto_bigdata']
            coleccion = db['agromet_v3']
            
            print(f"\n{'='*60}")
            print("📡 Conectando a MongoDB...")
            print(f"{'='*60}")
            
            # Insertar datos en la colección
            resultado = coleccion.insert_many(datos)
            
            print(f"✅ Conexión exitosa a MongoDB")
            print(f"✅ {len(resultado.inserted_ids)} documentos insertados en 'agromet_v3'")
            print(f"   • Base de datos: proyecto_bigdata")
            print(f"   • Colección: agromet_v3")
            
            # Mostrar estadísticas
            df_salida = pd.DataFrame(datos)
            print(f"\n📊 ESTADÍSTICAS DEL DATASET:")
            print(f"   • Total de registros: {len(df_salida)}")
            print(f"   • Comunas: {df_salida['comuna'].unique().tolist()}")
            print(f"   • Estaciones: {df_salida['estacion'].unique().tolist()}")
            print(f"   • Años: {sorted(df_salida['año'].unique().tolist())}")
            print(f"   • Rango de fechas: {df_salida['fecha'].min()} a {df_salida['fecha'].max()}")
            
            # Mostrar primeros registros
            print(f"\n📋 PRIMEROS 5 REGISTROS INSERTADOS:")
            print(df_salida.head(5).to_string(index=False))
            
            # Cerrar conexión
            client.close()
            print(f"\n✅ Datos cargados exitosamente en MongoDB ✨")
            
        except Exception as e:
            print(f"\n❌ Error al conectar/insertar en MongoDB: {e}")
            print("Asegúrate de que el contenedor de MongoDB esté corriendo")