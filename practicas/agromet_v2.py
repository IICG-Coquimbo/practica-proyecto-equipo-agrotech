"""
Scraper de Datos Climáticos - Agromet Chile [DESARROLLO]
=========================================================
Versión con validaciones y tests para desarrollo local.

Extrae temperatura promedio y precipitación de la estación Coquimbo.
Granularidad: 1 registro por día, con 2 filas por día (temperatura y precipitación)
Esquema estandarizado del equipo G9_Agrotech para MongoDB Atlas.

Base de datos: practicas_agrotech
Colección: agromet_v1

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
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Configuración para ignorar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Configuración del proyecto
CONFIG = {
    "integrante": "gabriel tenorio",
    "producto": "clima_coquimbo",
    "region_id": 5,
    "comuna_id": 4102,
    "estacion_id": 327,
    # MongoDB
    "mongo_host": "database",  # Nombre del servicio en docker-compose
    "mongo_port": 27017,
    "db_name": "practicas_agrotech",
    "collection_name": "agromet_v1"
}

# Mapeo de número de mes a nombre en español
MESES_ES = {
    "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
    "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
    "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
}


def mes_a_nombre(fecha_hora_str):
    """Convierte 'DD-MM-YYYY HH:MM' al nombre del mes en español."""
    try:
        return MESES_ES.get(fecha_hora_str[3:5], fecha_hora_str)
    except (IndexError, TypeError):
        return fecha_hora_str


def validar_datos(datos_finales):
    """
    Valida la calidad de los datos extraídos.
    
    Args:
        datos_finales (list): Lista de diccionarios con los datos
        
    Returns:
        dict: Diccionario con estadísticas de validación
    """
    print("\n" + "=" * 60)
    print("🔍 VALIDACIÓN DE DATOS")
    print("=" * 60)
    
    if not datos_finales:
        print("❌ ERROR: No hay datos para validar")
        return {"valido": False, "error": "Sin datos"}
    
    total_registros = len(datos_finales)
    print(f"📊 Total de registros: {total_registros}")
    
    # Validar estructura
    campos_requeridos = ["integrante", "producto", "etiqueta", "mes", "valor", "fecha_captura"]
    registros_validos = 0
    valores_nulos = 0
    
    for dato in datos_finales:
        campos_presentes = all(campo in dato for campo in campos_requeridos)
        if campos_presentes:
            registros_validos += 1
        if dato.get('valor') is None:
            valores_nulos += 1
    
    print(f"✅ Registros con estructura válida: {registros_validos}/{total_registros}")
    print(f"⚠️  Registros con valor nulo: {valores_nulos}/{total_registros}")
    
    # Validar etiquetas
    etiquetas = [d.get('etiqueta') for d in datos_finales]
    etiquetas_temp = etiquetas.count('temperatura_promedio')
    etiquetas_precip = etiquetas.count('precipitacion_total')
    
    print(f"🌡️  Registros de temperatura: {etiquetas_temp}")
    print(f"🌧️  Registros de precipitación: {etiquetas_precip}")
    
    # Validar que haya mismo número de registros de temperatura y precipitación
    if etiquetas_temp == etiquetas_precip:
        print("✅ Balance correcto entre temperatura y precipitación")
    else:
        print("⚠️  Desbalance entre temperatura y precipitación")
    
    # Validar porcentaje de nulos
    porcentaje_nulos = (valores_nulos / total_registros) * 100
    print(f"📉 Porcentaje de valores nulos: {porcentaje_nulos:.2f}%")
    
    if porcentaje_nulos > 50:
        print("❌ ADVERTENCIA: Más del 50% de valores nulos - revisar extracción")
        return {"valido": False, "error": "Demasiados valores nulos"}
    
    print("✅ Validación completada exitosamente")
    print("=" * 60)
    
    return {
        "valido": True,
        "total_registros": total_registros,
        "registros_validos": registros_validos,
        "valores_nulos": valores_nulos,
        "porcentaje_nulos": porcentaje_nulos,
        "temperatura_count": etiquetas_temp,
        "precipitacion_count": etiquetas_precip
    }


def ejecutar_extraccion():
    """
    Ejecuta la extracción de datos climáticos desde Agromet.
    
    Returns:
        list: Lista de diccionarios con estructura estandarizada:
            {
                "integrante": "gabriel tenorio",
                "producto": "clima_coquimbo",
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

    print("=" * 60)
    print("🚀 Iniciando extracción de datos climáticos")
    print("=" * 60)
    print(f"📍 Estación: Coquimbo (ID: {CONFIG['estacion_id']})")
    print(f"👤 Integrante: {CONFIG['integrante']}")
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
                print(f"   ⚠️  No se encontró tabla para {periodo}")
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
            print(f"   ✅ {len(df)} registros extraídos")
            
            # Rate limiting: esperar 1 segundo entre peticiones
            # Esto evita que Agromet bloquee el scraper por demasiadas peticiones
            if idx < len(TRIMESTRES_2025):
                print(f"   ⏱️  Esperando 1 segundo (rate limiting)...")
                time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error de conexión en {periodo}: {e}")
            print(f"   ℹ️  NOTA: Si ves errores 429 o timeouts, el servidor Agromet")
            print(f"           podría estar bloqueando peticiones. Espera unos minutos.")
            continue
        except Exception as e:
            print(f"   ❌ Error inesperado en {periodo}: {e}")
            continue

    if not todos_los_datos:
        print("\n❌ No se extrajeron datos de ningún trimestre")
        return []

    df_completo = pd.concat(todos_los_datos, ignore_index=True)
    print(f"\n{'=' * 60}")
    print("📊 Procesando datos por día...")
    print(f"{'=' * 60}")
    print(f"✅ Total de registros crudos (horarios): {len(df_completo)}")

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
            "producto": CONFIG['producto'],
            "etiqueta": "temperatura_promedio",
            "mes": row['mes'],
            "valor": round(row['temperatura_promedio'], 2) if pd.notna(row['temperatura_promedio']) else None,
            "fecha_captura": fecha_captura
        })
        
        # Registro para precipitación
        datos_finales.append({
            "integrante": CONFIG['integrante'],
            "producto": CONFIG['producto'],
            "etiqueta": "precipitacion_total",
            "mes": row['mes'],
            "valor": round(row['precipitacion_total'], 2) if pd.notna(row['precipitacion_total']) else None,
            "fecha_captura": fecha_captura
        })

    print(f"✅ Registros diarios generados: {len(datos_finales)} ({len(df_agrupado)} días × 2 variables)")
    print(f"{'=' * 60}")
    print("🎉 Extracción completada exitosamente")
    print(f"{'=' * 60}")

    return datos_finales


def insertar_en_mongodb(datos):
    """
    Inserta los datos en MongoDB local.
    
    Args:
        datos (list): Lista de diccionarios a insertar
        
    Returns:
        bool: True si la inserción fue exitosa
    """
    print("\n" + "=" * 60)
    print("💾 INSERTANDO EN MONGODB")
    print("=" * 60)
    
    try:
        # Conectar a MongoDB
        print(f"🔌 Conectando a MongoDB...")
        print(f"   Host: {CONFIG['mongo_host']}:{CONFIG['mongo_port']}")
        
        client = MongoClient(
            CONFIG['mongo_host'], 
            CONFIG['mongo_port'],
            serverSelectionTimeoutMS=5000  # Timeout de 5 segundos
        )
        
        # Verificar conexión
        client.admin.command('ping')
        print("✅ Conexión exitosa")
        
        # Seleccionar base de datos y colección
        db = client[CONFIG['db_name']]
        collection = db[CONFIG['collection_name']]
        
        print(f"📂 Base de datos: {CONFIG['db_name']}")
        print(f"📁 Colección: {CONFIG['collection_name']}")
        
        # Insertar datos
        print(f"📤 Insertando {len(datos)} registros...")
        result = collection.insert_many(datos)
        
        print(f"✅ Inserción completada: {len(result.inserted_ids)} documentos insertados")
        
        # Verificar inserción
        count = collection.count_documents({})
        print(f"📊 Total de documentos en la colección: {count}")
        
        # Mostrar muestra de datos insertados
        print("\n📋 Muestra de datos en MongoDB:")
        muestra = collection.find().limit(3)
        for i, doc in enumerate(muestra, 1):
            print(f"\n{i}. ID: {doc['_id']}")
            print(f"   Mes: {doc['mes']}")
            print(f"   Etiqueta: {doc['etiqueta']}")
            print(f"   Valor: {doc['valor']}")
        
        client.close()
        print("\n" + "=" * 60)
        print("✅ Proceso de inserción completado exitosamente")
        print(f"🌐 Puedes ver los datos en: http://localhost:8081")
        print("=" * 60)
        
        return True
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌ Error de conexión a MongoDB: {e}")
        print(f"ℹ️  Verifica que el contenedor de MongoDB esté corriendo:")
        print(f"   docker ps | grep mongo")
        return False
        
    except Exception as e:
        print(f"❌ Error inesperado al insertar en MongoDB: {e}")
        return False


def test_extraccion():
    """Tests básicos para verificar la extracción."""
    print("\n" + "=" * 60)
    print("🧪 EJECUTANDO TESTS BÁSICOS")
    print("=" * 60)
    
    datos = ejecutar_extraccion()
    
    # Test 1: Debe retornar datos
    assert len(datos) > 0, "❌ FALLO: No se retornaron datos"
    print("✅ Test 1: Se retornaron datos")
    
    # Test 2: Verificar estructura
    primer_dato = datos[0]
    campos_requeridos = ["integrante", "producto", "etiqueta", "mes", "valor", "fecha_captura"]
    for campo in campos_requeridos:
        assert campo in primer_dato, f"❌ FALLO: Falta el campo '{campo}'"
    print("✅ Test 2: Estructura de datos correcta")
    
    # Test 3: Verificar etiquetas válidas
    etiquetas_validas = ["temperatura_promedio", "precipitacion_total"]
    for dato in datos:
        assert dato['etiqueta'] in etiquetas_validas, f"❌ FALLO: Etiqueta inválida '{dato['etiqueta']}'"
    print("✅ Test 3: Etiquetas válidas")
    
    # Test 4: Verificar que todos tienen valor (o None)
    for dato in datos:
        assert 'valor' in dato, "❌ FALLO: Registro sin campo 'valor'"
    print("✅ Test 4: Todos los registros tienen campo 'valor'")
    
    # Test 5: Verificar balance entre etiquetas
    temp_count = sum(1 for d in datos if d['etiqueta'] == 'temperatura_promedio')
    precip_count = sum(1 for d in datos if d['etiqueta'] == 'precipitacion_total')
    assert temp_count == precip_count, "❌ FALLO: Desbalance entre temperatura y precipitación"
    print("✅ Test 5: Balance correcto entre temperatura y precipitación")
    
    print("\n" + "=" * 60)
    print("🎉 TODOS LOS TESTS PASARON EXITOSAMENTE")
    print("=" * 60)
    
    return datos


# Punto de entrada para pruebas locales
if __name__ == "__main__":
    print("\n" + "🔬" * 30)
    print("MODO DESARROLLO - Scraper Agromet")
    print("🔬" * 30)
    
    # Ejecutar tests
    datos = test_extraccion()
    
    # Validar datos
    stats = validar_datos(datos)
    
    if stats.get('valido'):
        # Insertar en MongoDB
        exito = insertar_en_mongodb(datos)
        
        if exito:
            print("\n✅ ¡Todo funcionó correctamente!")
        else:
            print("\n⚠️  Extracción exitosa pero fallo en MongoDB")
            print("💾 Guardando respaldo en CSV...")
            df_salida = pd.DataFrame(datos)
            archivo = f"agromet_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_salida.to_csv(archivo, index=False, encoding='utf-8-sig')
            print(f"   Archivo guardado: {archivo}")
    else:
        print(f"\n❌ Validación falló: {stats.get('error')}")
