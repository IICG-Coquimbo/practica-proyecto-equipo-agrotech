from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from scrapers import scraper_alejandro_nunez
from scrapers import scraper_lissette_mathieu
from scrapers import scraper_gabriel_tenorio
from scrapers import scraper_maximiliano_berrios
from scrapers import scraper_mathieus_villavicencio
from scrapers import scraper_sebastian_castillo

# ═══════════════════════════════════════════════════════════════════
# CONFIGURACIÓN MONGODB
# ═══════════════════════════════════════════════════════════════════
MONGO_URI        = "mongodb+srv://agrotech_sebastiancastillo:agrotechbigdata2026@cluster0.7z77rka.mongodb.net/?appName=Cluster0"
MONGO_DATABASE   = "db_g9agrotech"
MONGO_COLLECTION = "raw_data"

print("=" * 70)
print("🚀 AGROTECH - INTEGRACIÓN BIG DATA")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════════
# PASO 1: EXTRACCIÓN DE DATOS
# ═══════════════════════════════════════════════════════════════════
print("\n📊 EXTRAYENDO DATOS DE SCRAPERS...")

try:
    data_ale      = scraper_alejandro_nunez.ejecutar_extraccion()
    print(f"  ✓ Alejandro Núñez:        {len(data_ale)} registros")
except Exception as e:
    print(f"  ✗ Alejandro Núñez:        ERROR - {e}")
    data_ale = []

try:
    data_lissette = scraper_lissette_mathieu.ejecutar_extraccion()
    print(f"  ✓ Lissette Mathieu:       {len(data_lissette)} registros")
except Exception as e:
    print(f"  ✗ Lissette Mathieu:       ERROR - {e}")
    data_lissette = []

try:
    data_gabriel  = scraper_gabriel_tenorio.ejecutar_extraccion()
    print(f"  ✓ Gabriel Tenorio:        {len(data_gabriel)} registros")
except Exception as e:
    print(f"  ✗ Gabriel Tenorio:        ERROR - {e}")
    data_gabriel = []

try:
    data_maxi     = scraper_maximiliano_berrios.ejecutar_extraccion()
    print(f"  ✓ Maximiliano Berrios:    {len(data_maxi)} registros")
except Exception as e:
    print(f"  ✗ Maximiliano Berrios:    ERROR - {e}")
    data_maxi = []

try:
    data_mathieus = scraper_mathieus_villavicencio.ejecutar_extraccion()
    print(f"  ✓ Mathieus Villavicencio: {len(data_mathieus)} registros")
except Exception as e:
    print(f"  ✗ Mathieus Villavicencio: ERROR - {e}")
    data_mathieus = []

try:
    data_seba     = scraper_sebastian_castillo.ejecutar_extraccion()
    print(f"  ✓ Sebastián Castillo:     {len(data_seba)} registros")
except Exception as e:
    print(f"  ✗ Sebastián Castillo:     ERROR - {e}")
    data_seba = []

total_registros = (len(data_ale) + len(data_lissette) + len(data_gabriel) + 
                   len(data_maxi) + len(data_mathieus) + len(data_seba))

print(f"\n📈 TOTAL DE REGISTROS EXTRAÍDOS: {total_registros}")

if total_registros == 0:
    print("\n⚠️  No hay datos para procesar. Terminando...")
    exit(0)

# ═══════════════════════════════════════════════════════════════════
# PASO 2: INICIALIZACIÓN DE SPARK
# ═══════════════════════════════════════════════════════════════════
print("\n⚡ INICIANDO SPARK SESSION...")

spark = SparkSession.builder \
    .appName("IntegradoraBigDataAgroTech") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.database",       MONGO_DATABASE) \
    .config("spark.mongodb.write.collection",     MONGO_COLLECTION) \
    .getOrCreate()

print("  ✓ Spark Session iniciada")

# ═══════════════════════════════════════════════════════════════════
# PASO 3: CONVERSIÓN A DATAFRAMES
# ═══════════════════════════════════════════════════════════════════
print("\n🔄 CONVIRTIENDO DATOS A DATAFRAMES...")

# Usamos inferencia automática de schema (más flexible)
df_ale      = spark.createDataFrame(data_ale)      if data_ale      else None
df_lissette = spark.createDataFrame(data_lissette) if data_lissette else None
df_gabriel  = spark.createDataFrame(data_gabriel)  if data_gabriel  else None
df_maxi     = spark.createDataFrame(data_maxi)     if data_maxi     else None
df_mathieus = spark.createDataFrame(data_mathieus) if data_mathieus else None
df_seba     = spark.createDataFrame(data_seba)     if data_seba     else None

print("  ✓ DataFrames creados")

# ═══════════════════════════════════════════════════════════════════
# PASO 4: UNIÓN DE DATAFRAMES
# ═══════════════════════════════════════════════════════════════════
print("\n🔗 UNIENDO DATAFRAMES...")

# Recolectamos solo los DataFrames que NO son None
dfs_validos = []
if df_ale is not None:      dfs_validos.append(df_ale)
if df_lissette is not None: dfs_validos.append(df_lissette)
if df_gabriel is not None:  dfs_validos.append(df_gabriel)
if df_maxi is not None:     dfs_validos.append(df_maxi)
if df_mathieus is not None: dfs_validos.append(df_mathieus)
if df_seba is not None:     dfs_validos.append(df_seba)

if len(dfs_validos) == 0:
    print("  ✗ No hay DataFrames válidos para unir")
    spark.stop()
    exit(1)

# Unión flexible: permite columnas faltantes entre DataFrames
df_final = dfs_validos[0]
for df in dfs_validos[1:]:
    df_final = df_final.unionByName(df, allowMissingColumns=True)

print(f"  ✓ DataFrames unidos: {df_final.count()} registros totales")

# ═══════════════════════════════════════════════════════════════════
# PASO 5: MOSTRAR SCHEMA Y PREVIEW
# ═══════════════════════════════════════════════════════════════════
print("\n📋 SCHEMA DEL DATAFRAME UNIFICADO:")
df_final.printSchema()

print("\n👀 PREVIEW DE DATOS (primeras 5 filas):")
df_final.show(5, truncate=False)

# ═══════════════════════════════════════════════════════════════════
# PASO 6: ESCRITURA A MONGODB
# ═══════════════════════════════════════════════════════════════════
print("\n💾 ESCRIBIENDO A MONGODB...")

try:
    df_final.write \
        .format("mongodb") \
        .mode("append") \
        .option("database",   MONGO_DATABASE) \
        .option("collection", MONGO_COLLECTION) \
        .save()
    
    print(f"  ✓ Datos escritos exitosamente")
    print(f"  📍 Database:   {MONGO_DATABASE}")
    print(f"  📍 Collection: {MONGO_COLLECTION}")
    print(f"  📍 Registros:  {df_final.count()}")
    
except Exception as e:
    print(f"  ✗ ERROR AL ESCRIBIR EN MONGODB:")
    print(f"     {e}")
    spark.stop()
    exit(1)

# ═══════════════════════════════════════════════════════════════════
# PASO 7: FINALIZACIÓN
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("✅ PROCESO COMPLETADO EXITOSAMENTE")
print("=" * 70)

spark.stop()
