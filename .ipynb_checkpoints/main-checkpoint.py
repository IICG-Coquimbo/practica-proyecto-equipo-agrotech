from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from scrapers import scraper_alejandro_nunez
from scrapers import scraper_lissette_mathieu
from scrapers import scraper_gabriel_tenorio
from scrapers import scraper_maximiliano_berrios
from scrapers import scraper_mathieus_villavicencio
from scrapers import scraper_sebastian_castillo

# ─────────────────────────────────────────────
# FIX 3: Schema explícito para evitar crash cuando
#         un scraper devuelve lista vacía []
# ─────────────────────────────────────────────
SCHEMA = StructType([
    StructField("integrante",    StringType(), True),
    StructField("etiqueta",      StringType(), True),
    StructField("mes",           StringType(), True),
    StructField("valor",         FloatType(),  True),
    StructField("fecha_captura", StringType(), True),
])

# 1. Recolectamos listas de Python de todos los scrapers
data_ale      = scraper_alejandro_nunez.ejecutar_extraccion()
data_lissette = scraper_lissette_mathieu.ejecutar_extraccion()
data_gabriel  = scraper_gabriel_tenorio.ejecutar_extraccion()
data_maxi     = scraper_maximiliano_berrios.ejecutar_extraccion()
data_mathieus = scraper_mathieus_villavicencio.ejecutar_extraccion()
data_seba     = scraper_sebastian_castillo.ejecutar_extraccion()

# 2. Iniciamos Spark
# ─────────────────────────────────────────────
# FIX 5: El conector MongoDB Spark 10+ ya no lee database/collection
#         desde la URI — lanza "Missing configuration for: database".
#         Se deben declarar como configs separadas en el builder
#         Y también como .option() al momento del write.
# ─────────────────────────────────────────────
MONGO_URI        = "mongodb+srv://agrotech_sebastiancastillo:agrotechbigdata2026@cluster0.7z77rka.mongodb.net/?appName=Cluster0"
MONGO_DATABASE   = "db_g9agrotech"
MONGO_COLLECTION = "entrega1"

spark = SparkSession.builder \
    .appName("IntegradoraBigDataAgroTech") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.database",       MONGO_DATABASE) \
    .config("spark.mongodb.write.collection",     MONGO_COLLECTION) \
    .getOrCreate()

# 3. Spark convierte las listas en DataFrames
#    Con schema explícito no falla si algún scraper devuelve []
df_ale      = spark.createDataFrame(data_ale,      SCHEMA)
df_lissette = spark.createDataFrame(data_lissette, SCHEMA)
df_gabriel  = spark.createDataFrame(data_gabriel,  SCHEMA)
df_maxi     = spark.createDataFrame(data_maxi,     SCHEMA)
df_mathieus = spark.createDataFrame(data_mathieus, SCHEMA)
df_seba     = spark.createDataFrame(data_seba,     SCHEMA)

# 4. Unión de los 6 DataFrames en uno solo
df_final = (
    df_ale
    .union(df_lissette)
    .union(df_gabriel)
    .union(df_maxi)
    .union(df_mathieus)
    .union(df_seba)
)

# ─────────────────────────────────────────────
# FIX 4: "valor" ya llega como float desde los scrapers,
#         regexp_replace no opera sobre columnas numéricas.
#         Se castea directamente en lugar de limpiar texto.
# ─────────────────────────────────────────────
df_limpio = df_final.withColumn(
    "valor_numerico",
    col("valor").cast("float")
)

# 5. Spark guarda todo en MongoDB
#    Las opciones se repiten en el .option() del write porque algunos
#    entornos requieren ambas declaraciones (builder + write time).
df_limpio.write \
    .format("mongodb") \
    .mode("append") \
    .option("database",   MONGO_DATABASE) \
    .option("collection", MONGO_COLLECTION) \
    .save()